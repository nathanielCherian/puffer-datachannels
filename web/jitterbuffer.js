'use strict';

// ---------------------------------------------------------------------------
// The video jitter buffer: everything between "a datagram arrived" and "decode
// this frame now". It is deliberately self-contained so it can be replaced.
// The page only uses:
//
//   push(packet, now)   a video DATA packet arrived (see Wire.parseData)
//   tick(now)           send any NACKs that are due
//   pop(now)            the next frame to decode, if its time has come, else null
//   forceKeyframe(now)  the decoder failed: resynchronise on a keyframe
//   ownOffsetMs()       playout time − capture time this buffer wants, or null
//   syncOffsetMs        set by the page: the audio buffer's offset (lip sync)
//   stats()
//
// and gives it three callbacks: sendNack(items), sendKeyRequest(frameId), and
// rttMs(). `now` is always the page's wall clock in ms. The audio buffer
// (audio.js) has the same shape and shares DelayEstimator.
//
// What this one does:
//
// Reassembly. Fragments of a frame arrive in any order; a frame is complete
// when all fragCount of them are here.
//
// Playout delay. For every complete frame, d = completion time − capture time
// (browser clock minus server clock, so it includes an unknown constant). The
// minimum of d over the last 30 s is the baseline: a frame that had no extra
// delay. d − baseline is that frame's lateness. The target delay is the 95th
// percentile of lateness over the last 5 s plus a margin; it rises at once and
// decays slowly. A frame is played at capture + baseline + target, which keeps
// the capture spacing as long as the target holds still. If the audio buffer
// needs a larger capture-to-playout offset, video uses that one instead, so
// the two stay in sync.
//
// Order. The encoder emits no B-frames and every frame references the one
// before it, so frames are released strictly in order and a missing frame
// blocks everything behind it until it arrives or a keyframe does.
//
// Loss. A missing fragment is NACKed after a short reordering delay and again
// every ~1.5 RTT, a limited number of times. If the head frame is still
// missing maxWaitMs after its playout time, the buffer gives up on it and asks
// for a keyframe; a complete keyframe further on is always jumped to once its
// playout time comes.
// ---------------------------------------------------------------------------
class DelayEstimator {
  // o: minDelayMs, maxDelayMs, marginMs, quantile, windowMs, baseWindowMs, decay.
  constructor(o) {
    this.o = o;
    this.samples = [];          // [time, d] of units that arrived without retransmission
    this.baseMs = null;
    this.targetMs = o.minDelayMs;
  }

  add(now, d) {
    const o = this.o;
    this.samples.push([now, d]);
    while (now - this.samples[0][0] > o.baseWindowMs) this.samples.shift();
    let base = Infinity;
    for (const s of this.samples) if (s[1] < base) base = s[1];
    this.baseMs = base;
    const late = [];
    for (let i = this.samples.length - 1; i >= 0 && now - this.samples[i][0] <= o.windowMs; i--) late.push(this.samples[i][1] - base);
    late.sort((a, b) => a - b);
    const q = late[Math.min(late.length - 1, Math.floor(o.quantile * late.length))];
    const desired = Math.min(o.maxDelayMs, Math.max(o.minDelayMs, q + o.marginMs));
    this.targetMs = desired > this.targetMs ? desired : this.targetMs + (desired - this.targetMs) * o.decay;
  }

  // Playout time − capture time (it includes the unknown clock offset), or
  // null until something has arrived without retransmission.
  offsetMs() { return this.baseMs === null ? null : this.baseMs + this.targetMs; }
}

class JitterBuffer {
  constructor(opts) {
    this.o = Object.assign({
      frameIntervalMs: 1000 / 30,
      minDelayMs: 0,            // floor for the target delay
      maxDelayMs: 1000,
      marginMs: 10,
      quantile: 0.95,
      windowMs: 5000,           // lateness history behind the target
      baseWindowMs: 30000,      // history behind the baseline
      decay: 0.05,              // per frame, when the target should fall
      nack: true,
      nackReorderMs: 10,        // a gap younger than this may just be reordering
      nackMinIntervalMs: 30,
      nackMaxTries: 6,
      maxWaitMs: 300,           // how long past its playout time to wait for the head frame
      keyRequestIntervalMs: 500,
      maxFrames: 600,
      sendNack: () => {}, sendKeyRequest: () => {}, rttMs: () => 50,
    }, opts);
    this.frames = new Map();    // frameId -> frame, in id order (gaps are filled with placeholders)
    this.lastReleased = -1;
    this.lastCaptureMs = null;
    this.highest = -1;
    this.needKey = true;        // nothing decodable until a keyframe
    this.lastKeyReq = null;
    this.est = new DelayEstimator(this.o);
    this.syncOffsetMs = null;
    this.lastHoldMs = null;
    this.c = {
      packets: 0, duplicates: 0, latePackets: 0, framesComplete: 0, framesReleased: 0, framesSkipped: 0,
      framesAbandoned: 0, nacksSent: 0, nackItems: 0, keyRequests: 0, framesRecovered: 0,
    };
  }

  _newFrame(id) {
    return { id, fragCount: 0, frags: null, got: 0, maxIdx: -1, key: false, captureMs: null, bytes: 0,
      firstAt: null, completeAt: null, rtx: false, gapSince: null, lastNack: -Infinity, nackTries: 0 };
  }

  push(p, now) {
    this.c.packets++;
    if (this.lastKeyReq === null) this.lastKeyReq = now; // the first frame is a keyframe: give it time to arrive
    if (p.frameId <= this.lastReleased) { this.c.latePackets++; return; }
    if (p.frameId > this.highest) {
      // Frames we have seen nothing of yet get a placeholder, so they can be NACKed.
      const from = Math.max(this.highest, this.lastReleased, p.frameId - this.o.maxFrames) + 1;
      for (let id = from; id <= p.frameId; id++) this.frames.set(id, this._newFrame(id));
      this.highest = p.frameId;
      while (this.frames.size > this.o.maxFrames) {
        const oldest = this.frames.keys().next().value;
        this.frames.delete(oldest);
        this.lastReleased = oldest;
        this.needKey = true;
      }
    }
    const f = this.frames.get(p.frameId);
    if (!f) { this.c.latePackets++; return; }
    if (f.fragCount === 0) {
      f.fragCount = p.fragCount;
      f.frags = new Array(p.fragCount);
      f.key = p.key;
      f.captureMs = p.captureMs;
      f.firstAt = now;
    }
    if (p.fragIdx >= f.fragCount) return;
    if (f.frags[p.fragIdx]) { this.c.duplicates++; return; }
    f.frags[p.fragIdx] = p.payload;
    f.got++;
    f.bytes += p.payload.byteLength;
    f.maxIdx = Math.max(f.maxIdx, p.fragIdx);
    if (p.rtx) f.rtx = true;
    if (f.got === f.fragCount) {
      f.completeAt = now;
      this.c.framesComplete++;
      // A retransmitted frame is late because of loss, not jitter: letting it
      // into the estimate would grow the delay to cover a NACK round trip.
      if (f.rtx || f.nackTries > 0) this.c.framesRecovered++;
      else this.est.add(now, now - f.captureMs);
    }
  }

  ownOffsetMs() { return this.est.offsetMs(); }

  // The offset in use: this buffer's own or the audio buffer's, whichever is larger.
  offsetMs() {
    const own = this.est.offsetMs();
    if (own === null || this.syncOffsetMs === null) return own === null ? this.syncOffsetMs : own;
    return Math.max(own, this.syncOffsetMs);
  }

  // Until there is an offset (nothing has arrived without retransmission yet): play at once.
  _playoutAt(f) {
    const off = this.offsetMs();
    return off === null ? 0 : f.captureMs + off;
  }

  _firstCompleteKey() {
    for (const f of this.frames.values()) if (f.key && f.completeAt !== null) return f;
    return null;
  }

  _requestKey(now) {
    if (now - this.lastKeyReq < this.o.keyRequestIntervalMs) return;
    this.lastKeyReq = now;
    this.c.keyRequests++;
    this.o.sendKeyRequest(Math.max(0, this.highest));
  }

  forceKeyframe(now) {
    this.needKey = true;
    this.lastKeyReq = -Infinity;
    this._requestKey(now);
  }

  tick(now) {
    const o = this.o;
    if (!o.nack) return;
    // While waiting for a keyframe only that keyframe and what follows it is worth repairing.
    let usefulFrom = this.lastReleased + 1;
    if (this.needKey) {
      usefulFrom = Infinity;
      for (const f of this.frames.values()) if (f.key) { usefulFrom = f.id; break; }
    }
    const retryMs = Math.max(o.nackMinIntervalMs, 1.5 * o.rttMs());
    const items = [];
    for (const f of this.frames.values()) {
      if (f.completeAt !== null || f.id < usefulFrom) continue;
      const missing = [];
      if (f.fragCount === 0) {
        missing.push(Wire.WHOLE_FRAME);
      } else {
        // In the newest frame only holes below the highest fragment seen are
        // known to be missing; the rest may simply not have been sent yet.
        const limit = f.id < this.highest ? f.fragCount : f.maxIdx;
        for (let i = 0; i < limit; i++) if (!f.frags[i]) missing.push(i);
      }
      if (!missing.length) continue;
      if (f.gapSince === null) f.gapSince = now;
      if (now - f.gapSince < o.nackReorderMs || f.nackTries >= o.nackMaxTries || now - f.lastNack < retryMs) continue;
      f.lastNack = now;
      f.nackTries++;
      for (const idx of missing) items.push([f.id, idx]);
    }
    for (let i = 0; i < items.length; i += Wire.MAX_NACK_ITEMS) {
      const chunk = items.slice(i, i + Wire.MAX_NACK_ITEMS);
      this.c.nacksSent++;
      this.c.nackItems += chunk.length;
      o.sendNack(chunk);
    }
  }

  pop(now) {
    if (this.needKey) {
      const k = this._firstCompleteKey();
      if (k) return now >= this._playoutAt(k) ? this._release(k, now) : null;
      if (this.lastKeyReq !== null) this._requestKey(now);
      return null;
    }
    const head = this.frames.get(this.lastReleased + 1);
    if (head && head.completeAt !== null) return now >= this._playoutAt(head) ? this._release(head, now) : null;
    if (this.highest <= this.lastReleased) return null; // nothing newer has been sent yet
    // The head frame is late or lost. A complete keyframe further on ends the wait.
    const k = this._firstCompleteKey();
    if (k && now >= this._playoutAt(k)) return this._release(k, now);
    const captureMs = head && head.captureMs !== null ? head.captureMs : this.lastCaptureMs + this.o.frameIntervalMs;
    const off = this.offsetMs();
    const due = off === null ? now : captureMs + off;
    if (now > due + (this.o.nack ? this.o.maxWaitMs : 0)) {
      this.c.framesAbandoned++;
      this.needKey = true;
      this._requestKey(now);
    }
    return null;
  }

  _release(f, now) {
    for (const id of this.frames.keys()) {
      if (id > f.id) break;
      if (id < f.id) this.c.framesSkipped++;
      this.frames.delete(id);
    }
    this.lastReleased = f.id;
    this.lastCaptureMs = f.captureMs;
    this.needKey = false;
    this.c.framesReleased++;
    this.lastHoldMs = now - f.completeAt;
    const data = new Uint8Array(f.bytes);
    let off = 0;
    for (const frag of f.frags) { data.set(frag, off); off += frag.byteLength; }
    return { frameId: f.id, key: f.key, captureMs: f.captureMs, data, firstAt: f.firstAt, completeAt: f.completeAt,
      releasedAt: now, recovered: f.rtx || f.nackTries > 0 };
  }

  stats() {
    let complete = 0;
    for (const f of this.frames.values()) if (f.completeAt !== null) complete++;
    const own = this.est.offsetMs(), off = this.offsetMs();
    return { targetMs: this.est.targetMs, heldForAudioMs: own === null || off === null ? 0 : off - own, holdMs: this.lastHoldMs, framesBuffered: this.frames.size, framesReady: complete,
      waitingForKey: this.needKey, ...this.c };
  }
}
