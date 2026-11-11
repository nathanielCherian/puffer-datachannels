'use strict';

// ---------------------------------------------------------------------------
// The audio jitter buffer. Same shape as the video one (jitterbuffer.js):
//
//   push(packet, now)   an audio DATA packet arrived (one Opus packet)
//   tick(now)           send any NACKs that are due
//   pop(now)            the next packet to decode, if its time has come, else null
//   offsetMs()          playout time − capture time in use, or null
//   syncOffsetMs        set by the page: the offset the video buffer wants
//   stats()
//
// What this one does:
//
// Time. Packet ids are consecutive and each packet is frameMs long, so the
// capture time of any id, received or not, follows from one packet.
//
// Playout delay. The same estimate as video (DelayEstimator), plus leadMs: a
// packet is handed to the decoder leadMs before it should be heard, to leave
// time for decoding and for the audio device's own output delay. The desired
// offset is the larger of this buffer's and the video buffer's. The offset in use only follows it in steps (up at once,
// down once it is stepDownMs too high), because every change is an audible
// discontinuity.
//
// Loss. A missing packet is NACKed if a retransmission could still arrive in
// time, which needs a playout delay of about an RTT: on a clean path the
// target is only a few ms, so set minDelayMs if audio should be repaired. A
// packet that is not here when its time comes is skipped: the player leaves
// 20 ms of silence (WebCodecs exposes no Opus concealment).
// ---------------------------------------------------------------------------
class AudioJitterBuffer {
  constructor(opts) {
    this.o = Object.assign({
      frameMs: 20,
      minDelayMs: 0,
      maxDelayMs: 1000,
      marginMs: 15,
      leadMs: 30,               // decode + device output delay; the page keeps it up to date
      quantile: 0.99,           // a late audio packet is a glitch, a late video frame is not: aim higher
      windowMs: 5000,
      baseWindowMs: 30000,
      decay: 0.05,
      stepDownMs: 40,
      nack: true,
      nackReorderMs: 10,
      nackMinIntervalMs: 30,
      nackMaxTries: 3,
      maxPackets: 250,          // 5 s
      sendNack: () => {}, rttMs: () => 50,
    }, opts);
    this.est = new DelayEstimator(this.o);
    this.syncOffsetMs = null;
    this.applied = null;        // the offset in use
    this.packets = new Map();   // id -> { payload, at, rtx }
    this.missing = new Map();   // id -> { since, lastNack, tries }
    this.lastReleased = null;
    this.highest = null;
    this.anchor = null;         // [id, captureMs]
    this.c = { packets: 0, duplicates: 0, latePackets: 0, released: 0, lost: 0, recovered: 0, nacksSent: 0, nackItems: 0, offsetChanges: 0 };
  }

  _captureMs(id) { return this.anchor[1] + (id - this.anchor[0]) * this.o.frameMs; }

  push(p, now) {
    const id = p.frameId;
    this.c.packets++;
    if (this.anchor === null) {
      this.anchor = [id, p.captureMs];
      this.lastReleased = id - 1;
      this.highest = id - 1;
    }
    if (id <= this.lastReleased) { this.c.latePackets++; return; }
    if (this.packets.has(id)) { this.c.duplicates++; return; }
    if (id - this.lastReleased > this.o.maxPackets) {
      // Far ahead of what is playing (a long stall): start again from here.
      this.packets.clear();
      this.missing.clear();
      this.lastReleased = id - 1;
      this.highest = id - 1;
    }
    for (let m = this.highest + 1; m < id; m++) this.missing.set(m, { since: now, lastNack: -Infinity, tries: 0 });
    if (id > this.highest) this.highest = id;
    const wasMissing = this.missing.get(id);
    this.missing.delete(id);
    this.packets.set(id, { payload: p.payload, at: now });
    if (p.rtx || (wasMissing && wasMissing.tries > 0)) this.c.recovered++;
    else this.est.add(now, now - p.captureMs);
  }

  _ownOffsetMs() {
    const e = this.est.offsetMs();
    return e === null ? null : e + this.o.leadMs;
  }

  offsetMs() {
    const own = this._ownOffsetMs();
    let want = own;
    if (this.syncOffsetMs !== null && (own === null || this.syncOffsetMs > own)) want = this.syncOffsetMs;
    if (want !== null && (this.applied === null || want > this.applied || want < this.applied - this.o.stepDownMs)) {
      if (this.applied !== null) this.c.offsetChanges++;
      this.applied = want;
    }
    return this.applied;
  }

  tick(now) {
    const o = this.o;
    const off = this.offsetMs();
    if (!o.nack || off === null || !this.missing.size) return;
    const rtt = o.rttMs();
    const retryMs = Math.max(o.nackMinIntervalMs, 1.5 * rtt);
    const items = [];
    for (const [id, m] of this.missing) {
      if (id <= this.lastReleased) { this.missing.delete(id); continue; }
      const deadline = this._captureMs(id) + off - o.leadMs;
      if (now + rtt > deadline || now - m.since < o.nackReorderMs || m.tries >= o.nackMaxTries || now - m.lastNack < retryMs) continue;
      m.lastNack = now;
      m.tries++;
      items.push([id, 0]);
    }
    if (items.length) {
      this.c.nacksSent++;
      this.c.nackItems += items.length;
      o.sendNack(items.slice(0, Wire.MAX_NACK_ITEMS));
    }
  }

  pop(now) {
    const off = this.offsetMs();
    while (this.lastReleased !== null && this.lastReleased < this.highest) {
      const id = this.lastReleased + 1;
      const captureMs = this._captureMs(id);
      const playoutAt = off === null ? now + this.o.leadMs : captureMs + off;
      if (now < playoutAt - this.o.leadMs) return null;
      this.lastReleased = id;
      const pkt = this.packets.get(id);
      if (!pkt) { this.c.lost++; this.missing.delete(id); continue; }
      this.packets.delete(id);
      this.c.released++;
      return { id, captureMs, playoutAt, payload: pkt.payload, heldMs: now - pkt.at };
    }
    return null;
  }

  stats() {
    const own = this._ownOffsetMs();
    return { targetMs: this.est.targetMs, heldForVideoMs: own === null || this.applied === null ? 0 : this.applied - own,
      leadMs: this.o.leadMs, buffered: this.packets.size, missing: this.missing.size, ...this.c };
  }
}

// ---------------------------------------------------------------------------
// The audio player: decodes the packets the buffer releases (WebCodecs
// AudioDecoder) and schedules each 20 ms block on the Web Audio clock at the
// wall-clock time the buffer asked for. Consecutive blocks are placed
// back to back, sample-exact. A packet released too late to start on time
// pushes the schedule back a little, and it stays there: pulling it forward
// again would mean cutting or overlapping sound. The schedule is only
// re-anchored when it is more than earlyMs ahead of, or lateMs behind, where
// the buffer wants it (the offset changed, late packets added up, or the
// audio and wall clocks drifted apart).
// ---------------------------------------------------------------------------
class AudioPlayer {
  // Must be constructed from a user gesture, or the browser keeps it suspended.
  constructor(opts) {
    this.o = Object.assign({ sampleRate: 48000, channels: 2, frameMs: 20, earlyMs: 10, lateMs: 40, onError: () => {} }, opts);
    this.ctx = new AudioContext({ sampleRate: this.o.sampleRate, latencyHint: 'interactive' });
    this.gain = this.ctx.createGain();
    this.analyser = this.ctx.createAnalyser();
    this.analyser.fftSize = 2048;
    this.analyser.connect(this.gain).connect(this.ctx.destination); // the level is measured before the mute
    this.levelBuf = new Float32Array(this.analyser.fftSize);
    this.ctx.resume().catch(() => {});
    this.pending = [];          // packets given to the decoder, oldest first
    this.next = null;           // context time where the next consecutive block goes
    this.nextId = null;
    this.clockDelta = null;     // context time (s) − performance.now() (s), smoothed
    this.played = [];           // [captureMs, wall ms it is heard], since the last stats()
    this.c = { decoded: 0, scheduled: 0, notRunning: 0, lateStarts: 0, reanchors: 0, gaps: 0, decodeErrors: 0, oddOutputs: 0 };
    this.decoder = new AudioDecoder({
      output: (ad) => this._onDecoded(ad),
      error: (e) => { this.c.decodeErrors++; this.o.onError(e.message); },
    });
    // Raw Opus packets, mono or stereo: no description needed.
    this.decoder.configure({ codec: 'opus', sampleRate: this.o.sampleRate, numberOfChannels: this.o.channels });
  }

  setMuted(m) { this.gain.gain.value = m ? 0 : 1; }

  // How long before it is heard a block must be scheduled: the device's output delay.
  latencyMs() { return 1000 * (this.ctx.outputLatency || this.ctx.baseLatency || 0); }

  // pkt: what AudioJitterBuffer.pop returned.
  play(pkt) {
    if (this.decoder.state !== 'configured') return;
    this.pending.push(pkt);
    try {
      this.decoder.decode(new EncodedAudioChunk({ type: 'key', timestamp: Math.round(pkt.id * this.o.frameMs * 1000),
        duration: this.o.frameMs * 1000, data: pkt.payload }));
    } catch (e) {
      this.pending.pop();
      this.c.decodeErrors++;
      this.o.onError(e.message);
    }
  }

  // Context time at which a sample is heard at wall-clock time wallMs.
  // getOutputTimestamp pairs the sample now leaving the device with the
  // performance clock, so the device's output delay is accounted for.
  _contextTime(wallMs) {
    const ts = this.ctx.getOutputTimestamp();
    let delta;
    if (ts.performanceTime > 0 && ts.contextTime > 0) delta = ts.contextTime - ts.performanceTime / 1000;
    else delta = this.ctx.currentTime - this.latencyMs() / 1000 - performance.now() / 1000;
    if (this.clockDelta === null || Math.abs(delta - this.clockDelta) > 0.1) this.clockDelta = delta;
    else this.clockDelta += 0.02 * (delta - this.clockDelta);
    return this.clockDelta + (wallMs - performance.timeOrigin) / 1000;
  }

  _onDecoded(ad) {
    // The decoder gives one block per packet, in order.
    const pkt = this.pending.shift();
    const frames = ad.numberOfFrames;
    if (!pkt || this.ctx.state === 'closed') { ad.close(); return; }
    if (frames !== Math.round(this.o.sampleRate * this.o.frameMs / 1000)) this.c.oddOutputs++;
    const buf = this.ctx.createBuffer(this.o.channels, frames, ad.sampleRate);
    for (let ch = 0; ch < this.o.channels; ch++) {
      ad.copyTo(buf.getChannelData(ch), { planeIndex: Math.min(ch, ad.numberOfChannels - 1), format: 'f32-planar' });
    }
    ad.close();
    this.c.decoded++;
    if (this.ctx.state !== 'running') {
      // Suspended (the browser wants a user gesture) or interrupted: its clock is stopped.
      this.c.notRunning++;
      this.next = null;
      return;
    }

    const ideal = this._contextTime(pkt.playoutAt);
    if (this.next !== null && pkt.id !== this.nextId) {
      this.c.gaps++;
      this.next += (pkt.id - this.nextId) * this.o.frameMs / 1000; // silence where packets are missing
    }
    if (this.next === null || this.next < ideal - this.o.earlyMs / 1000 || this.next > ideal + this.o.lateMs / 1000) {
      if (this.next !== null) this.c.reanchors++;
      this.next = ideal;
    }
    const earliest = this.ctx.currentTime + 0.002;
    if (this.next < earliest) {
      this.c.lateStarts++;
      this.next = earliest;
    }
    const src = this.ctx.createBufferSource();
    src.buffer = buf;
    src.connect(this.analyser);
    src.start(this.next);
    this.played.push([pkt.captureMs, (this.next - this.clockDelta) * 1000 + performance.timeOrigin]);
    this.next += buf.duration;
    this.nextId = pkt.id + 1;
    this.c.scheduled++;
  }

  // RMS of what is being played right now (muted or not), 0..1.
  level() {
    this.analyser.getFloatTimeDomainData(this.levelBuf);
    let sum = 0;
    for (const v of this.levelBuf) sum += v * v;
    return Math.sqrt(sum / this.levelBuf.length);
  }

  stats() {
    const played = this.played;
    this.played = [];
    return { state: this.ctx.state, outputLatencyMs: this.latencyMs(), level: this.level(), played, ...this.c };
  }

  close() {
    if (this.decoder.state !== 'closed') this.decoder.close();
    if (this.ctx.state !== 'closed') this.ctx.close();
  }
}
