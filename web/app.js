'use strict';

// ---------------------------------------------------------------------------
// The page: connects, feeds DATA packets to the video and audio jitter
// buffers, decodes what they release (WebCodecs) onto a canvas and the audio
// device, and reports packet arrivals back to the server. Chart, $, fmtNum and
// tilesHTML come from chart.js.
// ---------------------------------------------------------------------------
const S = { pc: null, timers: [] };
const canvas = $('video');
const ctx2d = canvas.getContext('2d');

// Wall clock in ms at sub-ms resolution.
const now = () => performance.timeOrigin + performance.now();

const charts = {
  bitrate: new Chart($('c-bitrate'), [
    { key: 'target', name: 'server target' },
    { key: 'sent', name: 'server sent' },
    { key: 'encoded', name: 'encoder out' },
    { key: 'recv', name: 'client received' },
    { key: 'rtx', name: 'retransmissions' },
    { key: 'audio', name: 'audio' },
    { key: 'layer', name: 'simulcast layer (nominal)' },
  ]),
  delay: new Chart($('c-delay'), [
    { key: 'e2e', name: 'capture → render' },
    { key: 'audioE2e', name: 'capture → heard (audio)' },
    { key: 'owd', name: 'network (server → client)' },
    { key: 'jbTarget', name: 'jitter buffer target' },
    { key: 'jbHold', name: 'time in jitter buffer' },
    { key: 'rtt', name: 'RTT' },
  ]),
  frames: new Chart($('c-frames'), [
    { key: 'rendered', name: 'rendered' },
    { key: 'encodedFps', name: 'server encoded' },
  ]),
};

function setStatus(s) { $('status').textContent = s; }

function logEvent(kind, value) {
  const li = document.createElement('li');
  const rel = S.t0 ? ((now() - S.t0) / 1000).toFixed(3) + 's' : '';
  li.textContent = `${rel} ${kind}: ${typeof value === 'object' ? JSON.stringify(value) : value}`;
  if (/fail|error|closed|freeze|abandon|keyframe request/i.test(`${kind} ${value}`)) li.classList.add('bad');
  const list = $('events');
  list.appendChild(li);
  while (list.children.length > 300) list.removeChild(list.firstChild);
  list.scrollTop = list.scrollHeight;
  sendCtrl('event', { kind, value });
}

function sendCtrl(type, data) {
  if (S.ctrl && S.ctrl.readyState === 'open') S.ctrl.send(JSON.stringify({ type, data: { tClient: now(), ...data } }));
}

function sendMedia(buf) {
  if (S.media && S.media.readyState === 'open') {
    try { S.media.send(buf); } catch { /* closing */ }
  }
}

$('start').onclick = start;
$('stop').onclick = () => stop('stopped');
$('mute').onchange = () => { if (S.player) S.player.setMuted($('mute').checked); };
const inputs = ['source', 'bitrate', 'minDelay', 'maxWait', 'nack', 'avsync', 'stun'];

// The sources this server offers. A simulcast source, when there is one, is
// the default: it is what the server was started for.
const SOURCE_NAMES = { file: 'Video file (encoded per viewer)', simulcast: 'Live simulcast (shared renditions)' };
fetch('info').then((r) => r.json()).then((info) => {
  const sources = info.sources || [];
  if (!sources.length) return;
  const sel = $('source');
  sel.innerHTML = '';
  for (const s of sources) sel.add(new Option(SOURCE_NAMES[s] || s, s));
  if (sources.includes('simulcast')) sel.value = 'simulcast';
}).catch(() => { /* the server's default source is used */ });

async function start() {
  if (!('VideoDecoder' in window)) {
    setStatus('WebCodecs is not available: use a current browser, over https:// or on localhost');
    return;
  }
  $('start').disabled = true;
  for (const id of inputs) $(id).disabled = true;
  $('events').innerHTML = '';
  for (const c of Object.values(charts)) c.reset();
  Object.assign(S, {
    t0: now(), hello: null, server: null, decoder: null, configured: false, sps: null, decodeMeta: new Map(), firstCaptureMs: null,
    layer: null, picture: null,
    arrivals: [], prevArrivals: [], fbSeq: 0, fbIntervalMs: 50,
    pings: [], clock: null, pingId: 1,
    firstSeq: null, maxSeq: -1,
    n: { packets: 0, bytes: 0, audioPackets: 0, rtxPackets: 0, rendered: 0, decodeErrors: 0, freezes: 0, freezeMs: 0 },
    win: { bytes: 0, audioBytes: 0, rendered: 0, e2e: [], offset: [], owd: [], decodeMs: [], at: now() },
    ajb: null, player: null, audioWarned: false, avsync: $('avsync').checked,
    lastRenderAt: null, firstRenderMs: null,
  });
  const frameIntervalMs = 1000 / 30; // corrected when the server's hello arrives
  S.jb = new JitterBuffer({
    frameIntervalMs,
    minDelayMs: Math.max(0, Number($('minDelay').value) || 0),
    maxWaitMs: Math.max(0, Number($('maxWait').value) || 0),
    nack: $('nack').checked,
    sendNack: (items) => sendMedia(Wire.nack(Wire.VIDEO, items)),
    sendKeyRequest: (frameId) => { logEvent('keyframe request', `at frame ${frameId}`); sendMedia(Wire.keyRequest(frameId)); },
    rttMs: () => (S.clock ? S.clock.rttMs : 50),
  });
  // The audio context has to be created here, inside the click.
  if ('AudioDecoder' in window) {
    try {
      S.player = new AudioPlayer({ onError: (why) => logEvent('audio decoder error', why) });
      S.player.setMuted($('mute').checked);
      S.ajb = new AudioJitterBuffer({
        minDelayMs: S.jb.o.minDelayMs,
        nack: S.jb.o.nack,
        sendNack: (items) => sendMedia(Wire.nack(Wire.AUDIO, items)),
        rttMs: S.jb.o.rttMs,
      });
    } catch (e) {
      logEvent('audio', `unavailable: ${e.message}`);
      S.player = S.ajb = null;
    }
  } else {
    logEvent('audio', 'unavailable: no AudioDecoder in this browser');
  }
  setStatus('connecting…');

  const stun = $('stun').value.trim();
  const pc = new RTCPeerConnection({ iceServers: stun ? [{ urls: stun }] : [] });
  S.pc = pc;
  // Unordered and never retransmitted: each message is a datagram on the ICE/DTLS path.
  S.media = pc.createDataChannel('media', { ordered: false, maxRetransmits: 0 });
  S.media.binaryType = 'arraybuffer';
  S.media.onmessage = (e) => onMedia(e.data);
  S.media.onopen = () => {
    logEvent('dataChannel', 'media open');
    ping();
    S.timers.push(setInterval(ping, 250));
    S.timers.push(setInterval(pump, 5));
    S.timers.push(setInterval(report, 500));
    feedbackLoop();
  };
  S.media.onclose = () => { if (S.pc === pc) stop('media channel closed'); };
  // Reliable: the server's hello and snapshots, our stats and events for its log.
  S.ctrl = pc.createDataChannel('ctrl');
  S.ctrl.onmessage = (e) => onCtrl(JSON.parse(e.data));

  pc.onconnectionstatechange = () => {
    logEvent('pcState', pc.connectionState);
    setStatus(pc.connectionState + (S.sessionId ? ` · session ${S.sessionId}` : ''));
    if (pc.connectionState === 'failed') stop('connection failed');
  };

  try {
    await pc.setLocalDescription(await pc.createOffer());
    await waitForGathering(pc, 3000);
    const kbps = Number($('bitrate').value);
    const res = await fetch('offer', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sdp: pc.localDescription.sdp, type: pc.localDescription.type, bitrate: kbps > 0 ? Math.round(kbps * 1000) : undefined,
        source: $('source').value || undefined,
        client: { userAgent: navigator.userAgent, timeOrigin: performance.timeOrigin, jitterBuffer: { ...S.jb.o, sendNack: undefined, sendKeyRequest: undefined, rttMs: undefined } },
      }),
    });
    const body = await res.json();
    if (!res.ok) throw new Error(body.error || res.statusText);
    S.sessionId = body.sessionId;
    S.logDir = body.logDir;
    await pc.setRemoteDescription({ type: body.type, sdp: body.sdp });
  } catch (err) {
    logEvent('error', err.message);
    stop('signaling error: ' + err.message);
    return;
  }
  $('stop').disabled = false;
}

function waitForGathering(pc, timeoutMs) {
  if (pc.iceGatheringState === 'complete') return Promise.resolve();
  return new Promise((resolve) => {
    const t = setTimeout(resolve, timeoutMs);
    pc.addEventListener('icegatheringstatechange', () => {
      if (pc.iceGatheringState === 'complete') { clearTimeout(t); resolve(); }
    });
  });
}

function stop(reason) {
  for (const t of S.timers) clearInterval(t);
  S.timers = [];
  clearTimeout(S.fbTimer);
  if (!S.pc) return;
  report();
  logEvent('stop', reason);
  const pc = S.pc;
  S.pc = null;
  if (S.decoder && S.decoder.state !== 'closed') S.decoder.close();
  if (S.player) S.player.close();
  // Let the last ctrl messages leave before the association goes away.
  setTimeout(() => pc.close(), 100);
  setStatus(`${reason}${S.logDir ? ` · server log ${S.logDir}` : ''}`);
  $('start').disabled = false;
  $('stop').disabled = true;
  for (const id of inputs) $(id).disabled = false;
}

function onCtrl(msg) {
  if (msg.type === 'hello') {
    S.hello = msg;
    S.fbIntervalMs = msg.feedbackIntervalMs || 50;
    S.jb.o.frameIntervalMs = 1000 / msg.media.fps;
    canvas.width = msg.media.width;
    canvas.height = msg.media.height;
    const a = msg.audio || {};
    if (!a.enabled && S.player) {
      S.player.close();
      S.player = S.ajb = null;
    }
    if (S.ajb) S.ajb.o.frameMs = S.player.o.frameMs = a.frameMs;
    logEvent('hello', `${msg.media.width}x${msg.media.height} @ ${msg.media.fps.toFixed(2)} fps, controller ${msg.config.Controller}, ${msg.config.StartBitrate / 1000} kbps, ` +
      (a.enabled ? `audio Opus ${a.bitrate / 1000} kbps` : 'no audio'));
    if (msg.simulcast) {
      logEvent('simulcast', msg.simulcast.layers.map((l) =>
        `${l.name} ${l.width}x${l.height} ${l.nominalBps / 1000} kbps${l.live ? '' : ' (not live)'}`).join(', '));
    }
  } else if (msg.type === 'server-stats') {
    S.server = msg.data;
    // Which rendition the server is forwarding. The switch itself needs nothing
    // from the page: it arrives as the next keyframe.
    const sim = msg.data.simulcast;
    if (sim && sim.layerIndex >= 0 && sim.layer !== S.layer) {
      logEvent('layer', S.layer ? `${S.layer} → ${sim.layer} (${sim.layerBps / 1000} kbps)` : `${sim.layer} (${sim.layerBps / 1000} kbps)`);
      S.layer = sim.layer;
    }
  }
}

// ---------------------------------------------------------------------------
// Receiving
// ---------------------------------------------------------------------------
function onMedia(buf) {
  const tRecv = now();
  const type = new Uint8Array(buf, 0, 1)[0];
  if (type === Wire.PONG) return onPong(Wire.parsePong(buf), tRecv);
  if (type !== Wire.DATA || buf.byteLength < Wire.DATA_HEADER) return;
  const p = Wire.parseData(buf);
  S.arrivals.push([p.seq, tRecv]);
  S.n.packets++;
  S.n.bytes += p.size;
  S.win.bytes += p.size;
  if (p.rtx) S.n.rtxPackets++;
  if (S.firstSeq === null) S.firstSeq = p.seq;
  S.maxSeq = Math.max(S.maxSeq, p.seq);
  if (S.clock) S.win.owd.push(tRecv + S.clock.offsetMs - p.sendMs);
  if (p.audio) {
    S.n.audioPackets++;
    S.win.audioBytes += p.size;
    if (S.ajb) S.ajb.push(p, tRecv);
  } else {
    S.jb.push(p, tRecv);
  }
  pump();
}

// pump runs on every arrival and every 5 ms: it lets the jitter buffers send
// NACKs and decodes everything whose playout time has come. For lip sync each
// buffer is told the capture-to-playout offset the other wants, and both use
// the larger.
function pump() {
  if (!S.pc) return;
  const t = now();
  if (S.ajb) {
    S.ajb.o.leadMs = 15 + S.player.latencyMs();
    if (S.avsync) S.ajb.syncOffsetMs = S.jb.ownOffsetMs();
    S.ajb.tick(t);
    for (let a = S.ajb.pop(t); a; a = S.ajb.pop(t)) S.player.play(a);
    if (S.avsync) S.jb.syncOffsetMs = S.ajb.offsetMs();
  }
  S.jb.tick(t);
  for (let f = S.jb.pop(t); f; f = S.jb.pop(t)) decode(f);
}

// Arrival reports. Each one is sent twice (in consecutive messages), because
// the channel is unreliable and a lost report would look like lost packets.
function feedbackLoop() {
  if (!S.pc) return;
  S.fbTimer = setTimeout(feedbackLoop, S.fbIntervalMs);
  const cur = S.arrivals;
  S.arrivals = [];
  const acks = S.prevArrivals.concat(cur);
  S.prevArrivals = cur;
  for (let i = 0; i < acks.length; i += Wire.MAX_FEEDBACK_ACKS) {
    sendMedia(Wire.feedback(S.fbSeq++, now(), acks.slice(i, i + Wire.MAX_FEEDBACK_ACKS)));
  }
}

// Clock offset, NTP style: the lowest-RTT ping of the last 10 s is the
// reference (error at most RTT/2). Only used for display and for NACK timing.
function ping() { sendMedia(Wire.ping(S.pingId++, now())); }
function onPong(p, t4) {
  const s = { t4, rttMs: (t4 - p.t1) - (p.t3 - p.t2), offsetMs: ((p.t2 - p.t1) + (p.t3 - t4)) / 2 };
  S.pings.push(s);
  while (t4 - S.pings[0].t4 > 10000) S.pings.shift();
  S.clock = S.pings.reduce((a, b) => (b.rttMs < a.rttMs ? b : a));
  S.lastRttMs = s.rttMs;
}

// ---------------------------------------------------------------------------
// Decoding and rendering
// ---------------------------------------------------------------------------
// The decoder is configured from the SPS (NAL type 7) of a keyframe. The codec
// string is avc1.PPCCLL = profile_idc, constraint flags, level_idc, the three
// bytes after the NAL header.
function spsOf(data) {
  for (let i = 0; i + 7 < data.length; i++) {
    if (data[i] === 0 && data[i + 1] === 0 && data[i + 2] === 1 && (data[i + 3] & 0x1f) === 7) {
      let end = i + 4;
      while (end + 2 < data.length && !(data[end] === 0 && data[end + 1] === 0 && data[end + 2] <= 1)) end++;
      if (end + 2 >= data.length) end = data.length;
      return data.subarray(i + 3, end);
    }
  }
  return null;
}
const codecFromSPS = (sps) => 'avc1.' + [sps[1], sps[2], sps[3]].map((b) => b.toString(16).padStart(2, '0')).join('');
const sameBytes = (a, b) => a && b && a.length === b.length && a.every((v, i) => v === b[i]);

function decoderFailed(why) {
  S.n.decodeErrors++;
  logEvent('decoder error', why);
  if (S.decoder && S.decoder.state !== 'closed') S.decoder.close();
  S.decoder = null;
  S.configured = false;
  S.decodeMeta.clear();
  if (S.pc) S.jb.forceKeyframe(now());
}

function decode(f) {
  if (!S.decoder) {
    const decoder = new VideoDecoder({
      output: (vf) => onDecoded(vf),
      error: (e) => { if (S.decoder === decoder) decoderFailed(e.message); },
    });
    S.decoder = decoder;
  }
  // A keyframe whose SPS differs from the one in use starts a different stream:
  // a simulcast session switched rendition, so the picture size (and perhaps
  // the level) changed. The decoder is configured again for it; frames it
  // still holds from before come out first.
  const sps = f.key ? spsOf(f.data) : null;
  if (!S.configured || (sps && !sameBytes(sps, S.sps))) {
    if (!sps) return decoderFailed('first frame is not a keyframe with an SPS');
    const codec = codecFromSPS(sps);
    // No `description`: the stream is Annex B and carries SPS/PPS in every keyframe.
    S.decoder.configure({ codec, optimizeForLatency: true });
    logEvent('decoder', S.configured ? `${codec} (new parameters at frame ${f.frameId})` : codec);
    S.configured = true;
    S.sps = sps;
  }
  if (S.firstCaptureMs === null) S.firstCaptureMs = f.captureMs;
  const timestamp = Math.round((f.captureMs - S.firstCaptureMs) * 1000);
  S.decodeMeta.set(timestamp, f);
  try {
    S.decoder.decode(new EncodedVideoChunk({ type: f.key ? 'key' : 'delta', timestamp, data: f.data }));
  } catch (e) {
    decoderFailed(e.message);
  }
}

function onDecoded(vf) {
  const t = now();
  const f = S.decodeMeta.get(vf.timestamp);
  S.decodeMeta.delete(vf.timestamp);
  // The canvas keeps the size the server announced; a smaller rendition is scaled up to it.
  ctx2d.drawImage(vf, 0, 0, canvas.width, canvas.height);
  S.picture = `${vf.displayWidth}×${vf.displayHeight}`;
  vf.close();
  if (!S.pc || !f) return;
  S.n.rendered++;
  S.win.rendered++;
  S.win.decodeMs.push(t - f.releasedAt);
  if (S.clock) S.win.e2e.push(t + S.clock.offsetMs - f.captureMs);
  S.win.offset.push(t - f.captureMs);
  if (S.firstRenderMs === null) {
    S.firstRenderMs = t - S.t0;
    logEvent('first frame', `${S.firstRenderMs.toFixed(0)} ms after Start`);
  }
  // A freeze: the picture stood still for much longer than a frame interval.
  const interval = S.jb.o.frameIntervalMs;
  if (S.lastRenderAt !== null) {
    const gap = t - S.lastRenderAt;
    if (gap > Math.max(3 * interval, interval + 100)) {
      S.n.freezes++;
      S.n.freezeMs += gap;
      logEvent('freeze', `${gap.toFixed(0)} ms before frame ${f.frameId}${f.key ? ' (keyframe)' : ''}`);
    }
  }
  S.lastRenderAt = t;
}

// ---------------------------------------------------------------------------
// Stats: tiles, charts, and a record for the server's client.jsonl.
// ---------------------------------------------------------------------------
const avg = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : null);
const maxOf = (a) => (a.length ? a.reduce((x, y) => Math.max(x, y)) : null);

function report() {
  if (!S.pc || !S.win) return;
  const t = now();
  const w = S.win;
  const dt = (t - w.at) / 1000;
  if (dt <= 0) return;
  S.win = { bytes: 0, audioBytes: 0, rendered: 0, e2e: [], offset: [], owd: [], decodeMs: [], at: t };
  const jb = S.jb.stats();
  // Audio: when each block is heard, on the same terms as video's capture → render.
  let audio = null;
  if (S.ajb) {
    const { played, ...player } = S.player.stats();
    const offset = avg(played.map(([captureMs, heardMs]) => heardMs - captureMs));
    const videoOffset = avg(w.offset);
    audio = {
      recvBitrate: w.audioBytes * 8 / dt,
      e2eMs: offset !== null && S.clock ? offset + S.clock.offsetMs : null,
      // Positive: the picture is behind the sound.
      videoBehindAudioMs: offset !== null && videoOffset !== null ? videoOffset - offset : null,
      player, jitterBuffer: S.ajb.stats(),
    };
    if (player.state !== 'running' && !S.audioWarned) {
      S.audioWarned = true;
      logEvent('audio', `output is ${player.state}: the browser has not allowed playback (press Start by hand)`);
    }
  }
  const expected = S.firstSeq === null ? 0 : S.maxSeq - S.firstSeq + 1;
  const stats = {
    recvBitrate: w.bytes * 8 / dt, renderedFps: w.rendered / dt,
    e2eMs: avg(w.e2e), e2eMaxMs: maxOf(w.e2e), owdMs: avg(w.owd), owdMaxMs: maxOf(w.owd), decodeMs: avg(w.decodeMs),
    rttMs: S.lastRttMs, clockOffsetMs: S.clock && S.clock.offsetMs, clockRttMs: S.clock && S.clock.rttMs,
    packetsExpected: expected, lossPct: expected ? 100 * (1 - S.n.packets / expected) : 0,
    firstRenderMs: S.firstRenderMs, picture: S.picture, ...S.n, jitterBuffer: jb, audio,
  };
  sendCtrl('stats', stats);

  const sv = S.server || {};
  const enc = sv.encoder || {};
  const sim = sv.simulcast;
  const k = (bps) => (bps === undefined || bps === null ? null : bps / 1000);
  charts.bitrate.push(t, { target: k(sv.targetBitrate), sent: k(sv.sendBitrate), encoded: k(enc.bitrate), recv: k(stats.recvBitrate), rtx: k(sv.rtxBitrate), audio: k(sv.audioBitrate), layer: sim ? k(sim.layerBps) : null });
  charts.delay.push(t, { e2e: stats.e2eMs, audioE2e: audio && audio.e2eMs, owd: stats.owdMs, jbTarget: jb.targetMs, jbHold: jb.holdMs, rtt: stats.rttMs });
  charts.frames.push(t, { rendered: stats.renderedFps, encodedFps: enc.fps });
  for (const c of Object.values(charts)) c.draw(t);
  $('tiles').innerHTML = tilesHTML([
    ['received', k(stats.recvBitrate), 'kbps'],
    ['server target', k(sv.targetBitrate), 'kbps'],
    ['rendered', stats.renderedFps, 'fps'],
    ['capture → render', stats.e2eMs, 'ms'],
    ['network delay', stats.owdMs, 'ms'],
    ['RTT', stats.rttMs, 'ms'],
    ['jitter buffer target', jb.targetMs, 'ms'],
    ['time in buffer', jb.holdMs, 'ms'],
    ['packet loss', stats.lossPct, '%'],
    ['retransmitted', S.n.rtxPackets, 'pkts'],
    ['NACKs sent', jb.nacksSent, ''],
    ['keyframe requests', jb.keyRequests, ''],
    ['frames skipped', jb.framesSkipped, ''],
    ['freezes', S.n.freezes, `· ${fmtNum(S.n.freezeMs)} ms`],
    ['pacer queue', sv.queueMs, 'ms'],
    ['SCTP buffered', sv.sctpBufferedBytes, 'bytes'],
    ...(sim ? [
      ['simulcast layer', sim.layer + (sim.pending ? ` → ${sim.pending}` : ''), sim.layerBps ? `· ${sim.layerBps / 1000} kbps` : ''],
      ['picture', S.picture || '–', ''],
      ['video budget', k(sim.budgetBps), 'kbps'],
      ['layer switches', sim.switches, ''],
      ['waits for a keyframe', sim.keyframeWaits, sim.waitingForKey ? '· waiting' : ''],
    ] : []),
    ...(audio ? [
      ['audio capture → heard', audio.e2eMs, 'ms'],
      ['video behind audio', audio.videoBehindAudioMs, 'ms'],
      ['audio buffer target', audio.jitterBuffer.targetMs, 'ms'],
      ['audio packets lost', audio.jitterBuffer.lost, `· ${audio.jitterBuffer.recovered} recovered`],
      ['audio late starts', audio.player.lateStarts, `· ${audio.player.reanchors} re-anchors`],
      ['audio level', audio.player.level * 100, '%'],
    ] : []),
  ]);
  $('t-client').textContent = JSON.stringify(stats, null, 1);
  $('t-server').textContent = JSON.stringify(sv, null, 1);
}
