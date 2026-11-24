'use strict';

// ---------------------------------------------------------------------------
// The congestion-control page. The server sends junk as fast as a congestion
// controller lets it: Copa, CUBIC, BBR, Vegas or a slow start repeated every
// second (internal/copa and so on);
// this page is the receiver. It has two jobs:
//
//   1. Acknowledge every DATA packet: the moment it arrives, or in one
//      message per interval (see flushAcks). The controllers are clocked by
//      these acks and take an RTT sample from each packet.
//   2. Measure what arrives: throughput, one-way delay, queueing delay, jitter,
//      loss, reordering. Everything under "Measuring" is from the packets
//      themselves and owes nothing to what the server says about itself.
//
// Chart, $, fmtNum and tilesHTML come from chart.js, Wire from wire.js.
// ---------------------------------------------------------------------------
const S = { pc: null, timers: [], rows: [] };

// Wall clock in ms at sub-ms resolution.
const now = () => performance.timeOrigin + performance.now();

// Every arrival is named in this many consecutive acks, so that an ack lost
// on the way back (the channel is unreliable) does not look like a lost packet.
const ACK_REPEAT = 4;
const REPORT_MS = 200;
// One-way delays above the lowest seen, for percentiles over the whole run:
// 0.5 ms buckets up to 5 s.
const HIST_STEP_MS = 0.5, HIST_BUCKETS = 10000;

// The delay and window charts show what the running controller has: Copa's
// RTTstanding, velocity and 1/δ, CUBIC's ssthresh and W_max, BBR's bandwidth
// estimate and its bounds on what is in flight, Vegas's ssthresh and diff.
//
// The rate, delay and window charts also carry two kinds of vertical bar, so
// that cause and effect can be read off the same time axis:
//   loss   a packet this page never received, at the moment its absence
//          showed (a later packet arrived in its place); see noteGaps
//   cut    the sender reduced its window: a CUBIC or Vegas congestion event,
//          Copa (competitive) raising δ, BBR lowering one of its bounds after
//          loss, or a timeout. From the sender's snapshots, so within 100 ms
//          of when it happened.
const NAMES = { copa: 'Copa', cubic: 'CUBIC', bbr: 'BBR', vegas: 'Vegas', slowstart: 'Slow start, repeated' };
const MARKS = [
  { key: 'loss', name: 'loss (client)', color: '#d6336c' },
  { key: 'cut', name: 'sender cut its window', color: '#6b6b70', dash: [4, 3], width: 1.5 },
];
function makeCharts(algorithm) {
  const copa = algorithm === 'copa', bbr = algorithm === 'bbr';
  const marks = { marks: MARKS };
  const windowLines = {
    copa: [{ key: 'velocity', name: 'velocity v' }, { key: 'invDelta', name: '1/δ' }],
    cubic: [{ key: 'ssthresh', name: 'ssthresh' }, { key: 'wMax', name: 'W_max' }],
    bbr: [{ key: 'bdp', name: 'BDP (bw · min_rtt)' }, { key: 'inflightLongterm', name: 'inflight_longterm' },
      { key: 'inflightShortterm', name: 'inflight_shortterm' }],
    vegas: [{ key: 'ssthresh', name: 'ssthresh' }, { key: 'diff', name: 'diff (packets queued)' }],
    slowstart: [{ key: 'peak', name: 'window the lost packet left at' }],
  };
  return {
    rate: new Chart($('c-rate'), [
      { key: 'recv', name: 'received (client)' },
      { key: 'sent', name: 'sent' },
      { key: 'window', name: copa ? 'cwnd / RTTstanding (λ)' : bbr ? 'bandwidth estimate (bw)' : 'cwnd / smoothed RTT' },
      ...(bbr ? [{ key: 'maxBw', name: 'max_bw' }, { key: 'pacing', name: 'pacing rate' }] : []),
    ], marks),
    delay: new Chart($('c-delay'), [
      { key: 'owd', name: 'one-way delay, mean (client)' },
      { key: 'owdP95', name: 'one-way delay, p95 (client)' },
      { key: 'queue', name: 'queueing delay, mean (client)' },
      ...(copa ? [{ key: 'standing', name: 'RTTstanding' }] : []),
      { key: 'rttMin', name: 'RTTmin' },
      { key: 'srtt', name: 'smoothed RTT' },
    ], marks),
    window: new Chart($('c-window'), [
      { key: 'cwnd', name: 'cwnd' },
      { key: 'inFlight', name: 'in flight' },
      ...(windowLines[algorithm] || []),
    ], marks),
    loss: new Chart($('c-loss'), [
      { key: 'client', name: 'missing sequence numbers (client)' },
      { key: 'server', name: 'declared lost by the sender' },
    ]),
  };
}
let charts = makeCharts('copa');

function setStatus(s) { $('status').textContent = s; }

function logEvent(kind, value) {
  const li = document.createElement('li');
  const rel = S.t0 ? ((now() - S.t0) / 1000).toFixed(3) + 's' : '';
  li.textContent = `${rel} ${kind}: ${typeof value === 'object' ? JSON.stringify(value) : value}`;
  if (/fail|error|closed|timeout|competitive|congestion event/i.test(`${kind} ${value}`)) li.classList.add('bad');
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
    try { S.media.send(buf); return true; } catch { /* closing */ }
  }
  return false;
}

$('start').onclick = start;
$('stop').onclick = () => stop('stopped');
$('download').onclick = downloadCSV;
const inputs = ['algorithm', 'maxRate', 'ackMs', 'mode', 'delta', 'duration', 'stun'];

// Show the server's values where a blank field would use them.
let serverAlgorithm = 'copa';
fetch('info').then((r) => r.json()).then(({ config }) => {
  serverAlgorithm = config.CCAlgorithm;
  $('algorithm').options[0].textContent = `server default (${config.CCAlgorithm})`;
  $('maxRate').placeholder = (config.CCMaxRate / 1e6).toString();
  $('maxRate').max = config.CCMaxRate / 1e6;
  $('delta').placeholder = config.CopaDelta;
  $('mode').options[0].textContent = `server default (${config.CopaMode})`;
}).catch(() => {});

async function start() {
  $('start').disabled = true;
  $('download').disabled = true;
  for (const id of inputs) $(id).disabled = true;
  $('events').innerHTML = '';
  charts = makeCharts($('algorithm').value || serverAlgorithm);
  const t = now();
  Object.assign(S, {
    t0: t, hello: null, server: null, serverAt: null, sessionId: null, logDir: null, rows: [],
    recent: [], pending: [], prevBatch: [], ackMs: Number($('ackMs').value) > 0 ? Number($('ackMs').value) : 0,
    fbSeq: 0, acksSent: 0, ackErrors: 0,
    pings: [], clock: null, pingId: 1, lastRttMs: null,
    // Totals for the run.
    n: { packets: 0, bytes: 0, late: 0, duplicates: 0, lossMarks: 0 },
    firstSeq: null, maxSeq: -1, firstAt: null, lastAt: null,
    minTransit: Infinity, lastTransit: null, jitterMs: 0,
    hist: new Uint32Array(HIST_BUCKETS), histN: 0,
    seen: new Set(), lastMode: null, lastSlowStart: null, lastEvents: 0, lastTimeouts: 0, lastSpurious: 0,
    capWarned: false, prevAcks: null, lastDelta: null, lastBbrState: null, lastBbrCuts: 0, lastTooHigh: 0,
    gaps: new Map(), // sequence numbers skipped over and not yet arrived -> when the skip showed
    // The interval being measured, and where the previous one ended.
    win: newWindow(t), prev: { packets: 0, expected: 0, serverLost: 0, serverResolved: 0 },
  });
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
    S.timers.push(setInterval(report, REPORT_MS));
    if (S.ackMs > 0) S.timers.push(setInterval(flushAcks, S.ackMs));
  };
  S.media.onclose = () => { if (S.pc === pc) stop('media channel closed'); };
  // Reliable: the server's hello and snapshots, our measurements for its log.
  S.ctrl = pc.createDataChannel('ctrl');
  S.ctrl.onmessage = (e) => { if (S.pc === pc) onCtrl(JSON.parse(e.data)); };

  pc.onconnectionstatechange = () => {
    logEvent('pcState', pc.connectionState);
    if (S.pc === pc) setStatus(pc.connectionState + (S.sessionId ? ` · session ${S.sessionId}` : ''));
    if (pc.connectionState === 'failed') stop('connection failed');
  };

  try {
    await pc.setLocalDescription(await pc.createOffer());
    await waitForGathering(pc, 3000);
    const num = (id) => { const v = Number($(id).value); return v > 0 ? v : undefined; };
    const mbps = num('maxRate');
    const res = await fetch('cc/offer', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sdp: pc.localDescription.sdp, type: pc.localDescription.type, algorithm: $('algorithm').value || undefined,
        maxRate: mbps ? Math.round(mbps * 1e6) : undefined, mode: $('mode').value || undefined,
        delta: num('delta'), durationSec: num('duration'),
        client: { userAgent: navigator.userAgent, timeOrigin: performance.timeOrigin, ackRepeat: ACK_REPEAT, ackMs: S.ackMs, reportMs: REPORT_MS },
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
  if (!S.pc) return;
  report();
  const total = totals(now());
  logEvent('stop', reason);
  logEvent('totals', total);
  sendCtrl('totals', total);
  const pc = S.pc;
  S.pc = null;
  // Let the last ctrl messages leave before the association goes away.
  setTimeout(() => pc.close(), 100);
  setStatus(`${reason}${S.logDir ? ` · server log ${S.logDir}` : ''}`);
  $('start').disabled = false;
  $('stop').disabled = true;
  $('download').disabled = S.rows.length === 0;
  for (const id of inputs) $(id).disabled = false;
}

function onCtrl(msg) {
  if (msg.type === 'hello') {
    S.hello = msg;
    const c = msg.cc;
    logEvent('hello', (c.algorithm === 'copa' ? `Copa mode ${c.mode}, δ ${c.delta}` : NAMES[c.algorithm] || c.algorithm) +
      `, at most ${c.maxRate / 1e6} Mbps, ${c.packetBytes}-byte packets` + (c.durationSec ? `, for ${c.durationSec} s` : '') +
      (S.ackMs > 0 ? `, acks every ${S.ackMs} ms` : ', an ack per packet'));
  } else if (msg.type === 'server-stats') {
    onServerStats(msg.data);
  } else if (msg.type === 'closed') {
    stop(`server closed the session: ${msg.reason}`);
  }
}

// ---------------------------------------------------------------------------
// Receiving and acknowledging
// ---------------------------------------------------------------------------
function onMedia(buf) {
  const tRecv = now();
  const d = new DataView(buf);
  const type = d.getUint8(0);
  if (type === Wire.PONG) return onPong(Wire.parsePong(buf), tRecv);
  if (type !== Wire.DATA || buf.byteLength < Wire.DATA_HEADER || !S.pc) return;
  const seq = d.getUint32(2);
  const sendMs = Number(d.getBigInt64(6)) / 1000; // server clock

  if (S.ackMs > 0) {
    // Batched: flushAcks sends this arrival with the others of its interval.
    S.pending.push([seq, tRecv]);
  } else {
    // The ack first: the sender's RTT sample should carry as little of this
    // page's own time as possible. It names this arrival and the few before
    // it, each with when it arrived, so the sender can subtract the hold
    // exactly.
    S.recent.push([seq, tRecv]);
    if (S.recent.length > ACK_REPEAT) S.recent.shift();
    if (sendMedia(Wire.feedback(S.fbSeq++, now(), S.recent))) S.acksSent++; else S.ackErrors++;
  }

  measure(seq, sendMs, tRecv, buf.byteLength);
}

// Batched acks ("Acknowledge every" in Settings): one message per interval
// instead of one per packet. It names every arrival of the interval with its
// own arrival time, so the sender still gets an exact RTT sample for each
// packet; what the sender loses is the timing, since a window's worth of
// room now opens at once and the controller hears of congestion up to an
// interval late. The interval before is named again, so one lost message
// does not look like a run of lost packets.
function flushAcks() {
  if (!S.pending.length) return;
  const batch = S.pending;
  S.pending = [];
  const all = S.prevBatch.concat(batch); // oldest first: the sender wants the newest last
  for (let i = 0; i < all.length; i += Wire.MAX_FEEDBACK_ACKS) {
    if (sendMedia(Wire.feedback(S.fbSeq++, now(), all.slice(i, i + Wire.MAX_FEEDBACK_ACKS)))) S.acksSent++; else S.ackErrors++;
  }
  S.prevBatch = batch;
}

// Clock offset, NTP style: the lowest-RTT ping of the last 10 s is the
// reference (error at most RTT/2). Only the absolute one-way delay needs it.
function ping() { sendMedia(Wire.ping(S.pingId++, now())); }
function onPong(p, t4) {
  if (!S.pc) return;
  const s = { t4, rttMs: (t4 - p.t1) - (p.t3 - p.t2), offsetMs: ((p.t2 - p.t1) + (p.t3 - t4)) / 2 };
  S.pings.push(s);
  while (t4 - S.pings[0].t4 > 10000) S.pings.shift();
  S.clock = S.pings.reduce((a, b) => (b.rttMs < a.rttMs ? b : a));
  S.lastRttMs = s.rttMs;
}

// ---------------------------------------------------------------------------
// Measuring
// ---------------------------------------------------------------------------
function newWindow(at) {
  return { at, packets: 0, bytes: 0, owd: [], queue: [], gapMaxMs: 0, late: 0 };
}

function measure(seq, sendMs, tRecv, size) {
  const n = S.n, w = S.win;
  // The channel never retransmits, so a repeat would be the network's doing.
  // Remember recent sequence numbers only: enough to catch one.
  if (S.seen.has(seq)) { n.duplicates++; return; }
  S.seen.add(seq);
  if (S.seen.size > 4096) S.seen.delete(S.seen.values().next().value);

  n.packets++; n.bytes += size;
  w.packets++; w.bytes += size;
  if (S.firstSeq === null) { S.firstSeq = seq; S.firstAt = tRecv; }
  // Late: something sent after it arrived before it (reordering). Ahead: it
  // skipped some, which are lost unless they turn up (noteGaps decides).
  if (seq > S.maxSeq) {
    if (S.maxSeq >= 0 && seq - S.maxSeq > 1 && seq - S.maxSeq < 5000) {
      for (let missing = S.maxSeq + 1; missing < seq; missing++) S.gaps.set(missing, tRecv);
    }
    S.maxSeq = seq;
  } else {
    n.late++; w.late++;
    S.gaps.delete(seq);
  }
  if (S.lastAt !== null) w.gapMaxMs = Math.max(w.gapMaxMs, tRecv - S.lastAt);
  S.lastAt = tRecv;

  // Transit is receive time (our clock) minus send time (the server's): off by
  // the unknown clock offset, but its changes are real. So:
  //   jitter   RFC 3550's smoothed |transit difference| between arrivals
  //   queueing transit above the lowest transit seen: the delay the path added
  //            beyond its best, which is what Copa is trying to keep small
  //   one-way  transit corrected by the ping-derived clock offset
  const transit = tRecv - sendMs;
  if (S.lastTransit !== null) S.jitterMs += (Math.abs(transit - S.lastTransit) - S.jitterMs) / 16;
  S.lastTransit = transit;
  if (transit < S.minTransit) S.minTransit = transit;
  const queue = transit - S.minTransit;
  w.queue.push(queue);
  S.hist[Math.min(HIST_BUCKETS - 1, Math.floor(queue / HIST_STEP_MS))]++;
  S.histN++;
  if (S.clock) w.owd.push(transit + S.clock.offsetMs);
}

// noteGaps turns skipped sequence numbers into loss marks once they have had
// GAP_WAIT_MS to arrive late. The mark goes at the time the gap appeared, not
// at the time it was confirmed, so it lines up with the delay and the window
// at the moment of the loss.
const GAP_WAIT_MS = 300;
function noteGaps(t) {
  for (const [seq, at] of S.gaps) {
    if (t - at < GAP_WAIT_MS) break; // insertion order is time order
    S.gaps.delete(seq);
    S.n.lossMarks++;
    for (const c of [charts.rate, charts.delay, charts.window]) c.mark('loss', at);
  }
}
function markCut(t) { for (const c of [charts.rate, charts.delay, charts.window]) c.mark('cut', t); }

const avg = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : null);
// pct sorts its argument.
function pct(a, p) {
  if (!a.length) return null;
  a.sort((x, y) => x - y);
  return a[Math.min(a.length - 1, Math.floor(p * a.length))];
}
function histPct(p) {
  if (!S.histN) return null;
  let left = p * S.histN;
  for (let i = 0; i < HIST_BUCKETS; i++) {
    left -= S.hist[i];
    if (left < 0) return (i + 0.5) * HIST_STEP_MS;
  }
  return HIST_BUCKETS * HIST_STEP_MS;
}
const round = (v, digits = 3) => (v === null || v === undefined || !Number.isFinite(v) ? null : Number(v.toFixed(digits)));

// Totals since the first packet.
function totals(t) {
  const n = S.n;
  const expected = S.firstSeq === null ? 0 : S.maxSeq - S.firstSeq + 1;
  const secs = S.firstAt === null ? 0 : ((S.lastAt || t) - S.firstAt) / 1000;
  return {
    seconds: round(secs), packets: n.packets, bytes: n.bytes,
    meanMbps: secs > 0 ? round(n.bytes * 8 / secs / 1e6) : null,
    packetsExpected: expected, packetsMissing: Math.max(0, expected - n.packets),
    lossPct: expected ? round(100 * Math.max(0, 1 - n.packets / expected)) : null,
    late: n.late, duplicates: n.duplicates,
    queueDelayP50Ms: histPct(0.5), queueDelayP95Ms: histPct(0.95), queueDelayP99Ms: histPct(0.99),
    jitterMs: round(S.jitterMs), acksSent: S.acksSent, ackErrors: S.ackErrors,
  };
}

// report closes the current interval: one row of metrics, to the tiles, the
// charts, the CSV and the server's client.jsonl.
function report() {
  if (!S.pc) return;
  const t = now();
  const w = S.win;
  const dt = (t - w.at) / 1000;
  if (dt <= 0) return;
  S.win = newWindow(t);
  noteGaps(t);

  // Loss over the interval: of the sequence numbers that became due (up to
  // the highest seen), how many did not arrive. A packet that turns up late
  // counts for the interval it arrives in, so this can undershoot then.
  const expected = S.firstSeq === null ? 0 : S.maxSeq - S.firstSeq + 1;
  const due = expected - S.prev.expected, got = S.n.packets - S.prev.packets;
  S.prev.expected = expected; S.prev.packets = S.n.packets;
  const lossPct = due > 0 ? 100 * Math.max(0, 1 - got / due) : (got > 0 ? 0 : null);

  // ctl is the controller's own state; cp, cu, bb and vg are it by name, all
  // but one of them empty, so that a row has the same columns whichever is
  // running.
  const sv = S.server || {}, ctl = sv[sv.algorithm] || {}, cp = sv.copa || {}, cu = sv.cubic || {};
  const bb = sv.bbr || {}, vg = sv.vegas || {}, ss = sv.slowstart || {};
  const mbps = (bps) => (bps === undefined || bps === null ? null : bps / 1e6);
  // A controller's rate in packets per second, as Mbps of DataChannel messages.
  const pps = (v) => (v === undefined || v === null ? null : v * (sv.packetBytes || 1150) * 8 / 1e6);
  const row = {
    tSec: round((t - S.t0) / 1000), algorithm: sv.algorithm ?? null,
    // Client: measured here.
    recvMbps: round(w.bytes * 8 / dt / 1e6), recvPackets: w.packets, lossPct: round(lossPct),
    owdMeanMs: round(avg(w.owd)), owdP50Ms: round(pct(w.owd, 0.5)), owdP95Ms: round(pct(w.owd, 0.95)), owdMaxMs: round(pct(w.owd, 1)),
    queueMeanMs: round(avg(w.queue)), queueP95Ms: round(pct(w.queue, 0.95)), queueMaxMs: round(pct(w.queue, 1)),
    jitterMs: round(S.jitterMs), gapMaxMs: round(w.gapMaxMs), late: w.late,
    pingRttMs: round(S.lastRttMs), clockOffsetMs: round(S.clock && S.clock.offsetMs), tabHidden: document.hidden,
    // Server: the sender's latest snapshot, for reading the two side by side.
    srvAgeMs: S.serverAt ? round(t - S.serverAt, 0) : null,
    srvSentMbps: round(mbps(sv.sendBitrate)), srvDeliveredMbps: round(mbps(sv.deliveryBitrate)), srvLossPct: round(sv.lossPct),
    cwnd: round(sv.cwnd), inFlight: sv.inFlight ?? null, slowStart: ctl.slowStart ?? null,
    rttMinMs: round(ctl.rttMinMs), srttMs: round(sv.srttMs),
    windowMbps: round(mbps(sv.windowBitrate)), pacingMbps: round(mbps(sv.pacingBitrate)),
    // Copa only.
    velocity: cp.velocity ?? null, delta: round(cp.delta, 4), mode: cp.mode ?? null,
    rttStandingMs: round(cp.rttStandingMs), copaQueueMs: round(cp.queueDelayMs), targetMbps: round(mbps(sv.targetBitrate)),
    // CUBIC only.
    cubicState: cu.state ?? null, ssthresh: round(cu.ssthresh ?? vg.ssthresh), wMax: round(cu.wMax), cubicK: round(cu.k),
    congestionEvents: ctl.counters ? ctl.counters.CongestionEvents ?? null : null,
    appLimitedAcks: ctl.counters ? ctl.counters.AppLimitedAcks ?? null : null,
    // BBR only. Rates as Mbps, like the others.
    bbrState: bb.state ?? null, bwMbps: round(pps(bb.bw)), maxBwMbps: round(pps(bb.maxBw)), bwShorttermMbps: round(pps(bb.bwShortterm)),
    bdp: round(bb.bdp), inflightLongterm: round(bb.inflightLongterm), inflightShortterm: round(bb.inflightShortterm),
    pacingGain: bb.pacingGain ?? null, cwndGain: bb.cwndGain ?? null, extraAcked: round(bb.extraAcked),
    bbrRound: bb.round ?? null, bbrProbes: bb.counters ? bb.counters.BwProbes : null,
    bbrProbeRTTs: bb.counters ? bb.counters.ProbeRTTs : null,
    bbrTooHigh: bb.counters ? bb.counters.InflightTooHigh : null, bbrLossRounds: bb.counters ? bb.counters.LossRounds : null,
    // Vegas only.
    vegasState: vg.state ?? null, vegasDiff: round(vg.diff),
    // Repeated slow start only.
    ssState: ss.state ?? null, ssPeak: round(ss.peak), ssReached: round(ss.reached), ssProbes: ss.counters ? ss.counters.Probes : null,
    // Loss detection, either controller.
    srvLost: sv.counters ? sv.counters.Lost : null, srvSpurious: sv.counters ? sv.counters.Spurious : null,
    reorderWindowMs: round(sv.reorderWindowMs),
  };
  S.rows.push(row);
  const total = totals(t);
  sendCtrl('stats', { ...row, totals: total });

  charts.rate.push(t, { recv: row.recvMbps });
  charts.delay.push(t, { owd: row.owdMeanMs, owdP95: row.owdP95Ms, queue: row.queueMeanMs });
  charts.loss.push(t, { client: row.lossPct });
  for (const c of Object.values(charts)) c.draw(t);

  const srv = sv.counters || {};
  const alg = sv.algorithm;
  let state = '–';
  if (alg === 'cubic') state = `CUBIC, ${cu.state}`;
  else if (alg === 'bbr') state = `BBR, ${bb.state}`;
  else if (alg === 'vegas') state = `Vegas, ${vg.state}`;
  else if (alg === 'slowstart') state = ss.state === 'waiting' ? `waiting ${fmtNum(ss.waitMs / 1000)} s after a loss` : 'slow start';
  else if (cp.cwnd !== undefined) state = `Copa, ${cp.mode}${cp.slowStart ? ', slow start' : ''}`;
  const share = (n, of) => (of ? 100 * n / of : null);
  const inf = (v) => (v === undefined || v === null ? '∞' : v);
  const own = {
    cubic: () => [
      ['ssthresh', inf(cu.ssthresh), 'pkts'],
      ['W_max', cu.wMax, 'pkts'],
      ['K', cu.k, `s · ${fmtNum(cu.epochSec)} s into the epoch`],
      ['congestion events', cu.counters.CongestionEvents, `· ${cu.counters.FastConvergences} fast conv.`],
      ['Reno-friendly acks', share(cu.counters.RenoFriendlyAcks, cu.counters.Acks), '%'],
      ['acks with the window not full', share(cu.counters.AppLimitedAcks, cu.counters.Acks), '%'],
    ],
    bbr: () => [
      ['bandwidth estimate (bw)', pps(bb.bw), `Mbps · max_bw ${fmtNum(pps(bb.maxBw))}`],
      ['bw_shortterm', inf(pps(bb.bwShortterm)), 'Mbps'],
      ['gains: pacing / cwnd', `${fmtNum(bb.pacingGain)} / ${fmtNum(bb.cwndGain)}`, ''],
      ['BDP', bb.bdp, `pkts · ${fmtNum(bb.extraAcked)} extra acked`],
      ['inflight_longterm', inf(bb.inflightLongterm), 'pkts'],
      ['inflight_shortterm', inf(bb.inflightShortterm), 'pkts'],
      ['bandwidth probes', bb.counters.BwProbes, `· ${bb.counters.InflightTooHigh} met high loss`],
      ['ProbeRTTs', bb.counters.ProbeRTTs, `· round ${bb.round}`],
      ['rounds with loss outside a probe', bb.counters.LossRounds, ''],
      ['acks while the cap was the limit', share(bb.counters.AppLimitedAcks, bb.counters.Acks), '%'],
    ],
    vegas: () => [
      ['diff (packets queued)', vg.diff, `pkts · kept within ${vg.alpha}..${vg.beta}`],
      ['ssthresh', inf(vg.ssthresh), 'pkts'],
      ['rounds: grew / shrank / held', `${vg.counters.Increases} / ${vg.counters.Decreases} / ${vg.counters.Holds}`, ''],
      ['congestion events (loss)', vg.counters.CongestionEvents, ''],
      ['acks with the window not full', share(vg.counters.AppLimitedAcks, vg.counters.Acks), '%'],
    ],
    slowstart: () => [
      ['window the lost packet left at', inf(ss.peak), `pkts · cut to ${ss.beta} of it`],
      ['window when the loss was noticed', inf(ss.reached), 'pkts'],
      ['probes', ss.counters.Probes, `· ${ss.counters.CongestionEvents} ended by loss`],
      ['losses not acted on', ss.counters.IgnoredLosses, 'pkts · while waiting, or sent before the probe'],
      ['acks with the window not full', share(ss.counters.AppLimitedAcks, ss.counters.Acks), '%'],
    ],
    copa: () => [
      ['RTTstanding', cp.rttStandingMs, 'ms'],
      ['Copa queueing delay', cp.queueDelayMs, 'ms'],
      ['target rate λt', mbps(sv.targetBitrate) === null ? '∞' : mbps(sv.targetBitrate), 'Mbps'],
      ['velocity', cp.velocity, ''],
      ['δ', cp.delta === undefined ? null : String(+cp.delta.toPrecision(4)), `· 1/δ = ${fmtNum(1 / cp.delta)} pkts`],
    ],
  };
  $('tiles').innerHTML = tilesHTML([
    ['received · client', row.recvMbps, 'Mbps'],
    ['mean since start · client', total.meanMbps, 'Mbps'],
    ['sent', row.srvSentMbps, 'Mbps'],
    ['cwnd', sv.cwnd, 'pkts'],
    ['in flight', sv.inFlight, 'pkts'],
    ['state', state, ''],
    ['one-way delay · client', row.owdMeanMs, `ms · p95 ${fmtNum(row.owdP95Ms)}`],
    ['queueing delay · client', row.queueMeanMs, `ms · p95 ${fmtNum(row.queueP95Ms)}`],
    ['queueing p50 / p95 / p99, whole run · client', `${fmtNum(total.queueDelayP50Ms)} / ${fmtNum(total.queueDelayP95Ms)} / ${fmtNum(total.queueDelayP99Ms)}`, 'ms'],
    ['jitter · client', row.jitterMs, 'ms'],
    ['longest gap · client', row.gapMaxMs, 'ms'],
    ['loss, whole run · client', total.lossPct, `% · ${total.packetsMissing} pkts`],
    ['late (reordered) · client', total.late, 'pkts'],
    ['smoothed RTT', sv.srttMs, 'ms'],
    ['RTTmin', ctl.rttMinMs, 'ms'],
    ['pacing rate', mbps(sv.pacingBitrate), 'Mbps'],
    ...(ctl.counters && own[alg] ? own[alg]() : []),
    ['declared lost', srv.Lost, `pkts · ${srv.TimedOut || 0} by timeout · ${srv.Spurious || 0} only late`],
    ['reordering window', sv.reorderWindowMs, 'ms'],
    ['ping RTT · client', row.pingRttMs, 'ms'],
    ['acks sent · client', S.acksSent, S.ackMs > 0 ? `· one per ${S.ackMs} ms` : '· one per packet'],
    ['SCTP buffered', sv.sctpBufferedBytes, 'bytes'],
  ]);
  $('t-client').textContent = JSON.stringify({ interval: row, totals: total }, null, 1);
}

// The sender's snapshots arrive on their own clock (every 100 ms); chart
// them as they come rather than at this page's reporting interval.
function onServerStats(sv) {
  const t = now();
  S.server = sv; S.serverAt = t;
  const name = sv.algorithm, ctl = sv[name] || {}, cp = sv.copa || {}, cu = sv.cubic || {};
  const bb = sv.bbr || {}, vg = sv.vegas || {}, ss = sv.slowstart || {};
  const mbps = (bps) => (bps === undefined || bps === null ? null : bps / 1e6);
  const pps = (v) => (v === undefined || v === null ? null : v * (sv.packetBytes || 1150) * 8 / 1e6);
  // Copa's target rate λt is a tile, not a line: it is 1/(δ·dq), so it shoots
  // off the scale every time the queue empties.
  charts.rate.push(t, {
    sent: mbps(sv.sendBitrate), window: mbps(sv.windowBitrate),
    maxBw: pps(bb.maxBw), pacing: name === 'bbr' ? mbps(sv.pacingBitrate) : null,
  });
  charts.delay.push(t, { standing: cp.rttStandingMs, rttMin: ctl.rttMinMs, srtt: sv.srttMs });
  charts.window.push(t, {
    cwnd: sv.cwnd, inFlight: sv.inFlight,
    velocity: cp.velocity, invDelta: cp.delta ? 1 / cp.delta : null,
    ssthresh: cu.ssthresh ?? vg.ssthresh, wMax: cu.wMax || null,
    bdp: bb.bdp || null, inflightLongterm: bb.inflightLongterm, inflightShortterm: bb.inflightShortterm,
    diff: vg.diff, peak: ss.peak ?? null,
  });
  charts.loss.push(t, { server: sv.lossPct ?? 0 });
  $('t-server').textContent = JSON.stringify(sv, null, 1);
  // Changes of state are worth a line in Events.
  const c = ctl.counters || {};
  if (S.lastMode !== null && cp.mode !== undefined && cp.mode !== S.lastMode) logEvent(name, `mode ${S.lastMode} → ${cp.mode}`);
  if (S.lastSlowStart === true && ctl.slowStart === false && !(c.CongestionEvents > 0) && name !== 'slowstart') logEvent(name, `left slow start at cwnd ${fmtNum(sv.cwnd)}`);
  if (c.Timeouts > S.lastTimeouts) { logEvent(name, name === 'bbr' ? 'timeout: window collapsed' : 'timeout: back to slow start'); markCut(t); }
  // BBR: changes of state, and loss lowering one of its bounds.
  if (name === 'bbr') {
    // ProbeBW's phases come round every few seconds; the others are news.
    const quiet = (st) => /^ProbeBW/.test(st || '');
    if (S.lastBbrState && bb.state !== S.lastBbrState && !(quiet(bb.state) && quiet(S.lastBbrState))) {
      logEvent(name, `${S.lastBbrState} → ${bb.state}: bw ${fmtNum(pps(bb.bw))} Mbps, min_rtt ${fmtNum(bb.rttMinMs)} ms, cwnd ${fmtNum(sv.cwnd)}`);
    }
    S.lastBbrState = bb.state;
    const cuts = (c.InflightTooHigh || 0) + (c.LossRounds || 0);
    if (cuts > S.lastBbrCuts) {
      markCut(t);
      if (c.InflightTooHigh > S.lastTooHigh) logEvent(name, `loss while probing: inflight_longterm ${fmtNum(bb.inflightLongterm)} pkts`);
    }
    S.lastBbrCuts = cuts; S.lastTooHigh = c.InflightTooHigh || 0;
  }
  // Copa only cuts on loss in competitive mode, by doubling δ.
  if (cp.delta !== undefined) {
    if (S.lastDelta !== undefined && S.lastDelta !== null && cp.mode === 'competitive' && S.lastMode === 'competitive' && cp.delta > S.lastDelta * 1.5) markCut(t);
    S.lastDelta = cp.delta;
  }
  if (c.CongestionEvents > S.lastEvents) {
    markCut(t);
    logEvent(name, `congestion event ${c.CongestionEvents}: cwnd ${fmtNum(sv.cwnd)}` +
      (name === 'cubic' ? `, W_max ${fmtNum(cu.wMax)}, K ${fmtNum(cu.k)} s` : '') +
      (name === 'slowstart' ? `: the lost packet left at ${fmtNum(ss.peak)}, noticed at ${fmtNum(ss.reached)}; probing again in ${fmtNum(ss.waitMs / 1000)} s` : ''));
  }
  S.lastMode = cp.mode ?? null; S.lastSlowStart = ctl.slowStart;
  const spurious = (sv.counters || {}).Spurious || 0;
  if (spurious > S.lastSpurious) {
    logEvent('loss detection', `${spurious - S.lastSpurious} packet(s) declared lost arrived after all: reordering window now ${fmtNum(sv.reorderWindowMs)} ms`);
  }
  // CUBIC and Vegas only probe when the window is what limits them. If most acks find
  // the window not full, the rate cap (or the sender itself) is the limit and
  // there will be no sawtooth to see: say so, once.
  const windowed = name === 'cubic' || name === 'vegas' || name === 'slowstart';
  if (windowed && S.prevAcks && c.Acks - S.prevAcks.acks > 200 && !S.capWarned && sv.uptimeSec > 3) {
    const share = (c.AppLimitedAcks - S.prevAcks.limited) / (c.Acks - S.prevAcks.acks);
    if (share > 0.8) {
      S.capWarned = true;
      const cap = S.hello ? `${S.hello.cc.maxRate / 1e6} Mbps` : 'the cap';
      logEvent(name, `the window is not the limit: the path carries the rate cap (${cap}) without filling it, so ${NAMES[name]} is not probing. ` +
        'A bottleneck below the cap (or a higher cap) is needed to see it work');
    }
  }
  if (windowed && (!S.prevAcks || c.Acks - S.prevAcks.acks > 200)) S.prevAcks = { acks: c.Acks, limited: c.AppLimitedAcks };
  S.lastEvents = c.CongestionEvents || 0; S.lastTimeouts = c.Timeouts || 0; S.lastSpurious = spurious;
}

// ---------------------------------------------------------------------------
// Export: one line per reporting interval.
// ---------------------------------------------------------------------------
function buildCSV(rows) {
  const cols = Object.keys(rows[0]);
  const cell = (v) => (v === null || v === undefined ? '' : String(v));
  return [cols.join(','), ...rows.map((r) => cols.map((c) => cell(r[c])).join(','))].join('\n') + '\n';
}

function downloadCSV() {
  if (!S.rows.length) return;
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([buildCSV(S.rows)], { type: 'text/csv' }));
  a.download = `cc-${(S.hello && S.hello.cc.algorithm) || 'run'}-${S.sessionId || 'session'}.csv`;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}
