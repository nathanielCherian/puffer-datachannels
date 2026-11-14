'use strict';

// ---------------------------------------------------------------------------
// The Copa page. The server sends junk as fast as Copa (internal/copa) lets
// it; this page is the receiver. It has two jobs:
//
//   1. Acknowledge every DATA packet the moment it arrives. Copa is clocked by
//      these acks and takes an RTT sample from each one.
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

const charts = {
  rate: new Chart($('c-rate'), [
    { key: 'recv', name: 'received (client)' },
    { key: 'sent', name: 'sent' },
    { key: 'window', name: 'cwnd / RTTstanding (λ)' },
  ]),
  delay: new Chart($('c-delay'), [
    { key: 'owd', name: 'one-way delay, mean (client)' },
    { key: 'owdP95', name: 'one-way delay, p95 (client)' },
    { key: 'queue', name: 'queueing delay, mean (client)' },
    { key: 'standing', name: 'RTTstanding' },
    { key: 'rttMin', name: 'RTTmin' },
    { key: 'srtt', name: 'smoothed RTT' },
  ]),
  window: new Chart($('c-window'), [
    { key: 'cwnd', name: 'cwnd' },
    { key: 'inFlight', name: 'in flight' },
    { key: 'velocity', name: 'velocity v' },
    { key: 'invDelta', name: '1/δ' },
  ]),
  loss: new Chart($('c-loss'), [
    { key: 'client', name: 'missing sequence numbers (client)' },
    { key: 'server', name: 'declared lost by the sender' },
  ]),
};

function setStatus(s) { $('status').textContent = s; }

function logEvent(kind, value) {
  const li = document.createElement('li');
  const rel = S.t0 ? ((now() - S.t0) / 1000).toFixed(3) + 's' : '';
  li.textContent = `${rel} ${kind}: ${typeof value === 'object' ? JSON.stringify(value) : value}`;
  if (/fail|error|closed|timeout|competitive/i.test(`${kind} ${value}`)) li.classList.add('bad');
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
const inputs = ['maxRate', 'mode', 'delta', 'duration', 'stun'];

// Show the server's values where a blank field would use them.
fetch('info').then((r) => r.json()).then(({ config }) => {
  $('maxRate').placeholder = (config.CopaMaxRate / 1e6).toString();
  $('maxRate').max = config.CopaMaxRate / 1e6;
  $('delta').placeholder = config.CopaDelta;
  $('mode').options[0].textContent = `server default (${config.CopaMode})`;
}).catch(() => {});

async function start() {
  $('start').disabled = true;
  $('download').disabled = true;
  for (const id of inputs) $(id).disabled = true;
  $('events').innerHTML = '';
  for (const c of Object.values(charts)) c.reset();
  const t = now();
  Object.assign(S, {
    t0: t, hello: null, server: null, serverAt: null, sessionId: null, logDir: null, rows: [],
    recent: [], fbSeq: 0, acksSent: 0, ackErrors: 0,
    pings: [], clock: null, pingId: 1, lastRttMs: null,
    // Totals for the run.
    n: { packets: 0, bytes: 0, late: 0, duplicates: 0 },
    firstSeq: null, maxSeq: -1, firstAt: null, lastAt: null,
    minTransit: Infinity, lastTransit: null, jitterMs: 0,
    hist: new Uint32Array(HIST_BUCKETS), histN: 0,
    seen: new Set(), lastMode: null, lastSlowStart: null,
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
    const res = await fetch('copa/offer', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sdp: pc.localDescription.sdp, type: pc.localDescription.type,
        maxRate: mbps ? Math.round(mbps * 1e6) : undefined, mode: $('mode').value || undefined,
        delta: num('delta'), durationSec: num('duration'),
        client: { userAgent: navigator.userAgent, timeOrigin: performance.timeOrigin, ackRepeat: ACK_REPEAT, reportMs: REPORT_MS },
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
    const c = msg.copa;
    logEvent('hello', `Copa mode ${c.mode}, δ ${c.delta}, at most ${c.maxRate / 1e6} Mbps, ${c.packetBytes}-byte packets` +
      (c.durationSec ? `, for ${c.durationSec} s` : ''));
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

  // The ack first: the sender's RTT sample should carry as little of this
  // page's own time as possible. It names this arrival and the few before it,
  // each with when it arrived, so the sender can subtract the hold exactly.
  S.recent.push([seq, tRecv]);
  if (S.recent.length > ACK_REPEAT) S.recent.shift();
  if (sendMedia(Wire.feedback(S.fbSeq++, now(), S.recent))) S.acksSent++; else S.ackErrors++;

  measure(seq, sendMs, tRecv, buf.byteLength);
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
  // Late: something sent after it arrived before it (reordering).
  if (seq > S.maxSeq) S.maxSeq = seq; else { n.late++; w.late++; }
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

  // Loss over the interval: of the sequence numbers that became due (up to
  // the highest seen), how many did not arrive. A packet that turns up late
  // counts for the interval it arrives in, so this can undershoot then.
  const expected = S.firstSeq === null ? 0 : S.maxSeq - S.firstSeq + 1;
  const due = expected - S.prev.expected, got = S.n.packets - S.prev.packets;
  S.prev.expected = expected; S.prev.packets = S.n.packets;
  const lossPct = due > 0 ? 100 * Math.max(0, 1 - got / due) : (got > 0 ? 0 : null);

  const sv = S.server || {}, cp = sv.copa || {};
  const mbps = (bps) => (bps === undefined || bps === null ? null : bps / 1e6);
  const row = {
    tSec: round((t - S.t0) / 1000),
    // Client: measured here.
    recvMbps: round(w.bytes * 8 / dt / 1e6), recvPackets: w.packets, lossPct: round(lossPct),
    owdMeanMs: round(avg(w.owd)), owdP50Ms: round(pct(w.owd, 0.5)), owdP95Ms: round(pct(w.owd, 0.95)), owdMaxMs: round(pct(w.owd, 1)),
    queueMeanMs: round(avg(w.queue)), queueP95Ms: round(pct(w.queue, 0.95)), queueMaxMs: round(pct(w.queue, 1)),
    jitterMs: round(S.jitterMs), gapMaxMs: round(w.gapMaxMs), late: w.late,
    pingRttMs: round(S.lastRttMs), clockOffsetMs: round(S.clock && S.clock.offsetMs), tabHidden: document.hidden,
    // Server: the sender's latest snapshot, for reading the two side by side.
    srvAgeMs: S.serverAt ? round(t - S.serverAt, 0) : null,
    srvSentMbps: round(mbps(sv.sendBitrate)), srvDeliveredMbps: round(mbps(sv.deliveryBitrate)), srvLossPct: round(sv.lossPct),
    cwnd: round(cp.cwnd), inFlight: sv.inFlight ?? null, velocity: cp.velocity ?? null, delta: round(cp.delta, 4),
    mode: cp.mode ?? null, slowStart: cp.slowStart ?? null,
    rttMinMs: round(cp.rttMinMs), rttStandingMs: round(cp.rttStandingMs), srttMs: round(cp.srttMs), copaQueueMs: round(cp.queueDelayMs),
    windowMbps: round(mbps(sv.windowBitrate)), targetMbps: round(mbps(sv.targetBitrate)), pacingMbps: round(mbps(sv.pacingBitrate)),
  };
  S.rows.push(row);
  const total = totals(t);
  sendCtrl('stats', { ...row, totals: total });

  charts.rate.push(t, { recv: row.recvMbps });
  charts.delay.push(t, { owd: row.owdMeanMs, owdP95: row.owdP95Ms, queue: row.queueMeanMs });
  charts.loss.push(t, { client: row.lossPct });
  for (const c of Object.values(charts)) c.draw(t);

  const srv = sv.counters || {};
  const state = cp.cwnd === undefined ? '–' : `${cp.mode}${cp.slowStart ? ', slow start' : ''}`;
  $('tiles').innerHTML = tilesHTML([
    ['received · client', row.recvMbps, 'Mbps'],
    ['mean since start · client', total.meanMbps, 'Mbps'],
    ['sent', row.srvSentMbps, 'Mbps'],
    ['cwnd', cp.cwnd, 'pkts'],
    ['in flight', sv.inFlight, 'pkts'],
    ['state', state, ''],
    ['one-way delay · client', row.owdMeanMs, `ms · p95 ${fmtNum(row.owdP95Ms)}`],
    ['queueing delay · client', row.queueMeanMs, `ms · p95 ${fmtNum(row.queueP95Ms)}`],
    ['queueing p50 / p95 / p99, whole run · client', `${fmtNum(total.queueDelayP50Ms)} / ${fmtNum(total.queueDelayP95Ms)} / ${fmtNum(total.queueDelayP99Ms)}`, 'ms'],
    ['jitter · client', row.jitterMs, 'ms'],
    ['longest gap · client', row.gapMaxMs, 'ms'],
    ['loss, whole run · client', total.lossPct, `% · ${total.packetsMissing} pkts`],
    ['late (reordered) · client', total.late, 'pkts'],
    ['RTTstanding', cp.rttStandingMs, 'ms'],
    ['RTTmin', cp.rttMinMs, 'ms'],
    ['Copa queueing delay', cp.queueDelayMs, 'ms'],
    ['target rate λt', mbps(sv.targetBitrate) === null ? '∞' : mbps(sv.targetBitrate), 'Mbps'],
    ['pacing rate', mbps(sv.pacingBitrate), 'Mbps'],
    ['velocity', cp.velocity, ''],
    ['δ', cp.delta, ''],
    ['declared lost', srv.Lost, `pkts · ${srv.TimedOut || 0} by timeout`],
    ['ping RTT · client', row.pingRttMs, 'ms'],
    ['acks sent · client', S.acksSent, ''],
    ['SCTP buffered', sv.sctpBufferedBytes, 'bytes'],
  ]);
  $('t-client').textContent = JSON.stringify({ interval: row, totals: total }, null, 1);
}

// The sender's snapshots arrive on their own clock (every 100 ms); chart
// them as they come rather than at this page's reporting interval.
function onServerStats(sv) {
  const t = now();
  S.server = sv; S.serverAt = t;
  const cp = sv.copa || {};
  const mbps = (bps) => (bps === undefined || bps === null ? null : bps / 1e6);
  // The target rate λt is a tile, not a line: it is 1/(δ·dq), so it shoots
  // off the scale every time the queue empties.
  charts.rate.push(t, { sent: mbps(sv.sendBitrate), window: mbps(sv.windowBitrate) });
  charts.delay.push(t, { standing: cp.rttStandingMs, rttMin: cp.rttMinMs, srtt: cp.srttMs });
  charts.window.push(t, { cwnd: cp.cwnd, inFlight: sv.inFlight, velocity: cp.velocity, invDelta: cp.delta ? 1 / cp.delta : null });
  charts.loss.push(t, { server: sv.lossPct ?? 0 });
  $('t-server').textContent = JSON.stringify(sv, null, 1);
  // Changes of state are worth a line in Events.
  if (S.lastMode !== null && cp.mode !== S.lastMode) logEvent('copa', `mode ${S.lastMode} → ${cp.mode}`);
  if (S.lastSlowStart === true && cp.slowStart === false) logEvent('copa', `left slow start at cwnd ${fmtNum(cp.cwnd)}`);
  if (S.lastSlowStart === false && cp.slowStart === true) logEvent('copa', 'timeout: back to slow start');
  S.lastMode = cp.mode; S.lastSlowStart = cp.slowStart;
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
  a.download = `copa-${S.sessionId || 'session'}.csv`;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}
