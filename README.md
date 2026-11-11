# puffer-datachannels

Server-to-browser video over a WebRTC **DataChannel** instead of an RTP media
track. WebRTC supplies only the encrypted datagram path (ICE + DTLS + a
neutralised SCTP); packetisation, pacing, rate control, retransmission, the
jitter buffer and decoding are all this project's code, so each can be replaced.

```
mp4 ─ffmpeg (decode, real time, loop)─► raw I420 ─► x264 ─► frames
   frames ─► fragment ─► pacer (rate from the controller) ─► "media" DataChannel ─► browser
   browser: reassemble ─► jitter buffer ─► WebCodecs VideoDecoder ─► <canvas>
   browser ─► server on the same channel: FEEDBACK (per-packet arrival times), NACK, KEYREQ, PING
   "ctrl" DataChannel (reliable): hello, server snapshots, the page's stats
```

Derived from `puffer-webrtc-v2` (same ffmpeg source, x264 wrapper and log
layout). Video only: there is no audio.

## Running

Needs Go 1.24+, a C toolchain, `libx264` with `pkg-config`, and `ffmpeg`.

```sh
make run                                  # http://localhost:8080, UDP 50000
./bin/server --bitrate 2500000 --http-addr :8090 --udp-port 50010
```

Open the page, press **Start**. The page needs WebCodecs, which browsers only
expose on `localhost` or over HTTPS (`--tls-cert`/`--tls-key`). Each session
writes `logs/<UTC time>_<id>/`. `./bin/server -h` lists every flag; each can
also be set as `PUFFER_<FLAG>`.

| flag | default | |
|---|---|---|
| `--bitrate` | 1500000 | what the fixed controller sends at (the page can override it per session) |
| `--cc` | `fixed` | rate controller |
| `--pacing-factor` | 2.0 | pacer drain rate ÷ bitrate |
| `--headroom` | 0.9 | encoder bitrate ÷ controller target |
| `--keyint` | 0 | seconds between keyframes (0 = only when the browser asks) |
| `--max-queue` | 500ms | pacer queue length beyond which a new frame is dropped |
| `--rtx-history` | 2s | how long frames can be retransmitted |
| `--feedback-interval` | 50ms | how often the browser reports arrivals |
| `--sctp-min-cwnd` | 64 MiB | floor for SCTP's congestion window; 0 leaves SCTP congestion control on |
| `--emulate-loss`, `--emulate-delay` | 0 | testing: drop a fraction of outgoing media packets / hold every outgoing packet |

## SCTP

A browser only speaks DataChannels as SCTP over DTLS, so SCTP cannot be taken
out of the path. What is taken out is everything in it that would compete with
the sender here (`session.Settings`, and the channel options in `web/app.js`):

- **Reliability, ordering:** the `media` channel is unordered with
  `maxRetransmits: 0`. Pion abandons each chunk as it is sent; the browser
  delivers messages as they arrive.
- **Fragmentation, bundling:** a message is at most 1150 bytes, so it is one
  DATA chunk in one UDP packet and two never share a packet.
- **Congestion control:** `SetSCTPMinCwnd(64 MiB)`. Pion never lets cwnd fall
  below the floor, so slow start, halving on loss and the collapse on a
  timeout do nothing, and `Send` goes straight to the wire.

Still there: the browser's receive window (only matters if the page stops
reading), SACKs on the uplink, and the browser's own SCTP congestion control
on the small feedback messages it sends.

Measured with `--emulate-delay 100ms --emulate-loss 0.03` at 1.5 Mbps, 25 s
each, the only difference being `--sctp-min-cwnd`:

| | cwnd floor 64 MiB | SCTP congestion control on (`0`) |
|---|---|---|
| worst network delay per 0.5 s: median / p95 / max | 52 / 55 / 56 ms | 54 / 220 / 286 ms |
| bytes waiting in SCTP, max | 24 kB (one RTT of data) | 56 kB |

The page shows the SCTP backlog as "SCTP buffered" (`sctpBufferedBytes` in
snapshots). It should stay near RTT × bitrate; growth means SCTP is holding
packets.

## Writing a rate controller

`internal/ratecontrol/ratecontrol.go` defines the interface:

```go
type Controller interface {
    OnPacketSent(p SentPacket)   // every packet as it leaves the pacer
    OnFeedback(fb Feedback)      // per-packet results: received + browser arrival time, or lost; RTT
    TargetBitrate() int          // bps; the encoder is set to --headroom × this
    PacingRate() int             // bps; how fast the pacer may drain
    Stats() map[string]any       // logged and shown on the page
}
```

Implement it, `Register("name", factory)` in an `init`, run with `--cc name`.
`Fixed` in the same file is the template. The encoder follows `TargetBitrate`
within 100 ms. Each sent packet appears in exactly one `Feedback`, either
received or lost (unacknowledged and 16 packets behind the newest ack, or 2 s
old). Arrival times are on the browser's clock: use differences.

## Writing a jitter buffer

`web/jitterbuffer.js` is one class with `push(packet, now)`, `tick(now)`,
`pop(now)`, `forceKeyframe(now)` and `stats()`; the header comment describes
the interface and the policy of the one provided. In short: frames play at
capture time + baseline + target, where the target is the 95th percentile of
recent frame lateness plus 10 ms; missing fragments are NACKed; a head frame
still missing 300 ms past its playout time is abandoned and a keyframe
requested. Frames repaired by retransmission are left out of the delay
estimate, so on a lossy path with a long RTT each repair shows as a short
freeze rather than as a permanently larger delay. Raise "Minimum playout delay"
on the page to trade the other way.

## Wire format

`internal/wire/wire.go` (Go) and `web/wire.js` (browser). DATA has a 30-byte
header: flags (key, rtx), a per-transmission sequence number, send time, frame
id, fragment index and count, capture time.

## Logs

One directory per session; every line is `{"t": <server unix ms>, "kind", "data"}`.

| file | |
|---|---|
| `meta.json` | config, media, client info, SDP |
| `events.jsonl` | state changes, keyframe requests, dropped frames |
| `cc.jsonl` | `sent` (every packet) and `feedback` (per-packet results, RTT, rates afterwards); `--log-packets=false` turns it off |
| `encoder.jsonl` | every frame (size, keyframe, encode time, whether it was queued), reconfigurations |
| `snapshots.jsonl` | every 250 ms: counters, pacer queue, rates, encoder, SCTP backlog |
| `client.jsonl` | the page's stats (every 500 ms) and events |

## Limits

- The page receives, timestamps and schedules on its main thread with a 5 ms
  timer: arrival times carry main-thread jitter, playout is accurate to a few
  ms, and a background tab stalls.
- "Network delay" and "capture → render" on the page assume a symmetric path
  (clock offset from the lowest-RTT ping).
- Each viewer costs one ffmpeg decode and one x264 encode.
