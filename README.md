# puffer-datachannels

Server-to-browser video and audio over a WebRTC **DataChannel** instead of RTP
media tracks. WebRTC supplies only the encrypted datagram path (ICE + DTLS + a
neutralised SCTP); packetisation, pacing, rate control, retransmission, the
jitter buffers and decoding are all this project's code, so each can be replaced.

```
mp4 ─ffmpeg (decode, real time, loop)─┬► raw I420 ─► x264 ─► frames ─► fragment ─┐
                                      └► Opus, one packet per 20 ms ─────────────┤
   pacer (rate from the controller; audio first) ─► "media" DataChannel ─► browser
   browser, video: reassemble ─► jitter buffer ─► WebCodecs VideoDecoder ─► <canvas>
   browser, audio: jitter buffer ─► WebCodecs AudioDecoder ─► Web Audio
   browser ─► server on the same channel: FEEDBACK (per-packet arrival times), NACK, KEYREQ, PING
   "ctrl" DataChannel (reliable): hello, server snapshots, the page's stats
```

Derived from `puffer-webrtc-v2` (same ffmpeg source, x264 wrapper and log
layout).

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
| `--bitrate` | 1500000 | what the fixed controller sends at, audio included (the page can override it per session) |
| `--audio`, `--audio-bitrate` | true, 64000 | send the source's audio as Opus; its bitrate comes out of `--bitrate` |
| `--cc` | `fixed` | rate controller |
| `--pacing-factor` | 2.0 | pacer drain rate ÷ bitrate |
| `--headroom` | 0.9 | (video encoder + audio) bitrate ÷ controller target |
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
    TargetBitrate() int          // bps; the video encoder gets --headroom × this, less audio
    PacingRate() int             // bps; how fast the pacer may drain
    Stats() map[string]any       // logged and shown on the page
}
```

Implement it, `Register("name", factory)` in an `init`, run with `--cc name`.
`Fixed` in the same file is the template. The encoder follows `TargetBitrate`
within 100 ms. Each sent packet appears in exactly one `Feedback`, either
received or lost (unacknowledged and 16 packets behind the newest ack, or 2 s
old). Arrival times are on the browser's clock: use differences. Audio packets
(`SentPacket.Audio`) share the sequence numbers and the pacer, and leave ahead
of video.

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

## Audio

ffmpeg encodes the source's audio to Opus (48 kHz stereo, 20 ms packets). Each
packet is one DATA message with the audio flag and its own id counter. Its
capture time is its position in the stream on the video's timeline (media time
zero = the first video frame), so equal capture times mean "play together".

In the browser, `web/audio.js` holds `AudioJitterBuffer` (same interface as
the video one) and `AudioPlayer` (WebCodecs `AudioDecoder`, then each 20 ms
block scheduled sample-exact on the Web Audio clock).

- **Lip sync:** each buffer is told the capture-to-playout offset the other
  wants and both use the larger. Audio's includes the output device's delay
  (`AudioContext.outputLatency`), so on a slow device (Bluetooth: 150 ms and
  more) video is held back by that much. "Delay video to stay in sync with
  audio" on the page turns this off.
- **Loss:** a lost packet is 20 ms of silence; WebCodecs exposes no Opus
  concealment. Audio is NACKed only if the retransmission can still arrive in
  time, which needs "Minimum playout delay" of about an RTT or more.
- **Autoplay:** the audio context is created in the Start click. If the
  browser still keeps it suspended, the page says so in Events.

## Wire format

`internal/wire/wire.go` (Go) and `web/wire.js` (browser). DATA has a 30-byte
header: flags (key, rtx, audio), a per-transmission sequence number, send time,
frame id, fragment index and count, capture time. A NACK names its stream.

## Logs

One directory per session; every line is `{"t": <server unix ms>, "kind", "data"}`.

| file | |
|---|---|
| `meta.json` | config, media, client info, SDP |
| `events.jsonl` | state changes, keyframe requests, dropped frames |
| `cc.jsonl` | `sent` (every packet) and `feedback` (per-packet results, RTT, rates afterwards); `--log-packets=false` turns it off |
| `encoder.jsonl` | every frame (size, keyframe, encode time, whether it was queued), reconfigurations |
| `snapshots.jsonl` | every 250 ms: counters, pacer queue, rates, encoder, audio, SCTP backlog |
| `client.jsonl` | the page's stats (every 500 ms) and events |

## Limits

- The page receives, timestamps and schedules on its main thread with a 5 ms
  timer: arrival times carry main-thread jitter, playout is accurate to a few
  ms, and a background tab stalls.
- "Network delay" and "capture → render" on the page assume a symmetric path
  (clock offset from the lowest-RTT ping).
- Each viewer costs one ffmpeg decode, one Opus encode and one x264 encode.
- A source without an audio stream plays video only.
