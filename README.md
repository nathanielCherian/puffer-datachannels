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

The source can also be the renditions of a simulcast transcoder (the `sfu`
project's, or any that sends the same thing): nothing is then encoded per
viewer, and the rate controller's target chooses which rendition a viewer is
forwarded. See [Simulcast source](#simulcast-source).

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
| `--cc` | `fixed` | rate controller: `fixed`, or `sweep` (testing: the target goes from `--min-bitrate` to `--max-bitrate` and back every 60 s) |
| `--simulcast` | | renditions for the [simulcast source](#simulcast-source): `name=host:port@bitrate,...` (MPEG-TS over UDP; this server binds the ports) |
| `--simulcast-audio` | highest rendition | rendition whose AAC is transcoded, once, to Opus; `none` for no audio |
| `--pacing-factor` | 2.0 | pacer drain rate ÷ bitrate |
| `--headroom` | 0.9 | (video encoder + audio) bitrate ÷ controller target |
| `--keyint` | 0 | seconds between keyframes (0 = only when the browser asks) |
| `--max-queue` | 500ms | pacer queue length beyond which a new frame is dropped |
| `--rtx-history` | 2s | how long frames can be retransmitted |
| `--feedback-interval` | 50ms | how often the browser reports arrivals |
| `--sctp-min-cwnd` | 64 MiB | floor for SCTP's congestion window; 0 leaves SCTP congestion control on |
| `--emulate-loss`, `--emulate-delay` | 0 | testing: drop a fraction of outgoing media packets / hold every outgoing packet |
| `--emulate-rate`, `--emulate-queue` | 0, 200ms | testing: a bottleneck of this many bps for everything the server sends, with a drop-tail buffer this long |
| `--cc-algorithm` | `copa` | the congestion-control page: `copa`, `cubic`, `bbr`, `vegas` or `slowstart` when the page does not choose |
| `--cc-max-rate`, `--cc-max-duration` | 20000000, 10m | the congestion-control page: the most a session may send, and for how long |
| `--copa-mode`, `--copa-delta` | `default`, 0.5 | Copa: how δ is chosen, and δ (0.0001 to 2) |

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

- **FORWARD-TSN:** `third_party/sctp` is pion/sctp v1.11.3 with one change
  (`go.mod` replaces the module with it; the change is marked
  "puffer-datachannels patch" in `association.go`). Upstream sends a
  FORWARD-TSN for every SACK that leaves abandoned chunks unacknowledged,
  which on this channel is every SACK; the browser answers each with an
  immediate SACK, and the two trade packets for as long as data flows:
  measured at 5,000 FORWARD-TSNs a second beside 280 data packets, half of a
  5 Mbps link. The copy sends one only when a SACK reports a gap (the browser
  is missing a chunk that will not be resent), and at most every 20 ms:
  none at all on a path without loss.

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
`Fixed` in the same file is the template; `Sweep` beside it is a moving target
with no network behind it, for watching what follows the target. The encoder
follows `TargetBitrate` within 100 ms; a simulcast session re-chooses its
rendition from it every 100 ms. Each sent packet appears in exactly one `Feedback`, either
received or lost (unacknowledged and 16 packets behind the newest ack, or 2 s
old). Arrival times are on the browser's clock: use differences. Audio packets
(`SentPacket.Audio`) share the sequence numbers and the pacer, and leave ahead
of video.

## Congestion control: Copa, CUBIC, BBR, Vegas and a repeated slow start

`/cc.html` is the transport without the media: the server sends random bytes
as fast as a congestion controller allows, and the page acknowledges and
measures them. The controller is chosen on the page:
[Copa](https://www.usenix.org/conference/nsdi18/presentation/arun) (Arun and
Balakrishnan, NSDI 2018), delay-based; CUBIC
([RFC 9438](https://www.rfc-editor.org/rfc/rfc9438)), loss-based; BBR version 3
([draft-ietf-ccwg-bbr-07](https://datatracker.ietf.org/doc/draft-ietf-ccwg-bbr/)),
model-based; or TCP Vegas (Brakmo and Peterson, 1995), delay-based. Same
`media` channel, same DATA format, same SCTP settings as above; no ffmpeg, no
encoder, no retransmission.

```sh
./bin/server --emulate-rate 5000000 --emulate-delay 20ms   # a 5 Mbps, 20 ms link to converge on
# http://localhost:8080/cc.html, choose an algorithm, press Start
```

Without `--emulate-rate` (or a real bottleneck) there is nothing for a
controller to find, and it sends at `--cc-max-rate`. That goes for a deployed
server too: on a path that carries the cap without queueing, every controller
sits at the cap, and CUBIC in particular shows no sawtooth, because it only
probes while its window is what limits it. The page says so in Events when
that is the case. To see CUBIC work there, give it a bottleneck below the cap
(`--emulate-rate`) or a cap above what the path carries. The emulated link is
shared by every session, so two tabs compete for it.

**Copa** is `internal/copa`: no clock, no I/O, one call per ack. It
is §2 of the paper: RTTmin over 10 s and RTTstanding over srtt/2, queueing
delay `dq` their difference, target rate `1/(δ·dq)`, cwnd moved by
`v/(δ·cwnd)` per ack towards it, the velocity `v` doubling after three RTTs in
one direction, slow start until the rate first exceeds the target, pacing at
`2·cwnd/RTTstanding`, and no reaction to loss in default mode. Two additions
are marked in the code: `v` resets as soon as an ack decides against the
current direction, and `v` is bounded so that one RTT cannot more than double
cwnd. `MaxRate` (the page's rate cap) is not Copa: it caps the pacing rate and
stops cwnd growing past what that rate can keep in flight.

**CUBIC** is `internal/cubic`, in the same shape. It is §4 of the RFC: slow
start to the first loss; then the window follows `C·(t − K)³ + W_max` from
the time of the last reduction, aiming one RTT ahead and growing by at most
half a window per RTT; the Reno-friendly estimate takes over wherever the
curve would be slower than Reno; a loss multiplies the window by 0.7, once per
window of losses; fast convergence lowers `W_max` when a loss comes below the
previous peak; a timeout restarts slow start. C = 0.4, β = 0.7. Pacing is
Linux's (twice cwnd/srtt in slow start, 1.2 times after), since the RFC
defines none. While the window is not full (the sender is held by the rate
cap) nothing moves: not the window, not the Reno estimate, not the epoch's
clock (§5.8, §4.2). Left out: HyStart++ and the undo of spurious reductions.

**BBR** is `internal/bbr`, version 3 as in draft-ietf-ccwg-bbr-07, with the
draft's function names. Every ack gives a delivery-rate sample (§4.1); the
largest of the last two ProbeBW cycles is `max_bw`, the smallest RTT of the
last 10 s is `min_rtt`, and the flow paces at `pacing_gain · bw` with at most
`cwnd_gain · bw · min_rtt` (plus an allowance for aggregated acks) in flight.
The state machine is the draft's: Startup (2.77×) until the rate stops
growing by a quarter for three rounds or loss is high; Drain; then ProbeBW's
DOWN, CRUISE, REFILL and UP, probing every 2 to 3 s or as often as Reno would
recover; ProbeRTT for 200 ms when `min_rtt` is 10 s old. Loss above 2% while
probing sets `inflight_longterm`, a round with loss outside a probe lowers
`bw_shortterm` and `inflight_shortterm` by at most 0.7×, and a probe that
follows one that met loss stops at the ceiling (precautionary probing). BBR
is the one controller that is told of every packet sent (`OnSend`), which a
delivery-rate sample needs. Under the rate cap its samples are marked
application limited. Left out: the send quantum and offload budget,
restarting from idle, ECN, and the undo of a spurious recovery. One line is
Linux's rather than the draft's and is marked: after a timeout the short-term
bound starts from the window before the collapse.

**Vegas** is `internal/vegas`. `baseRTT` is the smallest RTT ever seen; once
per round trip `diff = cwnd · (rtt − baseRTT) / rtt`, with `rtt` the round's
smallest, estimates how many of the flow's packets are queued, and the window
grows by one below α = 2, shrinks by one above β = 4, and holds in between.
Slow start doubles every other round, as in the paper, and ends when diff
passes γ = 1; loss halves the window, once per window of losses. Constants,
the Reno fallback for a round with too few samples and the pacing are
Linux's. Like CUBIC, it does not grow a window that is not full.

**Slow start, repeated** is `internal/slowstart`, and is an experiment rather
than a published controller: the window doubles every round trip until a
packet is lost, is cut to half of what it was when that packet left (not of
what it has doubled to by the time the loss is noticed, which would still
fill the buffer), is held for one second whatever is lost meanwhile, and then
doubles again from there. Every cycle overshoots the path
the way a connection's first slow start does, so the charts show what that
costs once a second. A loss only ends a probe if the probe sent the packet.

**Acks.** By default the page acknowledges each packet as it arrives. With
"Acknowledge every N ms" in Settings it sends one message per interval naming
every arrival of the interval and its arrival time (and the interval before
again, against a lost message), so each packet still gives an exact RTT
sample and the uplink carries a tenth of the packets or fewer. The cost is
timing: room in the window opens a batch at a time and the controller hears
of queueing up to an interval late. At 20 ms on the link below, throughput
was unchanged for all four; Copa's median queueing delay went from 5 to
10 ms.

**The sender** is `internal/session/cc.go`, the same for all four: anything
with the six methods of its `controller` interface can drive it (and may also
implement `OnSend`). A packet leaves
when fewer than cwnd are unacknowledged and the pacer's budget allows.

A packet is lost once three sent after it are acknowledged and one of those
left more than a reordering window after it, or after max(1 s, 4 srtt); if
nothing at all was acknowledged in that time the controller is told to start
over. The reordering window is a quarter of the smoothed RTT. Counting packets
alone is not enough on a real path: at 20 Mbps three packets are 1.4 ms, and
over a 4 ms campus path 13 of 36 "losses" in 30 s were packets that had only
been overtaken. If a packet declared lost is acknowledged after all, it is
counted as spurious and the window grows by another quarter, up to the whole
RTT. The controller is not told to undo its reduction.

**Acks.** The page acknowledges each packet as it arrives (media reports every
50 ms; these controllers need the ack clock and an RTT per packet). An ack is a FEEDBACK
message naming the last four arrivals with their arrival times, so a lost ack
is covered by the next three, and the RTT is exact either way: ack arrival −
packet send time − how long the page held it.

**What the page measures** (`web/cc.js`), from the packets alone, every
200 ms and over the whole run: received rate; loss (missing sequence numbers);
late (reordered) and duplicate packets; one-way delay (needs the clock offset
from pings, so it is off by half the path's asymmetry); queueing delay (transit
time above the lowest seen, which needs no clock offset) with p50/p95/p99;
RFC 3550 jitter; the longest gap between arrivals. Beside these it shows the
sender's state from its 100 ms snapshots: cwnd, packets in flight, RTTmin,
smoothed RTT, pacing rate, and the controller's own (Copa: RTTstanding, δ,
velocity, mode, target rate; CUBIC: ssthresh, W_max, K, congestion events;
BBR: state, bw and max_bw, the gains, BDP, the long- and short-term bounds;
Vegas: diff, ssthresh, what each round decided; the repeated slow start: the
window the lost packet left at and the window when it was noticed, probes, losses it did not act on).
The rate, delay and window charts mark two kinds of moment with vertical
bars: a loss, at the time the page saw a later packet arrive in a missing
one's place (drawn once the missing one has had 300 ms to turn up late), and
the sender cutting its window (a CUBIC or Vegas congestion event, a slow-start probe ending, Copa in
competitive mode raising δ, BBR lowering a bound after loss, or a timeout). Each interval is a
row in "Download metrics (CSV)" and a `stats` line in the session's
`client.jsonl`; `cc.jsonl` has every packet (`sent`, `ack` with its RTT,
`lost`).

Measured in Chrome on localhost with `--emulate-rate 5000000 --emulate-delay
20ms` (1150-byte messages are about 1.84 ms each on that link, which carries
about 4.2 Mbps of them once SCTP and DTLS headers, SACKs and the stats channel
are paid for):

| | received | queueing delay p50 / p95 / p99 | lost |
|---|---|---|---|
| Copa, default mode, 30 s | 4.20 Mbps | 5.3 / 9.8 / 11.3 ms | 0 |
| Copa, default mode, `--emulate-loss 0.01`, 24 s | 4.21 Mbps | 5.3 / 9.8 / 11.3 ms | 1.1% |
| two Copa flows, default mode, 20 s | 1.90 and 1.85 Mbps | 8.8–10.3 / 16.8 ms | 0 |
| Copa, rate cap 2 Mbps, 10 s | 2.00 Mbps | 0.3 / 0.8 / 1.8 ms | 0 |
| Copa, competitive mode, 14 s | 4.13 Mbps | 147 / 196 / 199 ms | 0.3% |
| Copa, auto mode, 24 s | 4.14 Mbps | 157 / 195 / 198 ms | 0.2% |
| CUBIC, 40 s | 4.23 Mbps | 170 / 198 / 200 ms | 1.1% |

Copa's 5.3 ms is 2.9 packets, against the 2.5 (1.25/δ) the paper derives.
CUBIC fills the 200 ms buffer, as a loss-based controller must: its window
went between 72 and 105 packets over 11 reductions.

Sharing the link, each pair started together and measured over the same
window once both had settled:

| | Copa | CUBIC |
|---|---|---|
| Copa in default mode against CUBIC, 22 s | 0.36 Mbps | 3.69 Mbps |
| Copa in auto mode against CUBIC, 30 s | 2.01 Mbps | 1.91 Mbps |

**Copa's modes.** `default` keeps δ at 0.5. `competitive` runs AIMD on 1/δ (plus one
per RTT, halved on loss) so that Copa holds a share against a buffer-filling
flow: the second table shows it against CUBIC, and in the simulator in
`internal/copa/copa_test.go` default mode gets 7% of a link shared with one
AIMD flow and mode switching gets 44%. `auto` is the
paper's switching between the two (§2.2), implemented as written, and it is
not the default because, as written, it misfires with nothing but Copa on the
link: the "auto mode" row of the first table is a lone flow that turned
competitive 0.8 s in and stayed there. A Copa cycle bottoms out at exactly an empty queue with
no margin, so where delay comes in whole packets some cycles stop one packet
short of what the detector calls "nearly empty"; and once competitive, with
1/δ large next to the path's BDP, cwnd settles instead of oscillating, the
queue stops emptying, and only loss brings δ back. The note on `updateMode`
has the detail.

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

## Simulcast source

The file source encodes the video once per viewer. The simulcast source
encodes nothing: an SFU-side transcoder produces a few renditions of one live
feed, the server ingests them once, and every viewer is forwarded one of them
over the same DataChannel path as before (fragmenting, pacing, NACK, the
jitter buffers and decoders are unchanged).

```
                    MPEG-TS/UDP                   ┌─ viewer A: controller target ─► forwards high ─► sender ─► browser
transcoder ─┬─ high 2500k :5001 ─┐                │
(sfu, or    ├─ mid   800k :5002 ─┼─► hub ─────────┼─ viewer B: controller target ─► forwards mid ──► sender ─► browser
 ffmpeg)    └─ low   250k :5003 ─┘ (demux once,   │
                                   one timeline,  └─ viewer C: controller target ─► forwards low ──► sender ─► browser
                                   AAC→Opus once)
```

```sh
# 1. a transcoder: the sfu project's (`python -m transcoder`, UDP outputs), or
#    the stand-in here: one ffmpeg, three renditions of media/standin.mp4
#    (2 s between keyframes) to UDP 5001-5003. LAYERS/GOP/FPS/HOST are env knobs.
make renditions

# 2. this server; it binds those ports
./bin/server --simulcast high=127.0.0.1:5001@2500k,mid=127.0.0.1:5002@800k,low=127.0.0.1:5003@250k

# to watch it change rendition without a network that makes a controller move:
./bin/server --simulcast ... --cc sweep --min-bitrate 450000 --max-bitrate 5000000
```

On the page, **Settings → Video source** offers "Live simulcast" and selects it
when the server has it. With `--simulcast`, `--video` is optional: without a
file the server serves the renditions only. The offer field is
`"source": "simulcast"` (or `"file"`); `GET /info` lists `sources` and the
hub's per-rendition status under `simulcast`.

**What the renditions must be.** H.264 in MPEG-TS, one UDP port each, with:
keyframes (IDR) at the same timestamps in every rendition, SPS/PPS repeated
with each one, no B-frames, and one 90 kHz clock shared by all renditions (and
by the AAC, if there is audio). This is what the `sfu` transcoder and
`scripts/simulcast-renditions.sh` produce. Access unit delimiters and filler
(the padding of a strict-CBR encoder) are dropped on the way in.

**The hub (`internal/simulcast`).** One per server. It demultiplexes each
port, reads each rendition's size from its SPS, and hands every access unit to
all simulcast sessions without copying it. Two things here exist because the
transport is a DataChannel and not RTP:

- *Capture times.* The page schedules by each packet's capture time, so the
  hub gives every frame one: its PTS mapped onto the server's clock
  (`timeline.go`). The first frame anchors the mapping; after that a capture
  time is the anchor plus the PTS difference, so frames keep the source's
  spacing however unevenly the transcoder delivered them, and the same instant
  has the same capture time in every rendition. A source that restarts on a new
  timeline is anchored again after a second (`timelineReanchors`); a clock
  that runs a little fast or slow is followed 5 ms per 10 s
  (`timelineSlewMs`).
- *Nothing early.* A frame or audio packet that arrives ahead of its own
  capture time is held in the hub until then (`held` per rendition,
  `audioHeld`). A transcoder reading a looped file sends a run of them where
  the file wraps: with the stand-in, 15 frames up to 0.4 s early, once a
  minute. Passed straight on, the jitter buffers took "early" for the new
  normal and the picture froze for 0.2 s half a minute later, when they let go
  of it.

**Per viewer (`internal/session/simulcast.go`).** The viewer's controller
gives a video budget exactly as for the file source (`--headroom` × target,
less audio). The rendition forwarded is the highest whose nominal bitrate
fits:

- **Down** as soon as the current rendition no longer fits; **up** once the
  budget has covered 1.1× the higher one for 1 s.
- **When:** on the next keyframe of the rendition being switched to. The
  browser sees consecutive frame ids and continuous capture times; the
  keyframe's SPS is different, and the page configures its decoder again
  (Events: `decoder … (new parameters at frame N)`). The canvas keeps the size
  the session started with and a smaller picture is scaled up.
- **One keyframe per instant.** The renditions' copies of the keyframe instant
  arrive a few ms apart, in either order. Once a switch is decided the current
  rendition's keyframe is held for up to 100 ms and replaced by the new
  rendition's when that arrives (`framesReplacedAtSwitch`), rather than sent
  ahead of it for nothing.
- **Keyframes cannot be forced.** Where the file source makes an IDR (the
  browser asked for one, or the pacer refused a frame), a simulcast session
  stops forwarding until the rendition's next periodic keyframe
  (`keyframeWaits`, `framesSkippedWaitingForKey`): nothing in between would
  decode. The keyframe interval therefore bounds startup (1 s on average with
  2 s between keyframes), how soon a switch takes effect, and recovery from a
  loss that retransmission did not repair. A KEYREQ names the newest frame the
  browser had seen; one from before the newest keyframe was sent is ignored,
  so the browser repeating itself does not cost a second interval.

**Audio.** The AAC of one rendition goes through one ffmpeg for the whole
server and comes out as the same 20 ms Opus packets the file source sends. Its
timestamps do not travel through that pipe, so a packet's capture time is the
PTS of the first AAC frame given to this ffmpeg plus the packet's position in
its output; the hub checks every AAC packet against what it has fed, and a gap
(lost datagrams, a restarted source) starts a fresh ffmpeg and a fresh count
(`audioRestarts`). Packet ids follow capture time, so a gap in the audio is a
gap in the ids. MPEG-TS carries several AAC frames per packet, so audio
reaches the hub in steps of about 150 ms; the audio buffer absorbs that and
video waits for it (lip sync), which puts a simulcast viewer some 300 ms
behind the source where a file viewer is tens of ms behind its encoder.

Measured on loopback with the stand-in renditions and `--cc sweep` between
0.45 and 5 Mbps, 112 s in Chrome, across two wraps of the looped clip: 8
switches (both directions between each pair of neighbouring renditions), no
freeze, no decoder error, no keyframe wait, no frame dropped, video within
35 ms of audio. One audio packet (20 ms) came too late to be played, where
ffmpeg stalled for 150 ms at a wrap.

What is not here: padding. A controller that only raises its estimate as far
as it has seen delivered (GCC) would, forwarding a 250 kbps rendition, never
find out that 2.5 Mbps fits; `puffer-webrtc-v2` pads for that reason. The
controllers of the congestion-control page are window-based and would need
the same when they drive media. With `fixed` the rendition is simply the one
that fits `--bitrate` (or the page's bitrate): 1.5 Mbps gives `mid` above,
3 Mbps gives `high`. Selection also goes by the nominal bitrates in
`--simulcast`, not by what a rendition measures (`measuredBps` in `/info`).

## Wire format

`internal/wire/wire.go` (Go) and `web/wire.js` (browser). DATA has a 30-byte
header: flags (key, rtx, audio), a per-transmission sequence number, send time,
frame id, fragment index and count, capture time. A NACK names its stream.

## Logs

One directory per session; every line is `{"t": <server unix ms>, "kind", "data"}`.

| file | |
|---|---|
| `meta.json` | config, media, client info, SDP, `source`, and for simulcast the renditions |
| `events.jsonl` | state changes, keyframe requests, dropped frames; simulcast: `layerPending` (a switch was decided) and `layer` (it happened) |
| `cc.jsonl` | `sent` (every packet) and `feedback` (per-packet results, RTT, rates afterwards); `--log-packets=false` turns it off. A congestion-control session has `sent`, `ack` and `lost` instead |
| `encoder.jsonl` | every frame (size, keyframe, encode time, whether it was queued), reconfigurations. Simulcast: every forwarded frame (`layer`, `pts`, `lateMs` = how far behind its capture time it reached the hub, `hubMs` = how long it then waited there, `encMs` 0) and a `reconfig` per switch |
| `snapshots.jsonl` | every 250 ms: counters, pacer queue, rates, encoder, audio, SCTP backlog; simulcast: `simulcast` (rendition, pending switch, budget, switches, keyframe waits) and `hub` (per-rendition status, timeline, audio transcoder) |
| `client.jsonl` | the page's stats (every 500 ms) and events |

## Limits

- The page receives, timestamps and schedules on its main thread with a 5 ms
  timer: arrival times carry main-thread jitter, playout is accurate to a few
  ms, and a background tab stalls.
- "Network delay" and "capture → render" on the page assume a symmetric path
  (clock offset from the lowest-RTT ping).
- Each viewer of the file costs one ffmpeg decode, one Opus encode and one
  x264 encode. A simulcast viewer costs none of them.
- A simulcast viewer cannot be sent a keyframe on request and is not padded up
  to its controller's target (see [Simulcast source](#simulcast-source)).
- A source without an audio stream plays video only.
- `--emulate-rate` shapes only what the server sends, and all of it: data,
  SACKs for the page's acks, pongs and the stats channel share the link.
- On the congestion-control page, RTT samples include the page's main thread:
  an ack waits for the message event to be dispatched. Copa's RTTstanding (a
  minimum) absorbs most of it; a busy or throttled tab still reads as
  queueing delay.
- A flow that starts while another has the buffer full never sees the path
  empty, so the page's queueing delay (and Copa's RTTmin) start from too high
  a baseline.
