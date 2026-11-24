package session

import (
	"context"
	"sort"
	"sync"
	"time"

	"puffer-datachannels/internal/ratecontrol"
	"puffer-datachannels/internal/wire"
)

// sender is everything between the encoder and the DataChannel: it splits
// frames into datagrams, paces them at the controller's rate, keeps recent
// frames for NACK retransmission, and turns the browser's feedback into
// per-packet results for the controller.
type sender struct {
	write   func([]byte) error // DataChannel.Send
	ctrl    ratecontrol.Controller
	logf    func(kind string, t time.Time, data any)
	logPkts bool
	cfg     senderConfig

	mu          sync.Mutex
	queue       []*packet // new video, FIFO
	rtxQueue    []*packet // retransmissions, sent before new video
	audioQueue  []*packet // audio, sent before everything: small and it cannot wait
	queuedBytes int
	nextSeq     uint32
	sent        map[uint32]ratecontrol.SentPacket // on the wire, fate unknown
	highestAck  int64                             // -1 until the first ack
	history     map[frameKey]*sentFrame
	historyIDs  []frameKey
	rtt         time.Duration
	c           senderCounters

	wake chan struct{}
}

type senderConfig struct {
	MaxQueue    time.Duration // drop a new frame when the pacer queue is longer than this
	History     time.Duration // how long frames stay available for retransmission
	ReorderGap  int           // unacked packets this far behind the newest ack are lost
	LossTimeout time.Duration // unacked this long is lost
}

type packet struct {
	buf     []byte
	stream  uint8
	frameID uint32
	key     bool
	rtx     bool
}

type frameKey struct {
	stream uint8
	id     uint32
}

type sentFrame struct {
	at      time.Time
	key     bool
	frags   [][]byte
	lastRtx []time.Time
}

type senderCounters struct {
	Packets, Bytes       int64 // everything, audio and retransmissions included
	AudioPackets         int64
	AudioBytes           int64
	RtxPackets, RtxBytes int64
	Acked, Lost          int64
	RepeatAcks           int64 // the second copy of a report, or an ack for a packet already declared lost
	Feedbacks            int64
	Nacks, NackItems     int64
	NackMisses           int64 // asked for something no longer in the history
	FramesQueued         int64
	FramesDropped        int64 // not sent because the pacer queue was too long
	WriteErrors          int64
}

func newSender(write func([]byte) error, ctrl ratecontrol.Controller, cfg senderConfig, logPkts bool,
	logf func(string, time.Time, any)) *sender {
	return &sender{
		write: write, ctrl: ctrl, cfg: cfg, logf: logf, logPkts: logPkts,
		sent: map[uint32]ratecontrol.SentPacket{}, history: map[frameKey]*sentFrame{},
		highestAck: -1, wake: make(chan struct{}, 1),
	}
}

func (s *sender) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// queueDelayLocked is how long the pacer needs to drain what is queued.
func (s *sender) queueDelayLocked() time.Duration {
	rate := s.ctrl.PacingRate()
	if rate <= 0 {
		return 0
	}
	return time.Duration(float64(s.queuedBytes) * 8 / float64(rate) * float64(time.Second))
}

// enqueueFrame fragments one encoded video frame and hands it to the pacer.
// It returns false if the frame was dropped because the queue is too long; the
// caller must then make the next frame a keyframe.
func (s *sender) enqueueFrame(frameID uint32, data []byte, key bool, capture time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queueDelayLocked() > s.cfg.MaxQueue {
		s.c.FramesDropped++
		return false
	}
	s.enqueueLocked(wire.StreamVideo, frameID, data, key, capture)
	s.c.FramesQueued++
	return true
}

// enqueueAudio hands one Opus packet to the pacer. Audio is never dropped
// here: it is a small constant rate and goes out ahead of video.
func (s *sender) enqueueAudio(id uint32, data []byte, capture time.Time) {
	if len(data) == 0 || len(data) > wire.MaxPayload {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enqueueLocked(wire.StreamAudio, id, data, false, capture)
}

func (s *sender) enqueueLocked(stream uint8, frameID uint32, data []byte, key bool, capture time.Time) {
	n := (len(data) + wire.MaxPayload - 1) / wire.MaxPayload
	f := &sentFrame{at: time.Now(), key: key, frags: make([][]byte, n), lastRtx: make([]time.Time, n)}
	for i := 0; i < n; i++ {
		end := min((i+1)*wire.MaxPayload, len(data))
		buf := wire.NewData(stream, frameID, uint16(i), uint16(n), key, capture, data[i*wire.MaxPayload:end])
		f.frags[i] = buf
		p := &packet{buf: buf, stream: stream, frameID: frameID, key: key}
		if stream == wire.StreamAudio {
			s.audioQueue = append(s.audioQueue, p)
		} else {
			s.queue = append(s.queue, p)
		}
		s.queuedBytes += len(buf)
	}
	k := frameKey{stream, frameID}
	s.history[k] = f
	s.historyIDs = append(s.historyIDs, k)
	for len(s.historyIDs) > 0 {
		id := s.historyIDs[0]
		if f.at.Sub(s.history[id].at) <= s.cfg.History {
			break
		}
		delete(s.history, id)
		s.historyIDs = s.historyIDs[1:]
	}
	s.kick()
}

// run is the pacer. It holds a byte budget that refills at the controller's
// pacing rate, may go negative by one packet, and never saves up more than a
// few milliseconds, so an idle period does not turn into a burst.
func (s *sender) run(ctx context.Context) {
	const maxBurst = 5 * time.Millisecond
	var budget float64 // bytes
	last := time.Now()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		s.mu.Lock()
		now := time.Now()
		rate := float64(s.ctrl.PacingRate()) / 8 // bytes per second
		if rate < 1 {
			rate = 1
		}
		budget = min(budget+now.Sub(last).Seconds()*rate, rate*maxBurst.Seconds())
		last = now
		for budget >= 0 {
			p := s.popLocked()
			if p == nil {
				break
			}
			budget -= float64(len(p.buf))
			s.sendLocked(p)
		}
		empty := len(s.queue)+len(s.rtxQueue)+len(s.audioQueue) == 0
		s.mu.Unlock()

		wait := time.Hour
		if !empty {
			wait = max(time.Duration(-budget/rate*float64(time.Second)), time.Millisecond)
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-timer.C:
		}
	}
}

// videoQueued reports whether any part of a video frame is still waiting for
// its first transmission.
func (s *sender) videoQueued(frameID uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.queue {
		if p.frameID == frameID {
			return true
		}
	}
	return false
}

func (s *sender) popLocked() *packet {
	q := &s.queue
	if len(s.audioQueue) > 0 {
		q = &s.audioQueue
	} else if len(s.rtxQueue) > 0 {
		q = &s.rtxQueue
	}
	if len(*q) == 0 {
		return nil
	}
	p := (*q)[0]
	(*q)[0] = nil
	*q = (*q)[1:]
	s.queuedBytes -= len(p.buf)
	return p
}

func (s *sender) sendLocked(p *packet) {
	seq := s.nextSeq
	s.nextSeq++
	now := time.Now()
	wire.Stamp(p.buf, seq, now, p.rtx)
	if err := s.write(p.buf); err != nil {
		s.c.WriteErrors++
		return
	}
	audio := p.stream == wire.StreamAudio
	sp := ratecontrol.SentPacket{Seq: seq, Size: len(p.buf), SendTime: now, FrameID: p.frameID, Audio: audio, Key: p.key, Rtx: p.rtx}
	s.sent[seq] = sp
	s.c.Packets++
	s.c.Bytes += int64(len(p.buf))
	if audio {
		s.c.AudioPackets++
		s.c.AudioBytes += int64(len(p.buf))
	}
	if p.rtx {
		s.c.RtxPackets++
		s.c.RtxBytes += int64(len(p.buf))
	}
	s.ctrl.OnPacketSent(sp)
	if s.logPkts {
		rec := map[string]any{"seq": seq, "len": sp.Size, "frame": p.frameID, "rtx": p.rtx, "key": p.key}
		if audio {
			rec["audio"] = true
		}
		s.logf("sent", now, rec)
	}
}

// onFeedback matches the browser's report against what was sent, declares
// losses, and passes the result to the controller.
func (s *sender) onFeedback(b []byte, arrival time.Time) {
	fb, err := wire.ParseFeedback(b)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.c.Feedbacks++
	out := ratecontrol.Feedback{Arrival: arrival}
	var newest *ratecontrol.PacketResult
	for _, a := range fb.Acks {
		sp, ok := s.sent[a.Seq]
		if !ok {
			// Already acked through the previous feedback (each report is sent
			// twice), or declared lost before a late arrival.
			s.c.RepeatAcks++
			continue
		}
		delete(s.sent, a.Seq)
		s.c.Acked++
		s.highestAck = max(s.highestAck, int64(a.Seq))
		out.Results = append(out.Results, ratecontrol.PacketResult{SentPacket: sp, Received: true, RecvMs: a.RecvMs})
		if r := &out.Results[len(out.Results)-1]; newest == nil || r.Seq > newest.Seq {
			newest = r
		}
	}
	if newest != nil {
		hold := time.Duration((fb.SendMs - newest.RecvMs) * float64(time.Millisecond))
		if rtt := arrival.Sub(newest.SendTime) - hold; rtt > 0 {
			out.RTT, s.rtt = rtt, rtt
		}
	}
	for seq, sp := range s.sent {
		if int64(seq) < s.highestAck-int64(s.cfg.ReorderGap) || arrival.Sub(sp.SendTime) > s.cfg.LossTimeout {
			delete(s.sent, seq)
			s.c.Lost++
			out.Results = append(out.Results, ratecontrol.PacketResult{SentPacket: sp})
		}
	}
	sort.Slice(out.Results, func(i, j int) bool { return out.Results[i].Seq < out.Results[j].Seq })
	s.ctrl.OnFeedback(out)
	if s.logPkts {
		type res struct {
			Seq    uint32  `json:"seq"`
			RecvMs float64 `json:"recvMs,omitempty"`
			Lost   bool    `json:"lost,omitempty"`
		}
		rs := make([]res, len(out.Results))
		for i, r := range out.Results {
			rs[i] = res{Seq: r.Seq, RecvMs: r.RecvMs, Lost: !r.Received}
		}
		s.logf("feedback", arrival, map[string]any{
			"fbSeq": fb.FbSeq, "clientSendMs": fb.SendMs, "rttMs": ms(out.RTT), "results": rs,
			"target": s.ctrl.TargetBitrate(), "pacing": s.ctrl.PacingRate(),
		})
	}
}

// onNack queues retransmissions for the fragments the browser asked for.
func (s *sender) onNack(b []byte) {
	stream, items, err := wire.ParseNack(b)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.c.Nacks++
	now := time.Now()
	for _, it := range items {
		s.c.NackItems++
		f := s.history[frameKey{stream, it.FrameID}]
		if f == nil {
			s.c.NackMisses++
			continue
		}
		lo, hi := int(it.FragIdx), int(it.FragIdx)+1
		if it.FragIdx == wire.WholeFrame {
			lo, hi = 0, len(f.frags)
		}
		for i := lo; i < hi && i < len(f.frags); i++ {
			// The browser repeats a NACK until the fragment arrives; do not
			// answer the same one twice in quick succession.
			if now.Sub(f.lastRtx[i]) < 20*time.Millisecond {
				continue
			}
			f.lastRtx[i] = now
			buf := append([]byte(nil), f.frags[i]...)
			s.rtxQueue = append(s.rtxQueue, &packet{buf: buf, stream: stream, frameID: it.FrameID, key: f.key, rtx: true})
			s.queuedBytes += len(buf)
		}
	}
	s.kick()
}

func (s *sender) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{
		"counters": s.c, "queuedBytes": s.queuedBytes, "queueMs": ms(s.queueDelayLocked()),
		"unacked": len(s.sent), "rttMs": ms(s.rtt),
		"targetBitrate": s.ctrl.TargetBitrate(), "pacingRate": s.ctrl.PacingRate(), "controller": s.ctrl.Stats(),
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
