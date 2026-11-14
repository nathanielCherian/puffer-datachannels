package session

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"puffer-datachannels/internal/copa"
	"puffer-datachannels/internal/logs"
	"puffer-datachannels/internal/wire"
)

// A Copa session is the transport on its own: no ffmpeg, no encoder, no
// retransmission. The server always has data (junk) to send, and Copa
// (internal/copa) decides how much of it goes out. The channel and the DATA
// format are the ones media uses, so everything said about SCTP in Settings
// holds here too; the differences are on the return path:
//
//   - The browser acknowledges every packet the moment it arrives, instead of
//     reporting every 50 ms. Copa is clocked by acks: a packet may only leave
//     when one comes back, and each ack is an RTT sample.
//   - An ack is a FEEDBACK message naming the last few arrivals, the newest
//     last, so a lost ack is covered by the next ones. An arrival's RTT is
//     ack arrival - packet send time - how long the browser held it, which is
//     exact for the repeats as well.
//
// Nothing is ever retransmitted: a lost packet is counted and forgotten.

// CopaRequest is the body of POST /copa/offer. Zero fields take the server's
// values (--copa-max-rate, --copa-mode, --copa-delta, --copa-max-duration).
type CopaRequest struct {
	SDP         string          `json:"sdp"`
	Type        string          `json:"type"`
	MaxRate     int             `json:"maxRate,omitempty"`     // bps of DataChannel messages; at most the server's
	Mode        string          `json:"mode,omitempty"`        // default, auto, competitive
	Delta       float64         `json:"delta,omitempty"`       // δ in default mode
	DurationSec float64         `json:"durationSec,omitempty"` // stop by itself after this long; at most the server's
	Client      json.RawMessage `json:"client,omitempty"`
}

// copaParams is what one session runs with, after the request has been
// checked against the server's limits.
type copaParams struct {
	Mode        copa.Mode     `json:"mode"`
	Delta       float64       `json:"delta"`
	MaxRate     int           `json:"maxRate"`     // bps of DataChannel messages
	Duration    time.Duration `json:"-"`           // 0 = until the page stops
	DurationSec float64       `json:"durationSec"` // the same, for JSON
	PacketBytes int           `json:"packetBytes"`
}

// CopaSession is one browser receiving junk under Copa's control.
type CopaSession struct {
	id      string
	mgr     *Manager
	cfg     Config
	params  copaParams
	created time.Time
	ctx     context.Context
	cancel  context.CancelFunc
	log     *logs.Session

	pc      *webrtc.PeerConnection
	snd     atomic.Pointer[junkSender]
	ctrlDC  atomic.Pointer[webrtc.DataChannel]
	started atomic.Bool

	closeOnce sync.Once
}

// HandleCopaOffer creates a Copa session from a browser offer.
func (m *Manager) HandleCopaOffer(ctx context.Context, req CopaRequest, remoteAddr string) (*OfferResponse, error) {
	if m.full() {
		return nil, ErrTooManySessions
	}
	if req.SDP == "" {
		return nil, fmt.Errorf("%w: empty sdp", ErrBadOffer)
	}
	p := copaParams{Mode: copa.Mode(m.cfg.CopaMode), Delta: m.cfg.CopaDelta, MaxRate: m.cfg.CopaMaxRate,
		Duration: m.cfg.CopaMaxDuration, PacketBytes: wire.MaxMessage}
	if req.Mode != "" {
		if !copa.ValidMode(copa.Mode(req.Mode)) {
			return nil, fmt.Errorf("%w: mode must be default, auto or competitive", ErrBadOffer)
		}
		p.Mode = copa.Mode(req.Mode)
	}
	if req.Delta != 0 {
		if req.Delta < 0.01 || req.Delta > 2 {
			return nil, fmt.Errorf("%w: delta must be within 0.01..2", ErrBadOffer)
		}
		p.Delta = req.Delta
	}
	if req.MaxRate != 0 {
		if req.MaxRate < m.cfg.MinBitrate || req.MaxRate > m.cfg.CopaMaxRate {
			return nil, fmt.Errorf("%w: maxRate must be within %d..%d bps", ErrBadOffer, m.cfg.MinBitrate, m.cfg.CopaMaxRate)
		}
		p.MaxRate = req.MaxRate
	}
	if req.DurationSec != 0 {
		d := time.Duration(req.DurationSec * float64(time.Second))
		if d <= 0 || (m.cfg.CopaMaxDuration > 0 && d > m.cfg.CopaMaxDuration) {
			return nil, fmt.Errorf("%w: durationSec must be positive and at most %v", ErrBadOffer, m.cfg.CopaMaxDuration)
		}
		p.Duration = d
	}
	p.DurationSec = p.Duration.Seconds()

	s := &CopaSession{id: newID(), mgr: m, cfg: m.cfg, params: p, created: time.Now()}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	var err error
	if s.log, err = logs.NewSession(m.cfg.LogDir, s.id); err != nil {
		return nil, fmt.Errorf("create log: %w", err)
	}
	s.event("session", "created (copa)")
	if err := s.setup(ctx, req); err != nil {
		s.Close("setup failed: " + err.Error())
		return nil, err
	}
	m.mu.Lock()
	m.sessions[s.id] = s
	m.mu.Unlock()

	go func() {
		select {
		case <-s.ctx.Done():
		case <-time.After(m.cfg.ConnectTimeout):
			if !s.started.Load() {
				s.Close("connect timeout")
			}
		}
	}()

	ld := s.pc.LocalDescription()
	_ = s.log.WriteMeta(map[string]any{
		"kind": "copa", "sessionId": s.id, "createdAt": s.created.UTC().Format(time.RFC3339Nano), "createdMs": logs.Ms(s.created),
		"remoteAddr": remoteAddr, "client": req.Client, "copa": p, "config": m.cfg, "host": m.host,
		"offer": req.SDP, "answer": ld.SDP,
		"files": map[string]string{
			"events.jsonl":    "server state changes (pc/ice states, channels, start, close)",
			"cc.jsonl":        "every packet: sent (seq, cwnd when it left), ack (seq, rttMs, the browser's receive time, cwnd afterwards) and lost (seq, whether by timeout)",
			"snapshots.jsonl": "periodic server snapshot: counters, packets in flight, send and delivery rates, Copa's state",
			"client.jsonl":    "what the page measured and reported over the ctrl channel (t = server receive time)",
		},
	})
	log.Printf("[%s] new copa session from %s: mode %s, δ %g, at most %d bps (log: %s)", s.id, remoteAddr, p.Mode, p.Delta, p.MaxRate, s.log.Dir)
	return &OfferResponse{SDP: ld.SDP, Type: ld.Type.String(), SessionID: s.id, LogDir: s.log.Dir}, nil
}

func (s *CopaSession) setup(ctx context.Context, req CopaRequest) error {
	api := webrtc.NewAPI(webrtc.WithSettingEngine(s.mgr.settings))
	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: s.cfg.ICEServers})
	if err != nil {
		return err
	}
	s.pc = pc

	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		s.event("pcState", st.String())
		if st == webrtc.PeerConnectionStateFailed || st == webrtc.PeerConnectionStateClosed {
			s.Close("peer connection " + st.String())
		}
	})
	pc.OnICEConnectionStateChange(func(st webrtc.ICEConnectionState) { s.event("iceState", st.String()) })
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		switch dc.Label() {
		case "media":
			if dc.Ordered() || dc.MaxRetransmits() == nil || *dc.MaxRetransmits() != 0 {
				s.Close("the media channel must be unordered with maxRetransmits 0")
				return
			}
			dc.OnOpen(func() {
				s.event("dataChannel", "media open")
				if s.started.CompareAndSwap(false, true) {
					go s.run(dc)
				}
			})
			dc.OnClose(func() { s.Close("media channel closed") })
			dc.OnMessage(func(msg webrtc.DataChannelMessage) { s.onMediaMessage(dc, msg.Data) })
		case "ctrl":
			dc.OnOpen(func() {
				s.ctrlDC.Store(dc)
				s.event("dataChannel", "ctrl open")
				s.sendCtrl(map[string]any{
					"type": "hello", "sessionId": s.id, "serverTimeMs": logs.NowMs(), "logDir": s.log.Dir,
					"copa": s.params, "snapshotIntervalMs": ms(s.snapshotInterval()),
				})
			})
			dc.OnMessage(func(msg webrtc.DataChannelMessage) { s.onCtrlMessage(msg.Data) })
		default:
			s.event("dataChannel", "unexpected label "+dc.Label())
		}
	})
	return negotiate(ctx, pc, req.SDP)
}

func (s *CopaSession) onMediaMessage(dc *webrtc.DataChannel, b []byte) {
	now := time.Now()
	if len(b) == 0 {
		return
	}
	switch b[0] {
	case wire.TypePing:
		if len(b) >= 13 {
			_ = dc.Send(wire.Pong(b, now, time.Now()))
		}
	case wire.TypeFeedback:
		if snd := s.snd.Load(); snd != nil {
			snd.onFeedback(b, now)
		}
	}
}

func (s *CopaSession) onCtrlMessage(b []byte) {
	var rec struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(b, &rec) != nil || rec.Type == "" {
		return
	}
	s.log.Log("client", rec.Type, rec.Data)
}

// snapshotInterval is shorter than media's: a Copa cycle is five RTTs, and a
// quarter-second sample would show none of it.
func (s *CopaSession) snapshotInterval() time.Duration {
	return min(s.cfg.SnapshotInterval, 100*time.Millisecond)
}

func (s *CopaSession) run(dc *webrtc.DataChannel) {
	cc := copa.New(copa.Config{
		Delta: s.params.Delta, Mode: s.params.Mode,
		MaxRate: float64(s.params.MaxRate) / 8 / float64(s.params.PacketBytes),
	})
	snd := newJunkSender(dc.Send, cc, s.cfg.LogPackets, func(kind string, t time.Time, data any) { s.log.LogAt("cc", t, kind, data) })
	s.snd.Store(snd)
	s.event("copa", "started")
	if s.params.Duration > 0 {
		go func() {
			select {
			case <-s.ctx.Done():
			case <-time.After(s.params.Duration):
				s.Close(fmt.Sprintf("ran for %v", s.params.Duration))
			}
		}()
	}
	go snd.run(s.ctx)
	go s.snapshotLoop(dc, snd)
}

func (s *CopaSession) snapshotLoop(dc *webrtc.DataChannel, snd *junkSender) {
	t := time.NewTicker(s.snapshotInterval())
	defer t.Stop()
	var prev junkCounters
	prevT := time.Now()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		dt := now.Sub(prevT).Seconds()
		snap, c := snd.snapshot()
		snap["sendBitrate"] = float64(c.Bytes-prev.Bytes) * 8 / dt
		snap["deliveryBitrate"] = float64(c.AckedBytes-prev.AckedBytes) * 8 / dt // acknowledged by the browser
		if resolved := (c.Acked - prev.Acked) + (c.Lost - prev.Lost); resolved > 0 {
			snap["lossPct"] = 100 * float64(c.Lost-prev.Lost) / float64(resolved)
		}
		prev, prevT = c, now
		// See the media snapshot: with SCTP's congestion control out of the way
		// this is about one RTT of data. Growth means SCTP is holding packets
		// back and the RTTs Copa sees include that wait.
		snap["sctpBufferedBytes"] = dc.BufferedAmount()
		snap["uptimeSec"] = now.Sub(s.created).Seconds()
		s.log.Log("snapshots", "snapshot", snap)
		s.sendCtrl(map[string]any{"type": "server-stats", "data": snap})
	}
}

func (s *CopaSession) sendCtrl(v any) {
	dc := s.ctrlDC.Load()
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return
	}
	if b, err := json.Marshal(v); err == nil {
		_ = dc.SendText(string(b))
	}
}

func (s *CopaSession) event(kind string, value any) { s.log.Log("events", kind, value) }

// Close ends the session (idempotent).
func (s *CopaSession) Close(reason string) {
	s.closeOnce.Do(func() {
		log.Printf("[%s] closed: %s", s.id, reason)
		s.event("closed", reason)
		s.sendCtrl(map[string]any{"type": "closed", "reason": reason})
		s.cancel()
		if s.pc != nil {
			// A moment for the "closed" message to leave.
			time.AfterFunc(100*time.Millisecond, func() { _ = s.pc.Close() })
		}
		s.mgr.mu.Lock()
		delete(s.mgr.sessions, s.id)
		s.mgr.mu.Unlock()
		time.AfterFunc(500*time.Millisecond, s.log.Close)
	})
}

// junkSender is a sender that always has something to send. A packet leaves
// when there is room in Copa's window and the pacer's budget allows it.
type junkSender struct {
	write   func([]byte) error // DataChannel.Send
	cc      *copa.Copa
	logf    func(kind string, t time.Time, data any)
	logPkts bool
	junk    []byte // every packet's payload

	mu         sync.Mutex
	nextSeq    uint32
	oldest     uint32                // every seq below this has been acked or declared lost
	sent       map[uint32]junkPacket // in flight
	highestAck int64                 // -1 until the first ack
	lastAck    time.Time             // when anything was last acknowledged
	c          junkCounters

	wake chan struct{}
}

type junkPacket struct {
	at   time.Time
	cwnd float64 // Copa's window when it left
}

type junkCounters struct {
	Packets, Bytes    int64
	Acked, AckedBytes int64
	Lost              int64 // TimedOut included
	TimedOut          int64 // lost because unacknowledged for too long, not because later packets were acked
	RepeatAcks        int64 // an arrival reported again (every one is, by design), or reported after it was declared lost
	Feedbacks         int64
	WriteErrors       int64
}

const (
	// junkReorderGap: a packet is lost once this many sent after it are acked.
	junkReorderGap = 3
	// junkMinRTO: the least time a packet stays unacknowledged before it is
	// given up on. The timeout proper is four smoothed RTTs.
	junkMinRTO = time.Second
)

func newJunkSender(write func([]byte) error, cc *copa.Copa, logPkts bool, logf func(string, time.Time, any)) *junkSender {
	junk := make([]byte, wire.MaxPayload)
	_, _ = rand.Read(junk)
	return &junkSender{write: write, cc: cc, logf: logf, logPkts: logPkts, junk: junk,
		sent: map[uint32]junkPacket{}, highestAck: -1, wake: make(chan struct{}, 1)}
}

func (s *junkSender) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// run is the send loop. The pacer is the media sender's: a byte budget that
// refills at Copa's pacing rate, may go one packet negative, and saves up at
// most a couple of milliseconds. On top of it sits the window: nothing leaves
// while cwnd packets are unacknowledged, and an ack wakes the loop.
func (s *junkSender) run(ctx context.Context) {
	const (
		size     = float64(wire.MaxMessage)
		maxBurst = 2 * time.Millisecond
		idle     = 20 * time.Millisecond // how often to look for timeouts when nothing else happens
	)
	var budget float64 // bytes
	last := time.Now()
	s.mu.Lock()
	s.lastAck = last
	s.mu.Unlock()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		s.mu.Lock()
		now := time.Now()
		s.detectLossLocked(now)
		rate := max(s.cc.PacingRate()*size, 1) // bytes per second
		budget = min(budget+now.Sub(last).Seconds()*rate, rate*maxBurst.Seconds())
		last = now
		room := func() bool { return float64(len(s.sent)+1) <= s.cc.Cwnd() }
		for budget >= 0 && room() {
			budget -= size
			s.sendLocked(now)
		}
		wait := idle
		if room() {
			wait = min(max(time.Duration(-budget/rate*float64(time.Second)), 250*time.Microsecond), idle)
		}
		s.mu.Unlock()

		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-timer.C:
		}
	}
}

func (s *junkSender) sendLocked(now time.Time) {
	seq := s.nextSeq
	s.nextSeq++
	buf := wire.NewData(wire.StreamVideo, seq, 0, 1, false, now, s.junk)
	wire.Stamp(buf, seq, now, false)
	if err := s.write(buf); err != nil {
		s.c.WriteErrors++
		return
	}
	cwnd := s.cc.Cwnd()
	s.sent[seq] = junkPacket{at: now, cwnd: cwnd}
	s.c.Packets++
	s.c.Bytes += int64(len(buf))
	if s.logPkts {
		s.logf("sent", now, map[string]any{"seq": seq, "len": len(buf), "cwnd": cwnd, "inFlight": len(s.sent)})
	}
}

// onFeedback handles one ack message: an RTT sample to Copa for every packet
// it acknowledges for the first time.
func (s *junkSender) onFeedback(b []byte, arrival time.Time) {
	fb, err := wire.ParseFeedback(b)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.c.Feedbacks++
	for _, a := range fb.Acks {
		p, ok := s.sent[a.Seq]
		if !ok {
			s.c.RepeatAcks++
			continue
		}
		delete(s.sent, a.Seq)
		// How long the browser held the arrival before this message left: next
		// to nothing for the newest, one or more packet intervals for repeats.
		hold := max(time.Duration((fb.SendMs-a.RecvMs)*float64(time.Millisecond)), 0)
		rtt := arrival.Sub(p.at) - hold
		s.c.Acked++
		s.c.AckedBytes += wire.MaxMessage
		s.highestAck = max(s.highestAck, int64(a.Seq))
		s.lastAck = arrival
		s.cc.OnAck(arrival, p.at, rtt, p.cwnd)
		if s.logPkts {
			s.logf("ack", arrival, map[string]any{"seq": a.Seq, "rttMs": ms(rtt), "recvMs": a.RecvMs, "holdMs": ms(hold), "cwnd": s.cc.Cwnd()})
		}
	}
	s.detectLossLocked(arrival)
	s.kick()
}

// detectLossLocked gives up on packets from the oldest outstanding onwards:
// those with junkReorderGap later packets acknowledged, and those out for
// longer than the timeout. If the timeout fires with no ack at all in that
// time the path has gone quiet, and Copa is told to start over.
func (s *junkSender) detectLossLocked(now time.Time) {
	rto := max(junkMinRTO, 4*s.cc.SRTT())
	lost, timedOut := 0, false
	for ; s.oldest != s.nextSeq; s.oldest++ {
		p, ok := s.sent[s.oldest]
		if !ok {
			continue // acknowledged
		}
		behind := int64(s.oldest) < s.highestAck-junkReorderGap+1
		late := now.Sub(p.at) > rto
		if !behind && !late {
			break
		}
		delete(s.sent, s.oldest)
		s.c.Lost++
		lost++
		if !behind {
			s.c.TimedOut++
			timedOut = true
		}
		if s.logPkts {
			s.logf("lost", now, map[string]any{"seq": s.oldest, "timeout": !behind})
		}
	}
	if lost > 0 {
		s.cc.OnLoss(now, lost)
	}
	if timedOut && now.Sub(s.lastAck) > rto {
		s.cc.OnTimeout()
		s.lastAck = now
	}
}

func (s *junkSender) snapshot() (map[string]any, junkCounters) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.cc.Stats()
	bps := func(pps float64) float64 { return pps * wire.MaxMessage * 8 }
	snap := map[string]any{
		"counters": s.c, "inFlight": len(s.sent), "copa": st, "packetBytes": wire.MaxMessage,
		// Copa's rates as bits per second of DataChannel messages.
		"windowBitrate": bps(st.CurrentRate), "pacingBitrate": bps(st.PacingRate),
	}
	if st.TargetRate != nil {
		snap["targetBitrate"] = bps(*st.TargetRate)
	}
	return snap, s.c
}
