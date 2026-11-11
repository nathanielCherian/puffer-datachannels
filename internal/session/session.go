// Package session owns one viewer: its PeerConnection (DataChannels only),
// the ffmpeg decode pipeline, the x264 encoder, and the sender that paces
// frames onto the "media" DataChannel.
package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"puffer-datachannels/internal/logs"
	mediasrc "puffer-datachannels/internal/media"
	"puffer-datachannels/internal/ratecontrol"
	"puffer-datachannels/internal/wire"
	"puffer-datachannels/internal/x264"
)

var (
	ErrTooManySessions = errors.New("too many active sessions")
	ErrBadOffer        = errors.New("bad offer")
)

// Config holds per-session tunables (set from flags).
type Config struct {
	Controller                           string
	StartBitrate, MinBitrate, MaxBitrate int // bps
	PacingFactor                         float64
	Headroom                             float64 // encoder bitrate = Headroom x controller target
	KeyIntSec                            float64 // 0 = IDR only on request
	X264Preset                           string
	X264Threads                          int
	MaxQueue                             time.Duration
	History                              time.Duration
	FeedbackInterval                     time.Duration // how often the browser reports arrivals
	SnapshotInterval                     time.Duration
	LogPackets                           bool
	LogDir                               string
	ConnectTimeout                       time.Duration
	MaxSessions                          int
	ICEServers                           []webrtc.ICEServer `json:"-"`
}

// Settings applies what this design needs from pion to a SettingEngine.
//
// A browser only speaks DataChannels as SCTP over DTLS, so SCTP cannot be
// removed from the path. What can be removed is everything in it that would
// second-guess our own sender:
//
//   - Reliability and ordering: the browser opens "media" unordered with
//     maxRetransmits 0, so pion abandons a chunk as soon as it is sent and never
//     retransmits it, and the browser delivers messages as they arrive.
//   - Fragmentation and bundling: every message is at most wire.MaxMessage
//     bytes, so it is one DATA chunk in one packet, and two never share one.
//   - Congestion control: pion never lets cwnd fall below MinCwnd. With a
//     floor far above anything in flight, slow start, the halving on loss and
//     the collapse to one MTU on a timeout all become no-ops, and a message
//     handed to Send goes straight to the wire. minCwnd 0 leaves pion's
//     congestion control on (to see the difference).
//
// What remains is the peer's receive window, which only matters if the page
// stops reading, and SCTP's SACKs, which cost a little uplink.
func Settings(se *webrtc.SettingEngine, minCwnd uint32) {
	if minCwnd > 0 {
		se.SetSCTPMinCwnd(minCwnd)
	}
}

// Manager creates and tracks sessions.
type Manager struct {
	cfg      Config
	info     *mediasrc.Info
	settings webrtc.SettingEngine
	host     map[string]any

	mu       sync.Mutex
	sessions map[string]*Session
}

func NewManager(cfg Config, info *mediasrc.Info, settings webrtc.SettingEngine, host map[string]any) *Manager {
	return &Manager{cfg: cfg, info: info, settings: settings, host: host, sessions: map[string]*Session{}}
}

func (m *Manager) ActiveSessions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

func (m *Manager) CloseAll() {
	m.mu.Lock()
	all := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		all = append(all, s)
	}
	m.mu.Unlock()
	for _, s := range all {
		s.Close("server shutting down")
	}
}

// OfferRequest is the body of POST /offer.
type OfferRequest struct {
	SDP     string          `json:"sdp"`
	Type    string          `json:"type"`
	Bitrate int             `json:"bitrate,omitempty"` // bps; overrides --bitrate for this session
	Client  json.RawMessage `json:"client,omitempty"`  // arbitrary client metadata, logged
}

// OfferResponse is returned from POST /offer.
type OfferResponse struct {
	SDP       string `json:"sdp"`
	Type      string `json:"type"`
	SessionID string `json:"sessionId"`
	LogDir    string `json:"logDir"`
}

// Session is one viewer.
type Session struct {
	id      string
	mgr     *Manager
	cfg     Config
	info    *mediasrc.Info
	created time.Time
	ctx     context.Context
	cancel  context.CancelFunc
	log     *logs.Session

	pc      *webrtc.PeerConnection
	ctrl    ratecontrol.Controller
	snd     atomic.Pointer[sender]
	mediaDC atomic.Pointer[webrtc.DataChannel]
	ctrlDC  atomic.Pointer[webrtc.DataChannel] // reliable: hello, snapshots, the page's stats

	enc         *x264.Encoder
	forceIDR    atomic.Bool
	keyRequests atomic.Int64
	started     atomic.Bool
	encMu       sync.Mutex
	encWin      encWindow
	encTotal    encWindow

	closeOnce sync.Once
}

type encWindow struct {
	Frames, Keyframes int
	Bytes             int64
	EncodeMs          float64
	since             time.Time
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// HandleOffer creates a session from a browser offer and returns the answer
// (with all ICE candidates included; no trickle).
func (m *Manager) HandleOffer(ctx context.Context, req OfferRequest, remoteAddr string) (*OfferResponse, error) {
	m.mu.Lock()
	full := m.cfg.MaxSessions > 0 && len(m.sessions) >= m.cfg.MaxSessions
	m.mu.Unlock()
	if full {
		return nil, ErrTooManySessions
	}
	if req.SDP == "" {
		return nil, fmt.Errorf("%w: empty sdp", ErrBadOffer)
	}
	cfg := m.cfg
	if req.Bitrate != 0 {
		if req.Bitrate < cfg.MinBitrate || req.Bitrate > cfg.MaxBitrate {
			return nil, fmt.Errorf("%w: bitrate must be within %d..%d bps", ErrBadOffer, cfg.MinBitrate, cfg.MaxBitrate)
		}
		cfg.StartBitrate = req.Bitrate
	}
	ctrl, err := ratecontrol.New(cfg.Controller, ratecontrol.Config{
		StartBitrate: cfg.StartBitrate, MinBitrate: cfg.MinBitrate, MaxBitrate: cfg.MaxBitrate, PacingFactor: cfg.PacingFactor,
	})
	if err != nil {
		return nil, err
	}

	s := &Session{id: newID(), mgr: m, cfg: cfg, info: m.info, created: time.Now(), ctrl: ctrl}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if s.log, err = logs.NewSession(cfg.LogDir, s.id); err != nil {
		return nil, fmt.Errorf("create log: %w", err)
	}
	s.event("session", "created")
	if err := s.setup(ctx, req); err != nil {
		s.Close("setup failed: " + err.Error())
		return nil, err
	}
	m.mu.Lock()
	m.sessions[s.id] = s
	m.mu.Unlock()

	// Give up if ICE/DTLS/SCTP never completes.
	go func() {
		select {
		case <-s.ctx.Done():
		case <-time.After(cfg.ConnectTimeout):
			if !s.started.Load() {
				s.Close("connect timeout")
			}
		}
	}()

	ld := s.pc.LocalDescription()
	_ = s.log.WriteMeta(map[string]any{
		"sessionId": s.id, "createdAt": s.created.UTC().Format(time.RFC3339Nano), "createdMs": logs.Ms(s.created),
		"remoteAddr": remoteAddr, "client": req.Client, "media": s.info, "config": cfg, "host": m.host,
		"offer": req.SDP, "answer": ld.SDP,
		"files": map[string]string{
			"events.jsonl":    "server state changes (pc/ice states, channels, media start, keyframe requests, dropped frames, close)",
			"cc.jsonl":        "the rate controller's inputs: sent (every DATA packet: seq, len, frame, rtx) and feedback (per-packet results with browser receive times, RTT, target and pacing rate afterwards)",
			"encoder.jsonl":   "every encoded frame (id, bytes, keyframe, encode time, configured bitrate, whether it was sent) and every encoder reconfiguration",
			"snapshots.jsonl": "periodic server snapshot (sender counters, pacer queue, rates, encoder window, SCTP buffered amount)",
			"client.jsonl":    "what the page reported over the ctrl channel (t = server receive time)",
		},
	})
	log.Printf("[%s] new session from %s (log: %s)", s.id, remoteAddr, s.log.Dir)
	return &OfferResponse{SDP: ld.SDP, Type: ld.Type.String(), SessionID: s.id, LogDir: s.log.Dir}, nil
}

func (s *Session) setup(ctx context.Context, req OfferRequest) error {
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
				s.mediaDC.Store(dc)
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
					"media": s.info, "config": s.cfg, "feedbackIntervalMs": ms(s.cfg.FeedbackInterval),
				})
			})
			dc.OnMessage(func(msg webrtc.DataChannelMessage) { s.onCtrlMessage(msg.Data) })
		default:
			s.event("dataChannel", "unexpected label "+dc.Label())
		}
	})

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: req.SDP}); err != nil {
		return fmt.Errorf("%w: %v", ErrBadOffer, err)
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return fmt.Errorf("create answer: %w", err)
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}
	select {
	case <-gathered:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return errors.New("ICE gathering timed out")
	}
	return nil
}

// onMediaMessage handles what the browser sends on the media channel.
func (s *Session) onMediaMessage(dc *webrtc.DataChannel, b []byte) {
	now := time.Now()
	if len(b) == 0 {
		return
	}
	snd := s.snd.Load()
	switch b[0] {
	case wire.TypePing:
		if len(b) >= 13 {
			_ = dc.Send(wire.Pong(b, now, time.Now()))
		}
	case wire.TypeFeedback:
		if snd != nil {
			snd.onFeedback(b, now)
		}
	case wire.TypeNack:
		if snd != nil {
			snd.onNack(b)
		}
	case wire.TypeKeyReq:
		s.keyRequests.Add(1)
		if s.forceIDR.CompareAndSwap(false, true) {
			s.event("keyframeRequest", "browser asked for a keyframe")
		}
	}
}

// onCtrlMessage logs whatever JSON the page reports.
func (s *Session) onCtrlMessage(b []byte) {
	var rec struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(b, &rec) != nil || rec.Type == "" {
		return
	}
	s.log.Log("client", rec.Type, rec.Data)
}

// run starts media once the media channel is open.
func (s *Session) run(dc *webrtc.DataChannel) {
	keyint := 0
	if s.cfg.KeyIntSec > 0 {
		keyint = int(math.Round(s.cfg.KeyIntSec * s.info.FPS))
	}
	initial := s.videoBitrateFor(s.ctrl.TargetBitrate())
	var err error
	s.enc, err = x264.New(x264.Config{
		Width: s.info.Width, Height: s.info.Height, FPSNum: s.info.FPSNum, FPSDen: s.info.FPSDen,
		BitrateBps: initial, KeyInt: keyint, Threads: s.cfg.X264Threads, Preset: s.cfg.X264Preset,
	})
	if err != nil {
		s.Close("encoder: " + err.Error())
		return
	}
	go func() {
		<-s.ctx.Done()
		s.enc.Close()
	}()
	s.log.Log("encoder", "reconfig", map[string]any{"target": s.ctrl.TargetBitrate(), "applied": s.enc.Bitrate(), "initial": true,
		"preset": s.cfg.X264Preset, "keyint": keyint, "headroom": s.cfg.Headroom})

	snd := newSender(dc.Send, s.ctrl, senderConfig{
		MaxQueue: s.cfg.MaxQueue, History: s.cfg.History, ReorderGap: 16, LossTimeout: 2 * time.Second,
	}, s.cfg.LogPackets, func(kind string, t time.Time, data any) { s.log.LogAt("cc", t, kind, data) })
	s.snd.Store(snd)

	pipe, err := mediasrc.StartPipeline(s.ctx, s.info)
	if err != nil {
		s.Close("pipeline: " + err.Error())
		return
	}
	go func() {
		<-s.ctx.Done()
		pipe.Close()
	}()
	s.event("media", "started")
	s.encWin.since = time.Now()
	go snd.run(s.ctx)
	go s.videoLoop(pipe, snd)
	go s.bitrateLoop()
	go s.snapshotLoop(dc, snd)
}

// videoBitrateFor converts the controller's target into an encoder target.
func (s *Session) videoBitrateFor(target int) int {
	return max(int(float64(target)*s.cfg.Headroom), 30_000)
}

// bitrateLoop follows the controller's target. With the fixed controller it
// never fires; it is here so a controller that changes its mind is obeyed.
func (s *Session) bitrateLoop() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		snd := s.snd.Load()
		snd.mu.Lock()
		target := s.ctrl.TargetBitrate()
		snd.mu.Unlock()
		want := s.videoBitrateFor(target)
		cur := s.enc.Bitrate()
		if cur > 0 && math.Abs(float64(want-cur))/float64(cur) < 0.03 {
			continue
		}
		if err := s.enc.SetBitrate(want); err != nil {
			log.Printf("[%s] set bitrate: %v", s.id, err)
			continue
		}
		s.log.Log("encoder", "reconfig", map[string]any{"target": target, "applied": s.enc.Bitrate(), "previous": cur})
	}
}

func (s *Session) videoLoop(pipe *mediasrc.Pipeline, snd *sender) {
	frameDur := time.Duration(float64(time.Second) * float64(s.info.FPSDen) / float64(s.info.FPSNum))
	buf := make([]byte, s.enc.FrameSize())
	start := time.Now()
	var frameID uint32
	for n := 0; ; n++ {
		if err := pipe.ReadVideoFrame(buf); err != nil {
			s.Close("video read: " + err.Error())
			return
		}
		if !sleepUntil(s.ctx, start.Add(time.Duration(n)*frameDur)) {
			return
		}
		forced := s.forceIDR.Swap(false)
		capture := time.Now()
		f, err := s.enc.Encode(buf, forced)
		encDur := time.Since(capture)
		if err != nil {
			s.Close("encode: " + err.Error())
			return
		}
		if f == nil {
			continue
		}
		// Every frame references the one before it, so a frame the pacer
		// refuses breaks the chain: the next one must be a keyframe.
		queued := snd.enqueueFrame(frameID, f.Data, f.Keyframe, capture)
		if !queued {
			s.forceIDR.Store(true)
			s.event("frameDropped", fmt.Sprintf("frame %d: pacer queue over %v", frameID, s.cfg.MaxQueue))
		}
		s.encMu.Lock()
		for _, w := range []*encWindow{&s.encWin, &s.encTotal} {
			w.Frames++
			w.Bytes += int64(len(f.Data))
			w.EncodeMs += ms(encDur)
			if f.Keyframe {
				w.Keyframes++
			}
		}
		s.encMu.Unlock()
		s.log.LogAt("encoder", capture, "frame", map[string]any{
			"id": frameID, "n": n, "bytes": len(f.Data), "key": f.Keyframe, "forced": forced && f.Keyframe,
			"encMs": ms(encDur), "cfgBps": s.enc.Bitrate(), "queued": queued,
		})
		frameID++
	}
}

func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Session) snapshotLoop(dc *webrtc.DataChannel, snd *sender) {
	t := time.NewTicker(s.cfg.SnapshotInterval)
	defer t.Stop()
	var prevBytes, prevRtx int64
	prevT := time.Now()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		dt := now.Sub(prevT).Seconds()
		snap := snd.snapshot()
		c := snap["counters"].(senderCounters)
		snap["sendBitrate"] = float64(c.Bytes-prevBytes) * 8 / dt
		snap["rtxBitrate"] = float64(c.RtxBytes-prevRtx) * 8 / dt
		prevBytes, prevRtx, prevT = c.Bytes, c.RtxBytes, now

		s.encMu.Lock()
		w, total := s.encWin, s.encTotal
		s.encWin = encWindow{since: now}
		s.encMu.Unlock()
		enc := map[string]any{"configuredBitrate": s.enc.Bitrate(), "frames": total.Frames, "keyframes": total.Keyframes, "bytes": total.Bytes}
		if wdt := now.Sub(w.since).Seconds(); wdt > 0 && w.Frames > 0 {
			enc["bitrate"] = float64(w.Bytes) * 8 / wdt
			enc["fps"] = float64(w.Frames) / wdt
			enc["encodeMs"] = w.EncodeMs / float64(w.Frames)
		}
		snap["encoder"] = enc
		snap["keyRequests"] = s.keyRequests.Load()
		// Bytes SCTP has accepted but the browser has not yet SACKed. With SCTP's
		// congestion control out of the way this stays near one RTT of data; if it
		// grows, SCTP is holding packets back.
		snap["sctpBufferedBytes"] = dc.BufferedAmount()
		snap["uptimeSec"] = now.Sub(s.created).Seconds()
		s.log.Log("snapshots", "snapshot", snap)
		s.sendCtrl(map[string]any{"type": "server-stats", "data": snap})
	}
}

func (s *Session) sendCtrl(v any) {
	dc := s.ctrlDC.Load()
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return
	}
	if b, err := json.Marshal(v); err == nil {
		_ = dc.SendText(string(b))
	}
}

func (s *Session) event(kind string, value any) {
	s.log.Log("events", kind, value)
}

// Close ends the session (idempotent).
func (s *Session) Close(reason string) {
	s.closeOnce.Do(func() {
		log.Printf("[%s] closed: %s", s.id, reason)
		s.event("closed", reason)
		s.cancel()
		if s.pc != nil {
			go func() { _ = s.pc.Close() }()
		}
		s.mgr.mu.Lock()
		delete(s.mgr.sessions, s.id)
		s.mgr.mu.Unlock()
		// Let the last log lines from the stopping goroutines land.
		time.AfterFunc(500*time.Millisecond, s.log.Close)
	})
}
