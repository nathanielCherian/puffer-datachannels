// Package ratecontrol is where a rate controller plugs in. The sender tells
// the controller about every packet it puts on the wire and every feedback
// message the browser returns; the controller answers two questions: how fast
// should the encoder produce (TargetBitrate) and how fast may the pacer drain
// (PacingRate). Nothing else in the stack limits the send rate: SCTP's own
// congestion control is switched off (see session.Settings).
//
// To add a controller: implement Controller, Register it under a name in an
// init function, and start the server with --cc <name>.
package ratecontrol

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// SentPacket is one DATA packet as it left the pacer.
type SentPacket struct {
	Seq      uint32 // transport-wide, one per transmission
	Size     int    // DataChannel message bytes (header + payload); SCTP/DTLS/UDP/IP add roughly 93 more
	SendTime time.Time
	FrameID  uint32
	Audio    bool // an Opus packet (FrameID then counts audio packets)
	Key      bool // part of a keyframe
	Rtx      bool // a NACK retransmission
}

// PacketResult is the fate of one sent packet.
type PacketResult struct {
	SentPacket
	Received bool
	// RecvMs is the arrival time on the browser's clock (ms). The clocks are not
	// synchronised: use differences (delay variation), not the absolute delay.
	RecvMs float64
}

// Feedback is one FEEDBACK message joined with the sender's own records.
type Feedback struct {
	Arrival time.Time
	// Results holds every packet first acknowledged by this message and every
	// packet now declared lost, in seq order. Each sent packet appears in
	// exactly one Feedback.
	Results []PacketResult
	// RTT is measured on the newest acknowledged packet (feedback arrival minus
	// its send time, minus the time the browser held it). Zero if unknown.
	RTT time.Duration
}

// Controller decides the send rate. All methods are called from the sender
// with its lock held: they must be quick and must not call back into it.
type Controller interface {
	OnPacketSent(p SentPacket)
	OnFeedback(fb Feedback)
	// TargetBitrate is the total rate to aim for, in bits per second. The
	// encoder is configured to a fraction of it (--headroom).
	TargetBitrate() int
	// PacingRate is how fast the pacer may put queued packets on the wire, in
	// bits per second. It should exceed TargetBitrate so that frames larger
	// than average (keyframes) do not build a standing queue.
	PacingRate() int
	// Stats is logged with every snapshot and shown on the page.
	Stats() map[string]any
}

// Config is what the server passes to a controller factory.
type Config struct {
	StartBitrate, MinBitrate, MaxBitrate int // bps
	PacingFactor                         float64
}

// Factory creates one controller per session.
type Factory func(Config) Controller

var (
	mu        sync.Mutex
	factories = map[string]Factory{}
)

func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	factories[name] = f
}

func New(name string, cfg Config) (Controller, error) {
	mu.Lock()
	f := factories[name]
	mu.Unlock()
	if f == nil {
		return nil, fmt.Errorf("unknown rate controller %q (have %v)", name, Names())
	}
	return f(cfg), nil
}

func Names() []string {
	mu.Lock()
	defer mu.Unlock()
	names := make([]string, 0, len(factories))
	for n := range factories {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func init() {
	Register("fixed", func(cfg Config) Controller { return &Fixed{cfg: cfg} })
	Register("sweep", func(cfg Config) Controller { return &Sweep{Fixed: Fixed{cfg: cfg}, start: time.Now()} })
}

// Fixed sends at StartBitrate no matter what the network does. It only
// counts what it is told, as a template for a real controller.
type Fixed struct {
	cfg             Config
	sent, acked     int
	lost            int
	lastRTT         time.Duration
	lastRecvMs      float64
	lastSend        time.Time
	lastDelayVarMs  float64
	haveLastArrival bool
}

func (f *Fixed) OnPacketSent(SentPacket) { f.sent++ }

func (f *Fixed) OnFeedback(fb Feedback) {
	if fb.RTT > 0 {
		f.lastRTT = fb.RTT
	}
	for _, r := range fb.Results {
		if !r.Received {
			f.lost++
			continue
		}
		f.acked++
		// Delay variation between consecutive arrivals: how much later (or
		// earlier) this packet arrived than its send spacing predicts. This is
		// the raw signal a delay-based controller works from.
		if f.haveLastArrival {
			f.lastDelayVarMs = (r.RecvMs - f.lastRecvMs) - float64(r.SendTime.Sub(f.lastSend).Microseconds())/1000
		}
		f.lastRecvMs, f.lastSend, f.haveLastArrival = r.RecvMs, r.SendTime, true
	}
}

func (f *Fixed) TargetBitrate() int { return f.cfg.StartBitrate }

func (f *Fixed) PacingRate() int { return int(float64(f.cfg.StartBitrate) * f.cfg.PacingFactor) }

func (f *Fixed) Stats() map[string]any {
	return map[string]any{
		"name": "fixed", "sent": f.sent, "acked": f.acked, "lost": f.lost,
		"rttMs": float64(f.lastRTT.Microseconds()) / 1000, "delayVarMs": f.lastDelayVarMs,
	}
}

// SweepPeriod is how long Sweep takes to go from MinBitrate to MaxBitrate and back.
const SweepPeriod = 60 * time.Second

// Sweep ignores the network, like Fixed, but its target moves: from MinBitrate
// up to MaxBitrate and back down once every SweepPeriod, by the same ratio
// every second, starting at the bottom. It is for exercising whatever follows
// the target (the encoder's bitrate, which simulcast rendition is forwarded)
// without a network that makes a real controller move.
type Sweep struct {
	Fixed
	start time.Time
}

func (s *Sweep) TargetBitrate() int { return s.at(time.Since(s.start)) }

func (s *Sweep) at(elapsed time.Duration) int {
	lo, hi := float64(s.cfg.MinBitrate), float64(s.cfg.MaxBitrate)
	if lo <= 0 || hi <= lo {
		return s.cfg.StartBitrate
	}
	phase := math.Mod(elapsed.Seconds()/SweepPeriod.Seconds(), 1)
	up := 1 - math.Abs(2*phase-1) // 0 at the bottom, 1 at the top
	return int(lo * math.Pow(hi/lo, up))
}

func (s *Sweep) PacingRate() int { return int(float64(s.TargetBitrate()) * s.cfg.PacingFactor) }

func (s *Sweep) Stats() map[string]any {
	st := s.Fixed.Stats()
	st["name"] = "sweep"
	return st
}
