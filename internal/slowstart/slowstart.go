// Package slowstart is not a published congestion controller. It is an
// experiment: slow start and nothing else, over and over.
//
//	probing  the window grows by one packet per acknowledged packet, which
//	         doubles it every round trip, until a packet is lost
//	waiting  the window is cut and held for Wait (one second), whatever
//	         happens
//
// and then it probes again, doubling from the window it held. So every cycle
// overshoots the path the way a connection's first slow start does, and the
// charts show what that costs: how far the queue grows before the first loss,
// and how many packets go with it.
//
// What the window is cut to matters. A loss is only noticed a round trip
// after the packet was sent, and a full queue makes that a long round trip;
// the window has gone on doubling meanwhile, to about twice what the path
// holds. Half of that is still all the path holds, and a flow cut to it sits
// through its wait with the buffer full, losing packets throughout (with the
// emulated link here: 15% of them). So the cut is to Beta times the window
// the lost packet was sent at, which is the window that first overflowed the
// path: bottleneck buffer and all. Half of that leaves the queue room.
//
// Losses while waiting are counted and otherwise ignored. Most of them are
// the rest of the overshoot, sent before the cut and only now found missing;
// the wait is not restarted for the others either, so that a path with some
// random loss still gets its probe once a second. If the cut window is still
// more than the path holds, the next probe meets loss at once and cuts again.
// A loss while probing only counts if the packet was sent in this probe.
//
// Like the other controllers here the package only does arithmetic on what it
// is told: no clock, no timers, no locks. Windows are in packets.
package slowstart

import (
	"math"
	"time"
)

// Config holds the constants. Zero fields take the defaults in brackets.
type Config struct {
	Beta     float64       // a loss cuts to this much of the window the lost packet left at [0.5]
	Wait     time.Duration // how long the cut window is held before probing again [1 s]
	InitCwnd float64       // packets [10]
	MinCwnd  float64       // packets [2]
	MaxCwnd  float64       // packets [1e6]
	// MaxRate caps the pacing rate in packets per second (0 = no cap),
	// standing in for an application with only so much to send. The window
	// needs no cap of its own: it only grows while it is full (see OnAck).
	MaxRate float64
}

func (c Config) withDefaults() Config {
	if c.Beta <= 0 || c.Beta >= 1 {
		c.Beta = 0.5
	}
	if c.Wait <= 0 {
		c.Wait = time.Second
	}
	if c.InitCwnd <= 0 {
		c.InitCwnd = 10
	}
	if c.MinCwnd <= 0 {
		c.MinCwnd = 2
	}
	if c.MaxCwnd <= 0 {
		c.MaxCwnd = 1e6
	}
	return c
}

// SlowStart is one flow's controller.
type SlowStart struct {
	cfg Config

	cwnd    float64
	peak    float64 // the window the packet that ended the last probe was sent at; 0 before the first
	reached float64 // the window by the time that loss was noticed
	// The window over the current probe, oldest first, for looking up what it
	// was when a packet left. Thinned to steps of 1%, so it stays short.
	hist []windowAt

	waiting    bool
	cutAt      time.Time // when the wait began
	probeStart time.Time // when this probe began: losses of packets sent before it are not its doing
	// After a timeout the probe begins at the next ack (OnTimeout is not told
	// the time); until then every loss is of a packet from before it.
	afterTimeout bool

	srtt, latest, rttMin time.Duration
	c                    Counters
}

type windowAt struct {
	t time.Time
	w float64
}

// Counters count what the controller did, for logs.
type Counters struct {
	Acks, Losses     int64
	CongestionEvents int64 // cuts: a probe that met loss
	Probes           int64 // slow starts begun, the first included
	IgnoredLosses    int64 // losses while waiting, or of packets sent before the probe
	Timeouts         int64
	AppLimitedAcks   int64 // acks that found the window not full
}

// New returns a controller in its first probe.
func New(cfg Config) *SlowStart {
	cfg = cfg.withDefaults()
	return &SlowStart{cfg: cfg, cwnd: cfg.InitCwnd, c: Counters{Probes: 1}}
}

// Cwnd is the congestion window in packets (not an integer).
func (s *SlowStart) Cwnd() float64 { return s.cwnd }

// SRTT is the smoothed RTT (zero before the first sample).
func (s *SlowStart) SRTT() time.Duration { return s.srtt }

// PacingRate is how fast the sender may release packets, in packets per
// second: what Linux does for a window-based controller, twice cwnd/srtt
// while the window is doubling and 1.2 times while it is held.
func (s *SlowStart) PacingRate() float64 {
	rtt := s.srtt
	if rtt == 0 {
		rtt = 100 * time.Millisecond
	}
	factor := 2.0
	if s.waiting {
		factor = 1.2
	}
	rate := factor * s.cwnd / rtt.Seconds()
	if s.cfg.MaxRate > 0 {
		rate = math.Min(rate, s.cfg.MaxRate)
	}
	return rate
}

// OnAck processes the acknowledgement of one packet. now is when the ack
// arrived and rtt the round trip it measured. windowFull says whether the
// window was what held the sender back: a window that is not being used is
// not being tested, and does not grow. When the packet left, and the window
// then, are not used.
func (s *SlowStart) OnAck(now, _ time.Time, rtt time.Duration, _ float64, windowFull bool) {
	if rtt <= 0 {
		return
	}
	s.c.Acks++
	s.latest = rtt
	if s.srtt == 0 {
		s.srtt = rtt
	} else {
		s.srtt += (rtt - s.srtt) / 8
	}
	if s.rttMin == 0 || rtt < s.rttMin {
		s.rttMin = rtt
	}
	if !windowFull {
		s.c.AppLimitedAcks++
	}
	if s.afterTimeout {
		s.afterTimeout, s.probeStart = false, now
		s.hist = s.hist[:0]
	}
	if s.waiting {
		if now.Sub(s.cutAt) < s.cfg.Wait {
			return
		}
		s.waiting, s.probeStart = false, now
		s.hist = s.hist[:0]
		s.c.Probes++
	}
	if len(s.hist) == 0 {
		s.hist = append(s.hist, windowAt{now, s.cwnd})
	}
	if windowFull {
		s.cwnd = math.Min(s.cwnd+1, s.cfg.MaxCwnd)
		if last := s.hist[len(s.hist)-1].w; s.cwnd >= last+math.Max(1, last/100) {
			s.hist = append(s.hist, windowAt{now, s.cwnd})
		}
	}
}

// windowWhenSent is the window when a packet of this probe left.
func (s *SlowStart) windowWhenSent(sentAt time.Time) float64 {
	w := s.cwnd
	for i := len(s.hist) - 1; i >= 0; i-- {
		w = s.hist[i].w
		if !s.hist[i].t.After(sentAt) {
			break
		}
	}
	return w
}

// OnLoss reports one lost packet and when it was sent. A loss in a probe, of
// a packet the probe sent, ends it: the window is cut, to Beta times what it
// was when that packet left, and the wait begins.
func (s *SlowStart) OnLoss(now, sentAt time.Time) {
	s.c.Losses++
	if s.waiting || s.afterTimeout || sentAt.Before(s.probeStart) {
		s.c.IgnoredLosses++
		return
	}
	s.c.CongestionEvents++
	s.peak, s.reached = s.windowWhenSent(sentAt), s.cwnd
	s.cwnd = math.Max(s.peak*s.cfg.Beta, s.cfg.MinCwnd)
	s.waiting, s.cutAt = true, now
}

// OnTimeout is for the sender to call when nothing at all has been
// acknowledged for a retransmission timeout: probe again at once, from the
// smallest window. What was in flight is lost and will be reported as such;
// none of it is the new probe's.
func (s *SlowStart) OnTimeout() {
	s.c.Timeouts++
	s.cwnd = s.cfg.MinCwnd
	s.waiting, s.afterTimeout = false, true
	s.c.Probes++
}

// Stats is the controller's state, for logs and the page. Rates are in
// packets per second and times in milliseconds.
type Stats struct {
	Cwnd        float64  `json:"cwnd"`
	Peak        *float64 `json:"peak,omitempty"`    // the window the packet that ended the last probe left at; absent before the first
	Reached     *float64 `json:"reached,omitempty"` // the window by the time that loss was noticed
	SlowStart   bool     `json:"slowStart"`         // probing
	State       string   `json:"state"`             // slow start or waiting
	Beta        float64  `json:"beta"`
	WaitMs      float64  `json:"waitMs"`
	RTTMinMs    float64  `json:"rttMinMs"` // smallest ever
	SRTTMs      float64  `json:"srttMs"`
	LatestRTTMs float64  `json:"latestRttMs"`
	CurrentRate float64  `json:"currentRate"` // cwnd / srtt
	PacingRate  float64  `json:"pacingRate"`
	Counters    Counters `json:"counters"`
}

func (s *SlowStart) Stats() Stats {
	st := Stats{
		Cwnd: s.cwnd, SlowStart: !s.waiting, State: "slow start", Beta: s.cfg.Beta, WaitMs: msf(s.cfg.Wait),
		RTTMinMs: msf(s.rttMin), SRTTMs: msf(s.srtt), LatestRTTMs: msf(s.latest),
		PacingRate: s.PacingRate(), Counters: s.c,
	}
	if s.waiting {
		st.State = "waiting"
	}
	if s.srtt > 0 {
		st.CurrentRate = s.cwnd / s.srtt.Seconds()
	}
	if s.peak > 0 {
		p, r := s.peak, s.reached
		st.Peak, st.Reached = &p, &r
	}
	return st
}

func msf(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
