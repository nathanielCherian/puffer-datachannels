// Package copa is the Copa congestion controller: Arun and Balakrishnan,
// "Copa: Practical Delay-Based Congestion Control for the Internet", NSDI 2018.
// Section numbers below refer to that paper.
//
// Copa is window based. The sender keeps at most Cwnd packets in flight, paces
// them at PacingRate, and tells the controller about every acknowledgement
// (OnAck, with that packet's RTT) and every loss (OnLoss). On each ack (§2.1):
//
//	dq = RTTstanding - RTTmin         queueing delay
//	λt = 1 / (δ · dq)                 target rate, packets per second
//	λ  = cwnd / RTTstanding           current rate
//	λ <= λt:  cwnd += v / (δ · cwnd)  else  cwnd -= v / (δ · cwnd)
//
// RTTmin is the smallest RTT of the last 10 s, RTTstanding the smallest of the
// last srtt/2 (which filters out ack jitter), and v is a velocity that doubles
// while cwnd keeps moving the same way. In steady state this oscillates
// around 1/δ packets queued at the bottleneck, emptying the queue once every
// five RTTs.
//
// The package only does arithmetic on what it is told: it has no clock, no
// timers and no locks, so the caller serialises the calls.
package copa

import (
	"math"
	"time"
)

// Mode selects how δ is chosen (§2.2).
type Mode string

const (
	// ModeDefault keeps δ fixed: Copa as a pure delay-based scheme. This is
	// what §3 analyses, and what a zero Config gives.
	ModeDefault Mode = "default"
	// ModeCompetitive always runs AIMD on 1/δ, driven by loss.
	ModeCompetitive Mode = "competitive"
	// ModeAuto is the paper's mode switching: default while the queue is seen
	// nearly empty every five RTTs, competitive otherwise (something else is
	// keeping the buffer full). It is implemented as written, and as written
	// it misfires: see the note on updateMode.
	ModeAuto Mode = "auto"
)

// Config holds Copa's constants. Zero fields take the defaults in brackets.
type Config struct {
	Delta        float64       // δ in default mode [0.5]
	Mode         Mode          // [ModeDefault]
	InitCwnd     float64       // packets [10]
	MinCwnd      float64       // packets [2]
	MaxCwnd      float64       // packets [1e6]
	MinRTTWindow time.Duration // period RTTmin is taken over [10 s]
	// MaxRate caps the sender in packets per second (0 = no cap). It is not
	// part of Copa: it stands in for an application that has only so much to
	// send. The pacing rate never exceeds it, and cwnd stops growing at what
	// that rate can keep in flight, so the window does not run away while the
	// cap, not the network, is what limits the flow.
	MaxRate float64
}

func (c Config) withDefaults() Config {
	if c.Delta <= 0 {
		c.Delta = 0.5
	}
	if c.Mode == "" {
		c.Mode = ModeDefault
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
	if c.MinRTTWindow <= 0 {
		c.MinRTTWindow = 10 * time.Second
	}
	return c
}

// ValidMode reports whether m names a mode.
func ValidMode(m Mode) bool { return m == ModeDefault || m == ModeCompetitive || m == ModeAuto }

// Copa is one flow's controller.
type Copa struct {
	cfg Config

	cwnd      float64 // packets
	slowStart bool
	delta     float64

	// Velocity (§2.1 step 4).
	velocity   float64
	direction  int       // +1 cwnd grew over the last window, -1 it shrank, 0 not known yet
	sameDir    int       // consecutive windows that kept the direction
	windowFrom time.Time // the current window is over when a packet sent after this is acked

	srtt, latest time.Duration
	rttMin       extremum // over MinRTTWindow
	rttStanding  extremum // over srtt/2
	standingMax  extremum // largest RTTstanding of the last 4 RTTs, for "nearly empty"

	// Mode switching (§2.2).
	competitive     bool
	lastNearlyEmpty time.Time
	lastDeltaStep   time.Time // last additive increase of 1/δ
	lastDeltaCut    time.Time // last halving of 1/δ

	increasing bool // what the last ack decided
	c          Counters
}

// Counters count what the controller did, for logs.
type Counters struct {
	Acks, Losses, Timeouts int64
	Increases, Decreases   int64 // acks that raised / lowered cwnd
	VelocityResets         int64
	ModeSwitches           int64
	SlowStartExits         int64
}

// New returns a controller in slow start.
func New(cfg Config) *Copa {
	cfg = cfg.withDefaults()
	return &Copa{
		cfg: cfg, cwnd: cfg.InitCwnd, slowStart: true, delta: cfg.Delta, velocity: 1,
		competitive: cfg.Mode == ModeCompetitive,
		rttMin:      extremum{}, rttStanding: extremum{}, standingMax: extremum{max: true},
	}
}

// Cwnd is the congestion window in packets (not an integer).
func (c *Copa) Cwnd() float64 { return c.cwnd }

// PacingRate is how fast the sender may release packets, in packets per
// second: twice cwnd/RTTstanding (§2.1), so that pacing imperfections do not
// keep the window from being used. Before the first RTT sample it assumes
// 100 ms.
func (c *Copa) PacingRate() float64 {
	standing := 100 * time.Millisecond
	if c.rttStanding.ok() {
		standing = c.rttStanding.best()
	}
	rate := 2 * c.cwnd / standing.Seconds()
	if c.cfg.MaxRate > 0 {
		rate = math.Min(rate, c.cfg.MaxRate)
	}
	return rate
}

// SRTT is the smoothed RTT (zero before the first sample).
func (c *Copa) SRTT() time.Duration { return c.srtt }

// OnAck processes the acknowledgement of one packet. now is when the ack
// arrived, sentAt when the packet left, rtt the round trip it measured, and
// cwndAtSend what Cwnd returned when it left.
func (c *Copa) OnAck(now, sentAt time.Time, rtt time.Duration, cwndAtSend float64) {
	if rtt <= 0 {
		return
	}
	c.c.Acks++

	// Step 1: srtt, the RTT filters and the queueing delay.
	c.latest = rtt
	if c.srtt == 0 {
		c.srtt = rtt
	} else {
		c.srtt += (rtt - c.srtt) / 8
	}
	c.rttMin.update(now, rtt, c.cfg.MinRTTWindow)
	c.rttStanding.update(now, rtt, c.srtt/2)
	standing, rttMin := c.rttStanding.best(), c.rttMin.best()
	dq := standing - rttMin
	c.standingMax.update(now, standing, 4*c.srtt)

	c.updateMode(now, dq)

	// Steps 2 and 3: compare the current rate with the target rate.
	c.increasing = dq <= 0 || c.cwnd/standing.Seconds() <= 1/(c.delta*dq.Seconds())

	if c.slowStart {
		// Doubling once per RTT is one packet per acked packet.
		if c.increasing {
			c.cwnd++
		} else {
			c.slowStart = false
			c.c.SlowStartExits++
		}
	}
	if !c.slowStart {
		c.updateVelocity(now, sentAt, cwndAtSend)
		step := c.velocity / (c.delta * c.cwnd)
		if c.increasing {
			c.cwnd += step
			c.c.Increases++
		} else {
			c.cwnd -= step
			c.c.Decreases++
		}
	}
	c.clamp(standing)
}

// updateVelocity is step 4. Once per window, that is when a packet sent after
// the previous comparison is acknowledged, cwnd is compared with what it was
// when that packet was sent. Moving the same way as in the window before
// doubles v, but only from the fourth such window on: in steady state the
// direction legitimately holds for 2.5 RTTs and v must stay 1 there.
func (c *Copa) updateVelocity(now, sentAt time.Time, cwndAtSend float64) {
	// Beyond the paper, as in deployed implementations: once v has grown, an
	// ack that decides against the current direction resets it at once rather
	// than at the end of the window, which would otherwise be a whole RTT of
	// moving the new way at the old speed.
	if d := c.ackDirection(); c.velocity > 1 && d != c.direction {
		c.resetVelocity(d)
		c.windowFrom = now
		return
	}
	if c.windowFrom.IsZero() {
		c.windowFrom = now
		return
	}
	if sentAt.Before(c.windowFrom) {
		return
	}
	dir := c.direction
	switch {
	case c.cwnd > cwndAtSend:
		dir = 1
	case c.cwnd < cwndAtSend:
		dir = -1
	}
	if dir == c.direction {
		c.sameDir++
		if c.sameDir >= 3 {
			c.velocity *= 2
		}
	} else {
		c.resetVelocity(dir)
	}
	// The reference implementation's bound: one RTT of acks changes cwnd by
	// v/δ packets, and that should not be more than cwnd itself.
	if lim := math.Max(1, c.delta*c.cwnd); c.velocity > lim {
		c.velocity = lim
	}
	c.windowFrom = now
}

func (c *Copa) ackDirection() int {
	if c.increasing {
		return 1
	}
	return -1
}

func (c *Copa) resetVelocity(dir int) {
	if c.velocity > 1 {
		c.c.VelocityResets++
	}
	c.velocity, c.direction, c.sameDir = 1, dir, 0
}

// updateMode is §2.2. The queue counts as nearly empty when the queueing
// delay is under a tenth of how far RTTstanding has swung above RTTmin in the
// last four RTTs. Alone, or among Copa flows, that happens every five RTTs;
// if it has not, something is holding the buffer full and Copa turns
// competitive: 1/δ then grows by one per RTT and halves on loss, the way a
// Reno window would, so that Copa takes a fair share instead of yielding.
//
// Two things to know before trusting ModeAuto, both seen in this package's
// simulator with nothing but Copa on the link. First, a Copa cycle bottoms out
// at exactly an empty queue, with no margin (it turns one RTT after crossing
// 1/δ packets, having shed 1/δ in that RTT), so where delay comes in steps of
// one packet time, some cycles stop at one packet, which is more than a tenth
// of a swing of four or five: a lone flow then turns competitive. Second,
// competitive mode does not reliably undo itself. Once 1/δ is large next to
// the path's BDP, cwnd settles instead of oscillating, the queue stops
// emptying, and only loss brings δ back up.
func (c *Copa) updateMode(now time.Time, dq time.Duration) {
	if c.cfg.Mode == ModeAuto {
		swing := c.standingMax.best() - c.rttMin.best()
		if c.lastNearlyEmpty.IsZero() || float64(dq) <= 0.1*float64(swing) {
			c.lastNearlyEmpty = now
		}
		competitive := now.Sub(c.lastNearlyEmpty) > 5*c.srtt
		if competitive != c.competitive {
			c.competitive = competitive
			c.c.ModeSwitches++
			c.delta = c.cfg.Delta // back to default: reset; into competitive: start from it
			c.lastDeltaStep = now
		}
	}
	if c.competitive && now.Sub(c.lastDeltaStep) >= c.srtt {
		c.delta = 1 / (1/c.delta + 1)
		c.lastDeltaStep = now
	}
}

// OnLoss reports n lost packets. In default mode Copa does not react to loss
// at all (§5.4); in competitive mode it halves 1/δ, at most once per RTT.
func (c *Copa) OnLoss(now time.Time, n int) {
	c.c.Losses += int64(n)
	if !c.competitive || (!c.lastDeltaCut.IsZero() && now.Sub(c.lastDeltaCut) < c.srtt) {
		return
	}
	c.delta = math.Min(2*c.delta, c.cfg.Delta)
	c.lastDeltaCut, c.lastDeltaStep = now, now
}

// OnTimeout is for the sender to call when nothing at all has been
// acknowledged for a retransmission timeout's worth of time. The paper does
// not cover this; like TCP, start again from a small window.
func (c *Copa) OnTimeout() {
	c.c.Timeouts++
	c.cwnd = c.cfg.MinCwnd
	c.slowStart = true
	c.velocity, c.direction, c.sameDir = 1, 0, 0
	c.windowFrom = time.Time{}
}

func (c *Copa) clamp(standing time.Duration) {
	hi := c.cfg.MaxCwnd
	if c.cfg.MaxRate > 0 {
		// Twice what MaxRate keeps in flight, mirroring the pacing rate being
		// twice cwnd/RTTstanding: enough room that RTT jitter does not make the
		// window the limit when the cap should be.
		hi = math.Min(hi, 2*c.cfg.MaxRate*standing.Seconds())
	}
	c.cwnd = math.Max(math.Min(c.cwnd, hi), c.cfg.MinCwnd)
}

// Stats is the controller's state, for logs and the page. Rates are in
// packets per second and times in milliseconds.
type Stats struct {
	Cwnd          float64  `json:"cwnd"`
	SlowStart     bool     `json:"slowStart"`
	Mode          string   `json:"mode"` // "default" or "competitive": what is in force now
	Delta         float64  `json:"delta"`
	Velocity      float64  `json:"velocity"`
	Direction     int      `json:"direction"`
	Increasing    bool     `json:"increasing"`
	RTTMinMs      float64  `json:"rttMinMs"`
	RTTStandingMs float64  `json:"rttStandingMs"`
	SRTTMs        float64  `json:"srttMs"`
	LatestRTTMs   float64  `json:"latestRttMs"`
	QueueDelayMs  float64  `json:"queueDelayMs"`
	CurrentRate   float64  `json:"currentRate"`          // λ = cwnd / RTTstanding
	TargetRate    *float64 `json:"targetRate,omitempty"` // λt; absent while the queueing delay is zero
	PacingRate    float64  `json:"pacingRate"`
	Counters      Counters `json:"counters"`
}

func (c *Copa) Stats() Stats {
	s := Stats{
		Cwnd: c.cwnd, SlowStart: c.slowStart, Mode: string(ModeDefault), Delta: c.delta,
		Velocity: c.velocity, Direction: c.direction, Increasing: c.increasing,
		SRTTMs: msf(c.srtt), LatestRTTMs: msf(c.latest), PacingRate: c.PacingRate(), Counters: c.c,
	}
	if c.competitive {
		s.Mode = string(ModeCompetitive)
	}
	if c.rttStanding.ok() {
		standing, rttMin := c.rttStanding.best(), c.rttMin.best()
		dq := standing - rttMin
		s.RTTStandingMs, s.RTTMinMs, s.QueueDelayMs = msf(standing), msf(rttMin), msf(dq)
		s.CurrentRate = c.cwnd / standing.Seconds()
		if dq > 0 {
			t := 1 / (c.delta * dq.Seconds())
			s.TargetRate = &t
		}
	}
	return s
}

func msf(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// extremum is a sliding-window minimum (or maximum) of a series of durations:
// a deque in which every element is better than all that follow it, so the
// head is the answer.
type extremum struct {
	max bool
	q   []sample
}

type sample struct {
	t time.Time
	v time.Duration
}

func (e *extremum) update(now time.Time, v time.Duration, window time.Duration) {
	for n := len(e.q); n > 0; n = len(e.q) {
		last := e.q[n-1].v
		if (!e.max && last < v) || (e.max && last > v) {
			break
		}
		e.q = e.q[:n-1]
	}
	e.q = append(e.q, sample{now, v})
	for len(e.q) > 1 && now.Sub(e.q[0].t) > window {
		e.q = e.q[1:]
	}
}

func (e *extremum) ok() bool { return len(e.q) > 0 }

func (e *extremum) best() time.Duration {
	if len(e.q) == 0 {
		return 0
	}
	return e.q[0].v
}
