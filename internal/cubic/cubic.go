// Package cubic is the CUBIC congestion controller as specified in RFC 9438
// (Xu, Ha, Rhee, Goel, Eggert, Scheffenegger). Section numbers below refer to
// the RFC.
//
// CUBIC is window based and driven by loss. The sender keeps at most Cwnd
// packets in flight, and tells the controller about every acknowledgement
// (OnAck) and every lost packet (OnLoss). Between losses the window follows a
// cubic function of the time since the last reduction (§4.2):
//
//	W_cubic(t) = C · (t - K)³ + W_max        K = ∛((W_max - cwnd_epoch) / C)
//
// which climbs quickly back towards W_max, the window at the last loss,
// flattens around it, and then probes beyond it ever faster. On a loss the
// window is multiplied by β = 0.7 (§4.6). Where that curve would be slower
// than Reno, the window follows an estimate of Reno's instead (§4.3).
//
// Like internal/copa the package only does arithmetic on what it is told: no
// clock, no timers, no locks. Windows are in packets.
//
// Left out: HyStart++ (slow start ends at the first loss, as in the RFC's
// base algorithm), and the spurious-loss undo of §4.9. Nothing here is
// retransmitted, so "recovery" is only the time until a packet sent after the
// reduction is acknowledged; the window does not grow during it. The
// reduction is taken from cwnd rather than the flight size (§4.6 allows
// either), which is safe here because the sender always fills the window or
// is held by MaxRate, and MaxRate bounds the window.
package cubic

import (
	"math"
	"time"
)

// Config holds CUBIC's constants. Zero fields take the defaults in brackets.
type Config struct {
	C        float64 // how aggressively the window grows, packets per second cubed [0.4]
	Beta     float64 // what a loss multiplies the window by [0.7]
	InitCwnd float64 // packets [10]
	MinCwnd  float64 // packets [2]
	MaxCwnd  float64 // packets [1e6]
	// MaxRate caps the sender in packets per second (0 = no cap), standing in
	// for an application with only so much to send. The pacing rate never
	// exceeds it, and the window stops growing at twice what that rate keeps
	// in flight (§5.8: a window that is not being used must not grow).
	MaxRate float64
}

func (c Config) withDefaults() Config {
	if c.C <= 0 {
		c.C = 0.4
	}
	if c.Beta <= 0 || c.Beta >= 1 {
		c.Beta = 0.7
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

// Cubic is one flow's controller.
type Cubic struct {
	cfg Config

	cwnd     float64 // packets
	ssthresh float64 // +Inf until the first loss

	// The current congestion-avoidance epoch (§4.2). epochStart is zero when
	// the next ack must begin one.
	epochStart time.Time
	wMax       float64 // the window just before the last reduction (lowered by fast convergence)
	k          float64 // seconds from the epoch's start until W_cubic reaches wMax
	wEst       float64 // what Reno's window would be (§4.3)
	cwndPrior  float64 // the window before the last reduction

	// A loss among packets sent before recoveryStart was already paid for.
	recoveryStart time.Time
	inRecovery    bool
	afterTimeout  bool // the next epoch is the first since a timeout

	srtt, latest, rttMin time.Duration
	c                    Counters
}

// Counters count what the controller did, for logs.
type Counters struct {
	Acks, Losses     int64
	CongestionEvents int64 // window reductions: at most one per window of losses
	Timeouts         int64
	FastConvergences int64
	RenoFriendlyAcks int64 // acks on which the Reno estimate, not the cubic curve, set the window
}

// New returns a controller in slow start.
func New(cfg Config) *Cubic {
	cfg = cfg.withDefaults()
	return &Cubic{cfg: cfg, cwnd: cfg.InitCwnd, ssthresh: math.Inf(1)}
}

// Cwnd is the congestion window in packets (not an integer).
func (c *Cubic) Cwnd() float64 { return c.cwnd }

// SRTT is the smoothed RTT (zero before the first sample).
func (c *Cubic) SRTT() time.Duration { return c.srtt }

func (c *Cubic) slowStart() bool { return c.cwnd < c.ssthresh }

func (c *Cubic) rtt() time.Duration {
	if c.srtt == 0 {
		return 100 * time.Millisecond
	}
	return c.srtt
}

// PacingRate is how fast the sender may release packets, in packets per
// second. CUBIC does not define one; this is what Linux does for it: twice
// cwnd/srtt in slow start, so that the window can double in a round trip, and
// 1.2 times in congestion avoidance.
func (c *Cubic) PacingRate() float64 {
	factor := 1.2
	if c.slowStart() {
		factor = 2
	}
	rate := factor * c.cwnd / c.rtt().Seconds()
	if c.cfg.MaxRate > 0 {
		rate = math.Min(rate, c.cfg.MaxRate)
	}
	return rate
}

// OnAck processes the acknowledgement of one packet. now is when the ack
// arrived, sentAt when the packet left and rtt the round trip it measured.
// The last argument (the window when the packet left) is not used; it is
// there so that Copa and CUBIC have the same shape.
func (c *Cubic) OnAck(now, sentAt time.Time, rtt time.Duration, _ float64) {
	if rtt <= 0 {
		return
	}
	c.c.Acks++
	c.latest = rtt
	if c.srtt == 0 {
		c.srtt = rtt
	} else {
		c.srtt += (rtt - c.srtt) / 8
	}
	if c.rttMin == 0 || rtt < c.rttMin {
		c.rttMin = rtt
	}

	if c.inRecovery {
		if sentAt.Before(c.recoveryStart) {
			return
		}
		c.inRecovery = false
	}
	defer c.clamp()

	if c.slowStart() {
		c.cwnd++
		return
	}

	if c.epochStart.IsZero() {
		// §4.2: a new epoch starts from the current window. K may be negative
		// (the window is already above wMax); the curve still passes through
		// cwnd at t = 0.
		c.epochStart = now
		if c.afterTimeout {
			// §4.8: after a timeout the curve starts at its inflection point.
			c.wMax, c.afterTimeout = c.cwnd, false
		}
		c.k = math.Cbrt((c.wMax - c.cwnd) / c.cfg.C)
		c.wEst = c.cwnd
	}
	t := now.Sub(c.epochStart).Seconds()
	wCubic := func(t float64) float64 { d := t - c.k; return c.cfg.C*d*d*d + c.wMax }

	// §4.3: Reno's window over the same epoch. Until it is back to where the
	// window was before the loss, its additive increase is scaled so that a
	// flow with β = 0.7 averages what Reno does with 0.5.
	alpha := 3 * (1 - c.cfg.Beta) / (1 + c.cfg.Beta)
	if c.wEst >= c.cwndPrior {
		alpha = 1
	}
	c.wEst += alpha / c.cwnd

	if wCubic(t) < c.wEst {
		c.c.RenoFriendlyAcks++
		c.cwnd = math.Max(c.cwnd, c.wEst)
		return
	}
	// §4.4, §4.5: aim for where the curve will be one RTT from now, but for no
	// more than half a window of growth per RTT.
	target := math.Min(math.Max(wCubic(t+c.rtt().Seconds()), c.cwnd), 1.5*c.cwnd)
	c.cwnd += (target - c.cwnd) / c.cwnd
}

// OnLoss reports one lost packet and when it was sent. The first loss of a
// window reduces cwnd (§4.6); further losses among packets sent before that
// reduction are the same congestion event and change nothing.
func (c *Cubic) OnLoss(now, sentAt time.Time) {
	c.c.Losses++
	if !c.recoveryStart.IsZero() && sentAt.Before(c.recoveryStart) {
		return
	}
	c.c.CongestionEvents++
	c.reduce()
	c.cwnd = c.ssthresh
	c.recoveryStart, c.inRecovery = now, true
}

// OnTimeout is for the sender to call when nothing at all has been
// acknowledged for a retransmission timeout (§4.8): slow start again, from
// the smallest window, up to β times the old one.
func (c *Cubic) OnTimeout() {
	c.c.Timeouts++
	c.reduce()
	c.cwnd = c.cfg.MinCwnd
	c.inRecovery, c.afterTimeout = false, true
}

// reduce records the window that failed and sets ssthresh from it.
func (c *Cubic) reduce() {
	c.cwndPrior = c.cwnd
	if c.cwnd < c.wMax {
		// §4.7, fast convergence: losing again below the previous peak means
		// there is less room than before (another flow arrived), so aim lower
		// and leave the difference to it.
		c.wMax = c.cwnd * (1 + c.cfg.Beta) / 2
		c.c.FastConvergences++
	} else {
		c.wMax = c.cwnd
	}
	c.ssthresh = math.Max(c.cwnd*c.cfg.Beta, c.cfg.MinCwnd)
	c.epochStart = time.Time{}
}

func (c *Cubic) clamp() {
	hi := c.cfg.MaxCwnd
	if c.cfg.MaxRate > 0 {
		hi = math.Min(hi, 2*c.cfg.MaxRate*c.rtt().Seconds())
	}
	c.cwnd = math.Max(math.Min(c.cwnd, hi), c.cfg.MinCwnd)
}

// Stats is the controller's state, for logs and the page. Rates are in
// packets per second and times in milliseconds.
type Stats struct {
	Cwnd        float64  `json:"cwnd"`
	Ssthresh    *float64 `json:"ssthresh,omitempty"` // absent before the first loss
	SlowStart   bool     `json:"slowStart"`
	State       string   `json:"state"` // slow start, congestion avoidance or recovery
	WMax        float64  `json:"wMax"`
	K           float64  `json:"k"` // seconds
	WEst        float64  `json:"wEst"`
	EpochSec    float64  `json:"epochSec"` // time since the current epoch began
	SRTTMs      float64  `json:"srttMs"`
	RTTMinMs    float64  `json:"rttMinMs"`
	LatestRTTMs float64  `json:"latestRttMs"`
	CurrentRate float64  `json:"currentRate"` // cwnd / srtt
	PacingRate  float64  `json:"pacingRate"`
	Counters    Counters `json:"counters"`
}

// Stats takes the time so that it can say how far into the epoch the flow is.
func (c *Cubic) Stats(now time.Time) Stats {
	s := Stats{
		Cwnd: c.cwnd, SlowStart: c.slowStart(), State: "congestion avoidance", WMax: c.wMax, K: c.k, WEst: c.wEst,
		SRTTMs: msf(c.srtt), RTTMinMs: msf(c.rttMin), LatestRTTMs: msf(c.latest),
		CurrentRate: c.cwnd / c.rtt().Seconds(), PacingRate: c.PacingRate(), Counters: c.c,
	}
	switch {
	case c.inRecovery:
		s.State = "recovery"
	case s.SlowStart:
		s.State = "slow start"
	}
	if !math.IsInf(c.ssthresh, 1) {
		v := c.ssthresh
		s.Ssthresh = &v
	}
	if !c.epochStart.IsZero() {
		s.EpochSec = now.Sub(c.epochStart).Seconds()
	}
	return s
}

func msf(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
