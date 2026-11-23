// Package vegas is TCP Vegas: Brakmo and Peterson, "TCP Vegas: End to End
// Congestion Avoidance on a Global Internet", IEEE JSAC 1995, with the
// details the paper leaves open taken from Linux's tcp_vegas.c.
//
// Vegas is window based and driven by delay. baseRTT is the smallest RTT the
// flow has ever seen, taken to be the path with nothing queued. Once per
// round trip the window is compared with what it would deliver at that RTT:
//
//	expected = cwnd / baseRTT
//	actual   = cwnd / rtt                    rtt: the smallest RTT of this round
//	diff     = (expected - actual) · baseRTT = cwnd · (rtt - baseRTT) / rtt
//
// diff is how many of the flow's packets are sitting in queues. Vegas keeps
// it between α and β packets: under α the window grows by one packet per
// round, over β it shrinks by one, in between it is left alone. Slow start
// ends as soon as diff exceeds γ. Loss is handled as Reno handles it: the
// window is halved, once per window of losses.
//
// Like the other controllers here the package only does arithmetic on what it
// is told: no clock, no timers, no locks. Windows are in packets.
//
// Where the sources differ this follows Linux, except in two places. In slow
// start the paper doubles the window every other round trip, so that diff is
// measured over a round in which the window held still; Linux doubles it
// every round trip, and then the smallest RTT of a round comes from before
// the round's growth and the queue is seen a round late (in this package's
// simulator that cost 97 packets leaving slow start on a path where the
// paper's way cost 5: see TestSlowStartEndsOnDelay). This does it the paper's
// way. Either way the last doubling can overshoot the path by up to a BDP. And Linux divides
// diff by baseRTT rather than rtt, which overstates the queue as it grows;
// this uses the paper's formula. From Linux: the defaults α = 2, β = 4, γ = 1
// (the paper mostly ran 1 and 3; Config sets them), and that a round with
// fewer than three RTT samples cannot be trusted to hold an unqueued one and
// falls back to Reno's increase.
package vegas

import (
	"math"
	"time"
)

// Config holds Vegas's constants. Zero fields take the defaults in brackets.
type Config struct {
	Alpha    float64 // grow while fewer than this many packets are queued [2]
	Beta     float64 // shrink while more than this many are [4]
	Gamma    float64 // leave slow start once more than this many are [1]
	InitCwnd float64 // packets [10]
	MinCwnd  float64 // packets [2]
	MaxCwnd  float64 // packets [1e6]
	// MaxRate caps the pacing rate in packets per second (0 = no cap),
	// standing in for an application with only so much to send. The window
	// needs no cap of its own: it only grows while it is full (see OnAck).
	MaxRate float64
}

func (c Config) withDefaults() Config {
	if c.Alpha <= 0 {
		c.Alpha = 2
	}
	if c.Beta < c.Alpha {
		c.Beta = c.Alpha + 2
	}
	if c.Gamma <= 0 {
		c.Gamma = 1
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

// Vegas is one flow's controller.
type Vegas struct {
	cfg Config

	cwnd     float64
	ssthresh float64 // +Inf until slow start ends

	baseRTT time.Duration // smallest ever; 0 = no sample yet

	// The current round: it ends when a packet sent after roundFrom is
	// acknowledged.
	roundFrom time.Time
	roundMin  time.Duration // smallest RTT of the round
	roundCnt  int           // RTT samples in the round
	roundFull bool          // the window was full at some ack of the round
	ssHold    bool          // slow start: this round the window holds still, to be measured

	diff float64 // the last round's estimate of packets queued

	// A loss among packets sent before recoveryStart was already paid for.
	recoveryStart time.Time
	inRecovery    bool

	srtt, latest time.Duration
	c            Counters
}

// Counters count what the controller did, for logs.
type Counters struct {
	Acks, Losses     int64
	CongestionEvents int64 // halvings on loss: at most one per window of losses
	Timeouts         int64
	Rounds           int64
	Increases        int64 // rounds that grew the window (diff < α)
	Decreases        int64 // rounds that shrank it (diff > β)
	Holds            int64 // rounds that left it alone
	RenoRounds       int64 // rounds with too few RTT samples to judge
	SlowStartExits   int64
	AppLimitedAcks   int64 // acks that found the window not full
}

// New returns a controller in slow start.
func New(cfg Config) *Vegas {
	cfg = cfg.withDefaults()
	return &Vegas{cfg: cfg, cwnd: cfg.InitCwnd, ssthresh: math.Inf(1)}
}

// Cwnd is the congestion window in packets (not an integer).
func (v *Vegas) Cwnd() float64 { return v.cwnd }

// SRTT is the smoothed RTT (zero before the first sample).
func (v *Vegas) SRTT() time.Duration { return v.srtt }

func (v *Vegas) slowStart() bool { return v.cwnd < v.ssthresh }

// PacingRate is how fast the sender may release packets, in packets per
// second. Vegas defines none; this is what Linux does for a window-based
// controller: twice cwnd/srtt in slow start and 1.2 times after.
func (v *Vegas) PacingRate() float64 {
	rtt := v.srtt
	if rtt == 0 {
		rtt = 100 * time.Millisecond
	}
	factor := 1.2
	if v.slowStart() {
		factor = 2
	}
	rate := factor * v.cwnd / rtt.Seconds()
	if v.cfg.MaxRate > 0 {
		rate = math.Min(rate, v.cfg.MaxRate)
	}
	return rate
}

// OnAck processes the acknowledgement of one packet. now is when the ack
// arrived, sentAt when the packet left and rtt the round trip it measured.
// windowFull says whether the window was what held the sender back: a window
// that is not being used is not being tested, and must not grow (it may
// still shrink). The window when the packet left is not used.
func (v *Vegas) OnAck(now, sentAt time.Time, rtt time.Duration, _ float64, windowFull bool) {
	if rtt <= 0 {
		return
	}
	v.c.Acks++
	v.latest = rtt
	if v.srtt == 0 {
		v.srtt = rtt
	} else {
		v.srtt += (rtt - v.srtt) / 8
	}
	if v.baseRTT == 0 || rtt < v.baseRTT {
		v.baseRTT = rtt
	}
	if v.roundCnt == 0 || rtt < v.roundMin {
		v.roundMin = rtt
	}
	v.roundCnt++
	if windowFull {
		v.roundFull = true
	} else {
		v.c.AppLimitedAcks++
	}

	if v.inRecovery {
		if sentAt.Before(v.recoveryStart) {
			return
		}
		// Linux turns Vegas off for the recovery and starts a fresh round
		// after it.
		v.inRecovery = false
		v.newRound(now)
		return
	}
	if v.roundFrom.IsZero() {
		v.newRound(now)
	}
	if sentAt.Before(v.roundFrom) {
		// Between the once-per-round decisions only slow start moves.
		if v.slowStart() && windowFull && !v.ssHold {
			v.cwnd++
			v.clamp()
		}
		return
	}

	// A round trip has passed: decide.
	v.c.Rounds++
	grow := v.roundFull
	wasSlowStart, held := v.slowStart(), v.ssHold
	switch {
	case v.roundCnt <= 2:
		// Too few samples to trust the minimum: do what Reno would.
		v.c.RenoRounds++
		if grow {
			if v.slowStart() {
				if !held {
					v.cwnd++
				}
			} else {
				v.cwnd += 1 / v.cwnd
			}
		}
	default:
		rtt := v.roundMin
		v.diff = v.cwnd * float64(rtt-v.baseRTT) / float64(rtt)
		target := v.cwnd * float64(v.baseRTT) / float64(rtt) // what the path holds unqueued
		switch {
		case v.slowStart() && v.diff > v.cfg.Gamma:
			// The queue has begun to build: slow start is over. Fall back to
			// what fits, and let congestion avoidance take it from there.
			v.cwnd = math.Min(v.cwnd, target+1)
			v.ssthresh = math.Min(v.ssthresh, v.cwnd-1)
			v.c.SlowStartExits++
		case v.slowStart():
			if grow && !held {
				v.cwnd++
			}
		case v.diff > v.cfg.Beta:
			v.cwnd--
			v.ssthresh = math.Min(v.ssthresh, v.cwnd-1)
			v.c.Decreases++
		case v.diff < v.cfg.Alpha:
			if grow {
				v.cwnd++
				v.c.Increases++
			}
		default:
			v.c.Holds++
		}
	}
	v.clamp()
	// Linux's tcp_current_ssthresh: slow start does not resume just because
	// the window crept above an old threshold.
	if !math.IsInf(v.ssthresh, 1) {
		v.ssthresh = math.Max(v.ssthresh, 0.75*v.cwnd)
	}
	// Slow start alternates: a round of doubling, a round of holding still.
	v.ssHold = wasSlowStart && v.slowStart() && !held
	v.newRound(now)
}

func (v *Vegas) newRound(now time.Time) {
	v.roundFrom, v.roundCnt, v.roundMin, v.roundFull = now, 0, 0, false
}

// OnLoss reports one lost packet and when it was sent. The first loss of a
// window halves cwnd, as Reno does; further losses among packets sent before
// that are the same congestion event.
func (v *Vegas) OnLoss(now, sentAt time.Time) {
	v.c.Losses++
	if !v.recoveryStart.IsZero() && sentAt.Before(v.recoveryStart) {
		return
	}
	v.c.CongestionEvents++
	v.ssthresh = math.Max(v.cwnd/2, v.cfg.MinCwnd)
	v.cwnd = v.ssthresh
	v.recoveryStart, v.inRecovery = now, true
}

// OnTimeout is for the sender to call when nothing at all has been
// acknowledged for a retransmission timeout: slow start again, from the
// smallest window, up to half the old one.
func (v *Vegas) OnTimeout() {
	v.c.Timeouts++
	v.ssthresh = math.Max(v.cwnd/2, v.cfg.MinCwnd)
	v.cwnd = v.cfg.MinCwnd
	v.inRecovery, v.ssHold = false, false
	v.roundFrom = time.Time{}
	v.roundCnt = 0
}

func (v *Vegas) clamp() {
	v.cwnd = math.Max(math.Min(v.cwnd, v.cfg.MaxCwnd), v.cfg.MinCwnd)
}

// Stats is the controller's state, for logs and the page. Rates are in
// packets per second and times in milliseconds.
type Stats struct {
	Cwnd        float64  `json:"cwnd"`
	Ssthresh    *float64 `json:"ssthresh,omitempty"` // absent until slow start has ended
	SlowStart   bool     `json:"slowStart"`
	State       string   `json:"state"` // slow start, congestion avoidance or recovery
	Diff        float64  `json:"diff"`  // packets queued, as of the last round
	Alpha       float64  `json:"alpha"`
	Beta        float64  `json:"beta"`
	RTTMinMs    float64  `json:"rttMinMs"` // baseRTT
	SRTTMs      float64  `json:"srttMs"`
	LatestRTTMs float64  `json:"latestRttMs"`
	CurrentRate float64  `json:"currentRate"` // cwnd / srtt
	PacingRate  float64  `json:"pacingRate"`
	Counters    Counters `json:"counters"`
}

func (v *Vegas) Stats() Stats {
	s := Stats{
		Cwnd: v.cwnd, SlowStart: v.slowStart(), State: "congestion avoidance", Diff: v.diff,
		Alpha: v.cfg.Alpha, Beta: v.cfg.Beta,
		RTTMinMs: msf(v.baseRTT), SRTTMs: msf(v.srtt), LatestRTTMs: msf(v.latest),
		PacingRate: v.PacingRate(), Counters: v.c,
	}
	if v.srtt > 0 {
		s.CurrentRate = v.cwnd / v.srtt.Seconds()
	}
	switch {
	case v.inRecovery:
		s.State = "recovery"
	case s.SlowStart:
		s.State = "slow start"
	}
	if !math.IsInf(v.ssthresh, 1) {
		t := v.ssthresh
		s.Ssthresh = &t
	}
	return s
}

func msf(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
