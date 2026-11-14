package cubic

import (
	"math"
	"testing"
	"time"
)

// The same FIFO drop-tail bottleneck as internal/copa's tests: because it is
// deterministic, a packet's fate is known when it is sent.
type link struct {
	rate   float64 // packets per second
	prop   time.Duration
	buffer int // packets
	free   time.Time

	queueSum float64
	arrivals int
}

func (l *link) send(now time.Time) (time.Time, bool) {
	if l.free.Before(now) {
		l.free = now
	}
	queued := l.free.Sub(now).Seconds() * l.rate
	l.queueSum += queued
	l.arrivals++
	if queued > float64(l.buffer) {
		return time.Time{}, false
	}
	l.free = l.free.Add(time.Duration(float64(time.Second) / l.rate))
	return l.free.Add(l.prop), true
}

type pending struct {
	ackAt, sentAt time.Time
	lost          bool
}

type flow struct {
	cc       *Cubic
	inFlight []pending
	budget   float64
	startAt  time.Duration
	lost     int
	ackedWin int
}

type sim struct {
	l       *link
	flows   []*flow
	measure time.Duration
	onTick  func(t time.Duration, now time.Time)
}

func (s *sim) run(d time.Duration) {
	const step = 100 * time.Microsecond
	start := time.Unix(1_700_000_000, 0)
	for t := time.Duration(0); t < d; t += step {
		now := start.Add(t)
		if t == s.measure {
			s.l.queueSum, s.l.arrivals = 0, 0
			for _, f := range s.flows {
				f.ackedWin, f.lost = 0, 0
			}
		}
		for _, f := range s.flows {
			if t < f.startAt {
				continue
			}
			for len(f.inFlight) > 0 && !f.inFlight[0].ackAt.After(now) {
				p := f.inFlight[0]
				full := windowFull(len(f.inFlight), f.cc.Cwnd())
				f.inFlight = f.inFlight[1:]
				if p.lost {
					f.lost++
					f.cc.OnLoss(now, p.sentAt)
					continue
				}
				f.ackedWin++
				f.cc.OnAck(now, p.sentAt, p.ackAt.Sub(p.sentAt), 0, full)
			}
			f.budget = math.Min(f.budget+step.Seconds()*f.cc.PacingRate(), 2)
			for f.budget >= 0 && float64(len(f.inFlight)+1) <= f.cc.Cwnd() {
				f.budget--
				p := pending{sentAt: now}
				ackAt, ok := s.l.send(now)
				if !ok {
					p.lost, ackAt = true, s.l.free.Add(s.l.prop)
				}
				p.ackAt = ackAt
				f.inFlight = append(f.inFlight, p)
			}
		}
		if s.onTick != nil {
			s.onTick(t, now)
		}
	}
}

// windowFull is the sender's rule in internal/session.
func windowFull(inFlight int, cwnd float64) bool { return float64(inFlight) >= cwnd-2 }

// 1000 packets per second with 100 ms of propagation delay is a BDP of 100
// packets: large enough for the cubic curve, not Reno's line, to set the
// window (see TestRenoFriendlyOnASmallPath for the other case).
func TestSingleFlowSawtooth(t *testing.T) {
	l := &link{rate: 1000, prop: 100 * time.Millisecond, buffer: 100}
	f := &flow{cc: New(Config{})}
	s := &sim{l: l, flows: []*flow{f}, measure: 20 * time.Second}
	minCwnd, maxCwnd := math.Inf(1), 0.0
	s.onTick = func(d time.Duration, _ time.Time) {
		if d >= s.measure {
			minCwnd, maxCwnd = math.Min(minCwnd, f.cc.Cwnd()), math.Max(maxCwnd, f.cc.Cwnd())
		}
	}
	const dur = 120 * time.Second
	s.run(dur)
	secs := (dur - s.measure).Seconds()
	util := float64(f.ackedWin) / (l.rate * secs)
	st := f.cc.Stats(time.Time{})
	t.Logf("utilisation %.3f, cwnd %.0f..%.0f, wMax %.0f, K %.2f s, mean queue %.0f packets, %d lost in %d events",
		util, minCwnd, maxCwnd, st.WMax, st.K, l.queueSum/float64(l.arrivals), f.lost, st.Counters.CongestionEvents)
	if util < 0.95 {
		t.Errorf("utilisation %.3f, want at least 0.95", util)
	}
	// The window can hold the BDP plus the buffer: 200 packets and the one in
	// service. Each cycle should climb to that and fall to 0.7 of it.
	if maxCwnd < 195 || maxCwnd > 215 {
		t.Errorf("largest cwnd %.0f, want about 201", maxCwnd)
	}
	if minCwnd < 0.65*maxCwnd || minCwnd > 0.75*maxCwnd {
		t.Errorf("smallest cwnd %.0f is not 0.7 of the largest (%.0f)", minCwnd, maxCwnd)
	}
	if st.Counters.CongestionEvents < 5 {
		t.Errorf("only %d congestion events", st.Counters.CongestionEvents)
	}
	// One reduction per loss episode, not one per lost packet.
	if f.lost > 0 && st.Counters.Losses < st.Counters.CongestionEvents {
		t.Errorf("more reductions (%d) than losses (%d)", st.Counters.CongestionEvents, st.Counters.Losses)
	}
	if st.Counters.Timeouts != 0 {
		t.Errorf("%d timeouts", st.Counters.Timeouts)
	}
}

// After a reduction from W_max the window must be back at W_max K seconds
// later, K = cbrt(W_max (1 - β) / C), having followed the cubic curve.
func TestWindowFollowsTheCubicCurve(t *testing.T) {
	c := New(Config{})
	now := time.Unix(1_700_000_000, 0)
	const rtt = 100 * time.Millisecond
	// Slow start to 400 packets, then one loss.
	for c.Cwnd() < 400 {
		c.OnAck(now, now.Add(-rtt), rtt, 0, true)
	}
	c.OnLoss(now, now.Add(-rtt))
	if got := c.Cwnd(); math.Abs(got-280) > 1e-9 {
		t.Fatalf("cwnd %.2f after a loss at 400, want 280", got)
	}
	wantK := math.Cbrt(400 * 0.3 / 0.4)
	lossAt := now
	var epoch time.Time
	var atK, at2K float64
	// One ack per packet of window per RTT, in steps of 1 ms.
	var credit float64
	for now.Sub(lossAt) < time.Duration(2.2*wantK*float64(time.Second)) {
		now = now.Add(time.Millisecond)
		credit += c.Cwnd() / 100
		for ; credit >= 1; credit-- {
			c.OnAck(now, now.Add(-rtt), rtt, 0, true)
			if epoch.IsZero() && !c.epochStart.IsZero() {
				epoch = c.epochStart
			}
		}
		if epoch.IsZero() {
			continue
		}
		if e := now.Sub(epoch).Seconds(); atK == 0 && e >= wantK {
			atK = c.Cwnd()
		} else if at2K == 0 && e >= 2*wantK {
			at2K = c.Cwnd()
		}
	}
	st := c.Stats(now)
	t.Logf("K %.3f s (want %.3f), cwnd %.1f at K and %.1f at 2K, %d Reno-friendly acks of %d",
		st.K, wantK, atK, at2K, st.Counters.RenoFriendlyAcks, st.Counters.Acks)
	if math.Abs(st.K-wantK) > 1e-6 {
		t.Errorf("K = %.4f, want %.4f", st.K, wantK)
	}
	// The window chases the curve one RTT ahead, so it is a little past it.
	if atK < 398 || atK > 408 {
		t.Errorf("cwnd %.1f at t = K, want about W_max = 400", atK)
	}
	// At 2K the curve is as far above W_max as it started below: 400 + 120.
	if at2K < 515 || at2K > 545 {
		t.Errorf("cwnd %.1f at t = 2K, want about 520", at2K)
	}
}

func TestTwoFlowsConverge(t *testing.T) {
	l := &link{rate: 1000, prop: 100 * time.Millisecond, buffer: 100}
	a := &flow{cc: New(Config{})}
	b := &flow{cc: New(Config{}), startAt: 20 * time.Second}
	s := &sim{l: l, flows: []*flow{a, b}, measure: 60 * time.Second}
	const dur = 240 * time.Second
	s.run(dur)
	secs := (dur - s.measure).Seconds()
	ra, rb := float64(a.ackedWin)/secs, float64(b.ackedWin)/secs
	t.Logf("flow A %.0f pkt/s, flow B %.0f pkt/s, fast convergences %d and %d",
		ra, rb, a.cc.Stats(time.Time{}).Counters.FastConvergences, b.cc.Stats(time.Time{}).Counters.FastConvergences)
	if (ra+rb)/l.rate < 0.95 {
		t.Errorf("total %.0f pkt/s of %.0f", ra+rb, l.rate)
	}
	if ratio := math.Min(ra, rb) / math.Max(ra, rb); ratio < 0.6 {
		t.Errorf("unfair: %.0f vs %.0f pkt/s", ra, rb)
	}
}

// On a short, slow path the cubic curve is slower than Reno, and the window
// must follow the Reno estimate instead (§4.3).
func TestRenoFriendlyOnASmallPath(t *testing.T) {
	l := &link{rate: 500, prop: 20 * time.Millisecond, buffer: 10}
	f := &flow{cc: New(Config{})}
	s := &sim{l: l, flows: []*flow{f}, measure: 10 * time.Second}
	const dur = 60 * time.Second
	s.run(dur)
	st := f.cc.Stats(time.Time{})
	util := float64(f.ackedWin) / (l.rate * (dur - s.measure).Seconds())
	share := float64(st.Counters.RenoFriendlyAcks) / float64(st.Counters.Acks)
	t.Logf("utilisation %.3f, %.0f%% of acks in the Reno-friendly region, %d congestion events", util, 100*share, st.Counters.CongestionEvents)
	if share < 0.5 {
		t.Errorf("only %.0f%% of acks were Reno-friendly on a 10-packet BDP", 100*share)
	}
	if util < 0.9 {
		t.Errorf("utilisation %.3f, want at least 0.9", util)
	}
}

func TestMaxRateCapsTheWindow(t *testing.T) {
	l := &link{rate: 1000, prop: 20 * time.Millisecond, buffer: 100}
	f := &flow{cc: New(Config{MaxRate: 200})}
	s := &sim{l: l, flows: []*flow{f}, measure: 5 * time.Second}
	const dur = 20 * time.Second
	s.run(dur)
	r := float64(f.ackedWin) / (dur - s.measure).Seconds()
	st := f.cc.Stats(time.Time{})
	t.Logf("rate %.1f pkt/s, cwnd %.1f", r, st.Cwnd)
	if math.Abs(r-200) > 4 {
		t.Errorf("rate %.1f pkt/s, want the 200 cap", r)
	}
	// 200 packets per second over 21 ms is 4.2 in flight: the initial window
	// of 10 is never full, so it must still be 10.
	if st.Cwnd != 10 || f.lost != 0 {
		t.Errorf("cwnd %.1f grew past what the cap can use (%d lost)", st.Cwnd, f.lost)
	}
}

// While the window is not what limits the sender, nothing may move: not the
// window, not the Reno estimate, not the epoch's clock. This is the deployed
// case of a path faster than the rate cap.
func TestNothingGrowsWhileTheWindowIsNotFull(t *testing.T) {
	c := New(Config{})
	now := time.Unix(1_700_000_000, 0)
	const rtt = 10 * time.Millisecond
	for c.Cwnd() < 100 {
		c.OnAck(now, now.Add(-rtt), rtt, 0, true)
	}
	c.OnLoss(now, now.Add(-rtt))
	step := func(n int, full bool) {
		for i := 0; i < n; i++ {
			now = now.Add(time.Millisecond)
			c.OnAck(now, now.Add(-rtt), rtt, 0, full)
		}
	}
	step(1000, true) // one second into the epoch
	before := c.Stats(now)
	step(60_000, false) // a minute of not filling the window
	after := c.Stats(now)
	t.Logf("before: cwnd %.2f, wEst %.2f, epoch %.3f s; after a minute app-limited: cwnd %.2f, wEst %.2f, epoch %.3f s",
		before.Cwnd, before.WEst, before.EpochSec, after.Cwnd, after.WEst, after.EpochSec)
	if after.Cwnd != before.Cwnd || after.WEst != before.WEst {
		t.Errorf("cwnd %.2f -> %.2f, wEst %.2f -> %.2f while the window was not full", before.Cwnd, after.Cwnd, before.WEst, after.WEst)
	}
	if math.Abs(after.EpochSec-before.EpochSec) > 1e-6 {
		t.Errorf("the epoch's clock ran from %.3f s to %.3f s while the window was not full", before.EpochSec, after.EpochSec)
	}
	if after.Counters.AppLimitedAcks != 60_000 {
		t.Errorf("%d app-limited acks counted, want 60000", after.Counters.AppLimitedAcks)
	}
	// And it picks up where it left off.
	step(10, true)
	if got := c.Stats(now); got.Cwnd <= after.Cwnd || got.Cwnd > after.Cwnd+1 {
		t.Errorf("cwnd %.2f after ten more acks with a full window, want just above %.2f", got.Cwnd, after.Cwnd)
	}
}

func TestTimeout(t *testing.T) {
	c := New(Config{})
	now := time.Unix(1_700_000_000, 0)
	const rtt = 50 * time.Millisecond
	for c.Cwnd() < 100 {
		c.OnAck(now, now.Add(-rtt), rtt, 0, true)
	}
	c.OnTimeout()
	st := c.Stats(now)
	if st.Cwnd != 2 || !st.SlowStart || st.Ssthresh == nil || math.Abs(*st.Ssthresh-70) > 1e-9 {
		t.Fatalf("after a timeout at 100: cwnd %.1f, slow start %v, ssthresh %v", st.Cwnd, st.SlowStart, st.Ssthresh)
	}
	// Slow start back up to ssthresh, then an epoch that starts at its
	// inflection point: K = 0, W_max = the window there (§4.8).
	for c.slowStart() {
		now = now.Add(time.Millisecond)
		c.OnAck(now, now.Add(-rtt), rtt, 0, true)
	}
	now = now.Add(time.Millisecond)
	c.OnAck(now, now.Add(-rtt), rtt, 0, true)
	if st := c.Stats(now); st.K != 0 || math.Abs(st.WMax-70) > 1e-9 || st.State != "congestion avoidance" {
		t.Fatalf("first epoch after a timeout: K %.3f, wMax %.2f, state %s", st.K, st.WMax, st.State)
	}
}
