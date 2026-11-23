package vegas

import (
	"math"
	"testing"
	"time"
)

// The same FIFO drop-tail bottleneck as the other controllers' tests: because
// it is deterministic, a packet's fate is known when it is sent.
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
	cc       *Vegas
	inFlight []pending
	budget   float64
	startAt  time.Duration
	lost     int
	ackedWin int
}

// greedy is a Reno-like flow: one packet more per round trip, halved on loss,
// with no regard for delay.
type greedy struct {
	cwnd     float64
	inFlight []pending
	ackedWin int
	lastCut  time.Time
}

type sim struct {
	l       *link
	flows   []*flow
	greedy  *greedy
	measure time.Duration
	onTick  func(t time.Duration)
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
			if s.greedy != nil {
				s.greedy.ackedWin = 0
			}
		}
		for _, f := range s.flows {
			if t < f.startAt {
				continue
			}
			for len(f.inFlight) > 0 && !f.inFlight[0].ackAt.After(now) {
				p := f.inFlight[0]
				full := float64(len(f.inFlight)) >= f.cc.Cwnd()-2
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
		if g := s.greedy; g != nil {
			for len(g.inFlight) > 0 && !g.inFlight[0].ackAt.After(now) {
				p := g.inFlight[0]
				g.inFlight = g.inFlight[1:]
				if p.lost {
					if now.Sub(g.lastCut) > s.l.prop {
						g.cwnd = math.Max(g.cwnd/2, 2)
						g.lastCut = now
					}
					continue
				}
				g.ackedWin++
				g.cwnd += 1 / g.cwnd
			}
			for float64(len(g.inFlight)+1) <= g.cwnd {
				p := pending{sentAt: now}
				ackAt, ok := s.l.send(now)
				if !ok {
					p.lost, ackAt = true, s.l.free.Add(s.l.prop)
				}
				p.ackAt = ackAt
				g.inFlight = append(g.inFlight, p)
			}
		}
		if s.onTick != nil {
			s.onTick(t)
		}
	}
}

func (s *sim) meanQueue() float64 { return s.l.queueSum / float64(s.l.arrivals) }

// A lone flow should fill the link, lose nothing, and settle with between α
// and β of its packets queued: a window of the BDP (40) plus 2 to 4.
func TestSingleFlowKeepsAFewPacketsQueued(t *testing.T) {
	l := &link{rate: 1000, prop: 40 * time.Millisecond, buffer: 200}
	f := &flow{cc: New(Config{})}
	s := &sim{l: l, flows: []*flow{f}, measure: 10 * time.Second}
	minCwnd, maxCwnd := math.Inf(1), 0.0
	s.onTick = func(d time.Duration) {
		if d >= s.measure {
			minCwnd, maxCwnd = math.Min(minCwnd, f.cc.Cwnd()), math.Max(maxCwnd, f.cc.Cwnd())
		}
	}
	const dur = 40 * time.Second
	s.run(dur)
	util := float64(f.ackedWin) / (l.rate * (dur - s.measure).Seconds())
	st := f.cc.Stats()
	t.Logf("utilisation %.3f, mean queue %.2f packets, cwnd %.1f..%.1f, diff %.2f, baseRTT %.1f ms, rounds: +%d -%d =%d reno %d",
		util, s.meanQueue(), minCwnd, maxCwnd, st.Diff, st.RTTMinMs, st.Counters.Increases, st.Counters.Decreases, st.Counters.Holds, st.Counters.RenoRounds)
	if util < 0.97 {
		t.Errorf("utilisation %.3f, want at least 0.97", util)
	}
	if q := s.meanQueue(); q < 1 || q > 5 {
		t.Errorf("mean queue %.2f packets, want between alpha and beta (2..4), give or take one", q)
	}
	if minCwnd < 41 || maxCwnd > 47 {
		t.Errorf("cwnd %.1f..%.1f, want the BDP of 41 plus 2 to 4", minCwnd, maxCwnd)
	}
	if f.lost != 0 || st.Counters.CongestionEvents != 0 {
		t.Errorf("%d lost, %d congestion events", f.lost, st.Counters.CongestionEvents)
	}
	if st.Counters.SlowStartExits != 1 {
		t.Errorf("%d slow start exits, want 1", st.Counters.SlowStartExits)
	}
	if st.SlowStart {
		t.Errorf("still in slow start")
	}
}

// Slow start must end on delay, not on the buffer overflowing. The window
// goes 10, 20, 40, 80, 160 against a path that holds 161 (a BDP of 101 and a
// buffer of 60), so the last doubling just touches the top of the buffer and
// a handful of packets may go; the window must come straight back to the BDP
// without a loss-driven halving below it.
func TestSlowStartEndsOnDelay(t *testing.T) {
	l := &link{rate: 1000, prop: 100 * time.Millisecond, buffer: 60}
	f := &flow{cc: New(Config{})}
	s := &sim{l: l, flows: []*flow{f}}
	s.run(5 * time.Second)
	st := f.cc.Stats()
	t.Logf("after 5 s: cwnd %.1f, state %s, %d lost, %d slow start exits on delay", st.Cwnd, st.State, f.lost, st.Counters.SlowStartExits)
	if f.lost > 10 {
		t.Errorf("%d packets lost leaving slow start with a buffer of 60", f.lost)
	}
	if st.SlowStart {
		t.Errorf("still in slow start after 5 s")
	}
	if st.Cwnd < 85 || st.Cwnd > 110 {
		t.Errorf("cwnd %.1f, want near the BDP of 101", st.Cwnd)
	}
}

// Two flows that start together see the same baseRTT and share the link.
func TestTwoFlowsShareTheLink(t *testing.T) {
	l := &link{rate: 1000, prop: 40 * time.Millisecond, buffer: 200}
	a, b := &flow{cc: New(Config{})}, &flow{cc: New(Config{}), startAt: 50 * time.Millisecond}
	s := &sim{l: l, flows: []*flow{a, b}, measure: 20 * time.Second}
	const dur = 60 * time.Second
	s.run(dur)
	secs := (dur - s.measure).Seconds()
	ra, rb := float64(a.ackedWin)/secs, float64(b.ackedWin)/secs
	t.Logf("%.0f and %.0f packets/s, mean queue %.2f packets", ra, rb, s.meanQueue())
	if ra+rb < 0.97*l.rate {
		t.Errorf("together %.0f packets/s, want at least 970", ra+rb)
	}
	if lo, hi := math.Min(ra, rb), math.Max(ra, rb); lo < 0.5*hi {
		t.Errorf("shares %.0f and %.0f packets/s are too unequal", ra, rb)
	}
	if q := s.meanQueue(); q > 10 {
		t.Errorf("mean queue %.2f packets, want no more than beta for each flow", q)
	}
}

// What Vegas is known for: against a flow that fills the buffer it backs
// off, because all it sees is delay. This is the behaviour, not a defect of
// the implementation; the test pins it so that a change which makes Vegas
// hold its own here is noticed as a change of algorithm.
func TestYieldsToABufferFiller(t *testing.T) {
	l := &link{rate: 1000, prop: 40 * time.Millisecond, buffer: 100}
	f := &flow{cc: New(Config{})}
	g := &greedy{cwnd: 2}
	s := &sim{l: l, flows: []*flow{f}, greedy: g, measure: 20 * time.Second}
	const dur = 80 * time.Second
	s.run(dur)
	secs := (dur - s.measure).Seconds()
	rv, rg := float64(f.ackedWin)/secs, float64(g.ackedWin)/secs
	t.Logf("Vegas %.0f packets/s, the buffer filler %.0f", rv, rg)
	if rv+rg < 0.9*l.rate {
		t.Errorf("together %.0f packets/s, want at least 900", rv+rg)
	}
	if rv > 0.35*l.rate {
		t.Errorf("Vegas took %.0f packets/s from a buffer filler; it should yield", rv)
	}
}

// A window that is not full must not grow.
func TestNothingGrowsWhileTheWindowIsNotFull(t *testing.T) {
	l := &link{rate: 1000, prop: 40 * time.Millisecond, buffer: 200}
	f := &flow{cc: New(Config{MaxRate: 300})}
	s := &sim{l: l, flows: []*flow{f}, measure: 5 * time.Second}
	const dur = 30 * time.Second
	s.run(dur)
	rate := float64(f.ackedWin) / (dur - s.measure).Seconds()
	st := f.cc.Stats()
	t.Logf("%.0f packets/s, cwnd %.1f, %d of %d acks with the window not full", rate, st.Cwnd, st.Counters.AppLimitedAcks, st.Counters.Acks)
	if rate < 290 || rate > 305 {
		t.Errorf("%.0f packets/s, want the cap of 300", rate)
	}
	// 300 packets/s for 41 ms is 12 in flight.
	if st.Cwnd > 30 {
		t.Errorf("cwnd %.1f ran away under the cap", st.Cwnd)
	}
}

// A loss halves the window once per window of losses; a timeout restarts
// slow start.
func TestLossAndTimeout(t *testing.T) {
	v := New(Config{InitCwnd: 40})
	t0 := time.Unix(1_700_000_000, 0)
	v.OnLoss(t0.Add(100*time.Millisecond), t0)
	v.OnLoss(t0.Add(101*time.Millisecond), t0.Add(time.Millisecond)) // the same window
	if v.Cwnd() != 20 {
		t.Fatalf("cwnd %.1f after two losses in one window, want 20", v.Cwnd())
	}
	v.OnLoss(t0.Add(300*time.Millisecond), t0.Add(200*time.Millisecond)) // sent after the cut
	if v.Cwnd() != 10 {
		t.Fatalf("cwnd %.1f after a loss in the next window, want 10", v.Cwnd())
	}
	v.OnTimeout()
	st := v.Stats()
	if st.Cwnd != 2 || !st.SlowStart || st.Ssthresh == nil || *st.Ssthresh != 5 {
		t.Errorf("after a timeout: cwnd %.1f, slow start %v, ssthresh %v; want 2, true, 5", st.Cwnd, st.SlowStart, st.Ssthresh)
	}
	if st.Counters.CongestionEvents != 2 || st.Counters.Losses != 3 || st.Counters.Timeouts != 1 {
		t.Errorf("counters %+v", st.Counters)
	}
}
