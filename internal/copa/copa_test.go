package copa

import (
	"math"
	"testing"
	"time"
)

// link is a FIFO bottleneck: rate packets per second, a drop-tail buffer, and
// a fixed round-trip propagation delay. Because it is FIFO and deterministic,
// the time a packet's ack returns is known the moment the packet is sent.
type link struct {
	rate   float64
	prop   time.Duration
	buffer int       // packets
	free   time.Time // when the link finishes what it has accepted so far

	queueSum float64 // packets waiting, summed over arrivals
	arrivals int
}

// send returns when the ack comes back, or false if the buffer was full.
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
	cwnd          float64
	lost          bool
}

// flow is a sender with unlimited data: cwnd and a pacing budget, like the
// one in internal/session.
type flow struct {
	cc       *Copa
	inFlight []pending // in send order; acks return in the same order
	budget   float64   // packets
	startAt  time.Duration
	acked    int
	lost     int
	ackedWin int // acked since the measurement started
}

// greedy is a loss-based AIMD sender (Reno's congestion avoidance), which
// keeps the buffer full: what Copa's competitive mode is for.
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
	measure time.Duration // statistics start here
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
				f.ackedWin = 0
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
				f.inFlight = f.inFlight[1:]
				if p.lost {
					f.lost++
					f.cc.OnLoss(now, p.sentAt)
					continue
				}
				f.acked++
				f.ackedWin++
				f.cc.OnAck(now, p.sentAt, p.ackAt.Sub(p.sentAt), p.cwnd)
			}
			f.budget = math.Min(f.budget+step.Seconds()*f.cc.PacingRate(), 2)
			for f.budget >= 0 && float64(len(f.inFlight)+1) <= f.cc.Cwnd() {
				f.budget--
				p := pending{sentAt: now, cwnd: f.cc.Cwnd()}
				ackAt, ok := s.l.send(now)
				if !ok {
					// The sender learns of a loss about when the ack would have come.
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

// 12 Mbit/s of 1500-byte packets, the link of the paper's figure 2.
const rate12Mbps = 1000.0

func TestSingleFlowFillsTheLinkWithAShortQueue(t *testing.T) {
	// A zero Config must be default mode: see the note on updateMode for why
	// ModeAuto is not exercised here.
	for _, mode := range []Mode{"", ModeDefault} {
		l := &link{rate: rate12Mbps, prop: 20 * time.Millisecond, buffer: 1000}
		f := &flow{cc: New(Config{Mode: mode})}
		s := &sim{l: l, flows: []*flow{f}, measure: 5 * time.Second}
		var maxVelocity float64
		competitiveTicks := 0
		s.onTick = func(d time.Duration) {
			if d < s.measure {
				return
			}
			st := f.cc.Stats()
			maxVelocity = math.Max(maxVelocity, st.Velocity)
			if st.Mode == string(ModeCompetitive) {
				competitiveTicks++
			}
		}
		const dur = 30 * time.Second
		s.run(dur)

		util := float64(f.ackedWin) / (l.rate * (dur - s.measure).Seconds())
		st := f.cc.Stats()
		t.Logf("%s: utilisation %.3f, mean queue %.2f packets, cwnd %.1f, RTTmin %.1f ms, standing %.1f ms, max velocity %.0f, lost %d",
			mode, util, s.meanQueue(), st.Cwnd, st.RTTMinMs, st.RTTStandingMs, maxVelocity, f.lost)
		if util < 0.9 {
			t.Errorf("%s: utilisation %.3f, want at least 0.9", mode, util)
		}
		// §3: the queue oscillates between 0 and 2.5/δ packets, 1.25/δ = 2.5 on
		// average. Allow for the granularity of the simulation.
		if q := s.meanQueue(); q > 6 {
			t.Errorf("%s: mean queue %.2f packets, want about 2.5", mode, q)
		}
		if f.lost != 0 {
			t.Errorf("%s: %d packets lost with a 1000-packet buffer", mode, f.lost)
		}
		if st.SlowStart {
			t.Errorf("%s: still in slow start", mode)
		}
		if math.Abs(st.RTTMinMs-21) > 0.5 {
			t.Errorf("%s: RTTmin %.2f ms, want 21 (propagation plus one packet time)", mode, st.RTTMinMs)
		}
		if maxVelocity > 2 {
			t.Errorf("%s: velocity reached %.0f in steady state, want 1", mode, maxVelocity)
		}
		if competitiveTicks > 0 {
			t.Errorf("%s: competitive for %d ticks with no competing flow", mode, competitiveTicks)
		}
	}
}

func TestTwoFlowsShareTheLink(t *testing.T) {
	l := &link{rate: rate12Mbps, prop: 20 * time.Millisecond, buffer: 1000}
	a := &flow{cc: New(Config{Mode: ModeDefault})}
	b := &flow{cc: New(Config{Mode: ModeDefault}), startAt: 5 * time.Second}
	s := &sim{l: l, flows: []*flow{a, b}, measure: 15 * time.Second}
	const dur = 45 * time.Second
	s.run(dur)
	secs := (dur - s.measure).Seconds()
	ra, rb := float64(a.ackedWin)/secs, float64(b.ackedWin)/secs
	t.Logf("flow A %.0f pkt/s, flow B %.0f pkt/s, mean queue %.2f packets", ra, rb, s.meanQueue())
	if (ra+rb)/l.rate < 0.9 {
		t.Errorf("total %.0f pkt/s of %.0f", ra+rb, l.rate)
	}
	if ratio := math.Min(ra, rb) / math.Max(ra, rb); ratio < 0.7 {
		t.Errorf("unfair: %.0f vs %.0f pkt/s", ra, rb)
	}
	if q := s.meanQueue(); q > 10 {
		t.Errorf("mean queue %.2f packets, want about 2 x 2.5", q)
	}
}

// When the link gets faster, the velocity parameter has to find the new rate
// in a few RTTs rather than at one packet per RTT per RTT.
func TestVelocityFollowsARateIncrease(t *testing.T) {
	l := &link{rate: 500, prop: 20 * time.Millisecond, buffer: 1000}
	f := &flow{cc: New(Config{Mode: ModeDefault})}
	s := &sim{l: l, flows: []*flow{f}, measure: time.Hour}
	var at90 time.Duration
	var ackedAt time.Duration
	lastAcked := 0
	s.onTick = func(d time.Duration) {
		if d == 10*time.Second {
			l.rate = 2000
		}
		// Rate over 200 ms windows.
		if d >= 10*time.Second && d-ackedAt >= 200*time.Millisecond {
			if r := float64(f.acked-lastAcked) / (d - ackedAt).Seconds(); at90 == 0 && r > 0.9*2000 {
				at90 = d - 10*time.Second
			}
			ackedAt, lastAcked = d, f.acked
		}
	}
	s.run(20 * time.Second)
	t.Logf("reached 90%% of the new rate %v after the change", at90)
	// Without velocity: cwnd 10 -> 40 packets at 2 packets per RTT is 15 RTTs
	// of 20 ms plus; with it this takes about a second including the three
	// RTTs before doubling starts.
	if at90 == 0 || at90 > 2*time.Second {
		t.Errorf("took %v to reach 90%% of the new rate", at90)
	}
}

// Against a buffer-filling flow, default mode is starved (it backs off as the
// queue grows) and mode switching is what gets Copa a share (§2.2).
func TestCompetitiveModeHoldsItsOwnAgainstABufferFiller(t *testing.T) {
	share := func(mode Mode) (float64, Stats) {
		l := &link{rate: rate12Mbps, prop: 20 * time.Millisecond, buffer: 100}
		f := &flow{cc: New(Config{Mode: mode})}
		s := &sim{l: l, flows: []*flow{f}, greedy: &greedy{cwnd: 10}, measure: 20 * time.Second}
		s.run(80 * time.Second)
		return float64(f.ackedWin) / float64(f.ackedWin+s.greedy.ackedWin), f.cc.Stats()
	}
	def, _ := share(ModeDefault)
	auto, st := share(ModeAuto)
	t.Logf("Copa's share against an AIMD flow: default mode %.2f, auto %.2f (mode %s, δ %.3f, %d switches)",
		def, auto, st.Mode, st.Delta, st.Counters.ModeSwitches)
	if def > 0.25 {
		t.Errorf("default mode took %.2f of the link from a buffer-filling flow; expected it to yield", def)
	}
	if auto < 0.25 {
		t.Errorf("with mode switching Copa got only %.2f of the link", auto)
	}
	if st.Counters.ModeSwitches == 0 {
		t.Error("never switched to competitive mode")
	}
}

func TestMaxRateCapsTheWindow(t *testing.T) {
	l := &link{rate: rate12Mbps, prop: 20 * time.Millisecond, buffer: 1000}
	f := &flow{cc: New(Config{MaxRate: 200})}
	s := &sim{l: l, flows: []*flow{f}, measure: 5 * time.Second}
	const dur = 20 * time.Second
	s.run(dur)
	r := float64(f.ackedWin) / (dur - s.measure).Seconds()
	st := f.cc.Stats()
	t.Logf("rate %.1f pkt/s, cwnd %.1f", r, st.Cwnd)
	if math.Abs(r-200) > 4 {
		t.Errorf("rate %.1f pkt/s, want the 200 cap", r)
	}
	if st.Cwnd > 2*200*0.022 {
		t.Errorf("cwnd %.1f grew past what the cap can use", st.Cwnd)
	}
}

func TestTimeoutRestartsSlowStart(t *testing.T) {
	c := New(Config{})
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < 50; i++ {
		now = now.Add(time.Millisecond)
		c.OnAck(now, now.Add(-20*time.Millisecond), 20*time.Millisecond, c.Cwnd())
	}
	if c.Cwnd() != 60 {
		t.Fatalf("cwnd %.1f after 50 acks in slow start, want 60", c.Cwnd())
	}
	c.OnTimeout()
	if st := c.Stats(); st.Cwnd != 2 || !st.SlowStart || st.Counters.Timeouts != 1 {
		t.Fatalf("after a timeout: %+v", st)
	}
}

func TestExtremum(t *testing.T) {
	t0 := time.Unix(0, 0)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	lo, hi := extremum{}, extremum{max: true}
	in := []int{5, 3, 8, 7, 9, 4, 6, 6, 10}
	for i, v := range in {
		lo.update(at(i*10), time.Duration(v), 30*time.Millisecond)
		hi.update(at(i*10), time.Duration(v), 30*time.Millisecond)
		wantLo, wantHi := v, v
		for j := max(0, i-3); j <= i; j++ { // a 30 ms window holds four samples
			wantLo, wantHi = min(wantLo, in[j]), max(wantHi, in[j])
		}
		if int(lo.best()) != wantLo || int(hi.best()) != wantHi {
			t.Fatalf("after %v: min %d max %d, want %d and %d", in[:i+1], lo.best(), hi.best(), wantLo, wantHi)
		}
	}
}
