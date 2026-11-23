package bbr

import (
	"math"
	"math/rand"
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
	cc       *BBR
	inFlight []pending
	budget   float64
	startAt  time.Duration
	sentWin  int
	lostWin  int
	ackedWin int
}

type sim struct {
	l       *link
	flows   []*flow
	measure time.Duration
	// clump is how often acks reach the sender (0 = as they are due). Those
	// that came due in between arrive together, at one instant, and the
	// sender may send between any two of them, as the real one does: its
	// send loop is woken by every ack.
	clump  time.Duration
	onTick func(t time.Duration, now time.Time)
}

func (s *sim) run(d time.Duration) {
	const step = 100 * time.Microsecond
	start := time.Unix(1_700_000_000, 0)
	for t := time.Duration(0); t < d; t += step {
		now := start.Add(t)
		if t == s.measure {
			s.l.queueSum, s.l.arrivals = 0, 0
			for _, f := range s.flows {
				f.ackedWin, f.lostWin, f.sentWin = 0, 0, 0
			}
		}
		for _, f := range s.flows {
			if t < f.startAt {
				continue
			}
			send := func() {
				for f.budget >= 0 && float64(len(f.inFlight)+1) <= f.cc.Cwnd() {
					f.budget--
					p := pending{sentAt: now}
					ackAt, ok := s.l.send(now)
					if !ok {
						// The sender learns of a loss about when the ack would have come.
						p.lost, ackAt = true, s.l.free.Add(s.l.prop)
					}
					p.ackAt = ackAt
					f.inFlight = append(f.inFlight, p)
					f.sentWin++
					f.cc.OnSend(now)
				}
			}
			f.budget = math.Min(f.budget+step.Seconds()*f.cc.PacingRate(), 2)
			// Acks are in the order the packets left, losses included, so the
			// head of the queue is always the next to be heard of.
			for (s.clump == 0 || t%s.clump == 0) && len(f.inFlight) > 0 && !f.inFlight[0].ackAt.After(now) {
				p := f.inFlight[0]
				full := float64(len(f.inFlight)) >= f.cc.Cwnd()-2
				f.inFlight = f.inFlight[1:]
				if p.lost {
					f.lostWin++
					f.cc.OnLoss(now, p.sentAt)
					continue
				}
				f.ackedWin++
				f.cc.OnAck(now, p.sentAt, p.ackAt.Sub(p.sentAt), 0, full)
				if s.clump > 0 {
					send()
				}
			}
			send()
		}
		if s.onTick != nil {
			s.onTick(t, now)
		}
	}
}

func (s *sim) meanQueue() float64 { return s.l.queueSum / float64(s.l.arrivals) }

func newBBR(seed int64, cfg Config) *BBR {
	cfg.Rand = rand.New(rand.NewSource(seed)).Float64
	return New(cfg)
}

// 1000 packets per second with 40 ms of round trip: a BDP of 40 packets.
// With a buffer several times that, BBR should fill the link, never lose a
// packet, and keep the queue to a fraction of a BDP outside its probes.
func TestSingleFlowFillsTheLinkWithAShortQueue(t *testing.T) {
	l := &link{rate: 1000, prop: 40 * time.Millisecond, buffer: 400}
	f := &flow{cc: newBBR(1, Config{})}
	s := &sim{l: l, flows: []*flow{f}, measure: 5 * time.Second}
	states := map[State]int{}
	var startupEnd time.Duration
	s.onTick = func(d time.Duration, _ time.Time) {
		st := f.cc.state
		states[st]++
		if startupEnd == 0 && st != Startup {
			startupEnd = d
		}
	}
	const dur = 45 * time.Second
	s.run(dur)
	util := float64(f.ackedWin) / (l.rate * (dur - s.measure).Seconds())
	st := f.cc.Stats()
	t.Logf("utilisation %.3f, mean queue %.1f packets, left Startup at %v, bw %.0f, min_rtt %.1f ms, cwnd %.0f, %d probes, %d ProbeRTTs, states %v",
		util, s.meanQueue(), startupEnd, st.Bw, st.RTTMinMs, st.Cwnd, st.Counters.BwProbes, st.Counters.ProbeRTTs, states)
	if util < 0.93 {
		t.Errorf("utilisation %.3f, want at least 0.93", util)
	}
	if q := s.meanQueue(); q > 20 {
		t.Errorf("mean queue %.1f packets, want well under the BDP of 40", q)
	}
	if startupEnd == 0 || startupEnd > 2*time.Second {
		t.Errorf("left Startup at %v, want within 2 s", startupEnd)
	}
	if math.Abs(st.MaxBw-1000) > 60 {
		t.Errorf("max_bw %.0f packets/s, want about 1000", st.MaxBw)
	}
	if st.RTTMinMs < 40 || st.RTTMinMs > 43 {
		t.Errorf("min_rtt %.1f ms, want about 41", st.RTTMinMs)
	}
	if f.lostWin != 0 || st.Counters.Timeouts != 0 {
		t.Errorf("%d lost, %d timeouts with a deep buffer", f.lostWin, st.Counters.Timeouts)
	}
	// 40 s is long enough for every state to have come round.
	for _, want := range []State{ProbeBWDown, ProbeBWCruise, ProbeBWRefill, ProbeBWUp, ProbeRTT} {
		if states[want] == 0 {
			t.Errorf("never in %s", want)
		}
	}
	if st.Counters.ProbeRTTs < 2 || st.Counters.ProbeRTTs > 5 {
		t.Errorf("%d ProbeRTTs in 45 s, want one every 10 s or so", st.Counters.ProbeRTTs)
	}
}

// With a buffer of a fifth of a BDP, probing overflows it. Version 3 must
// learn the ceiling (inflight_longterm) from that and keep the loss it causes
// to about what it tolerates, where version 1 would lose a quarter of
// everything it sent in a probe.
func TestShallowBufferBoundsLoss(t *testing.T) {
	l := &link{rate: 1000, prop: 100 * time.Millisecond, buffer: 20}
	f := &flow{cc: newBBR(2, Config{})}
	s := &sim{l: l, flows: []*flow{f}, measure: 10 * time.Second}
	const dur = 60 * time.Second
	s.run(dur)
	util := float64(f.ackedWin) / (l.rate * (dur - s.measure).Seconds())
	loss := float64(f.lostWin) / float64(f.sentWin)
	st := f.cc.Stats()
	t.Logf("utilisation %.3f, loss %.2f%%, inflight_longterm %v, %d too-high events, %d loss rounds, %d probes",
		util, 100*loss, deref(st.InflightLongterm), st.Counters.InflightTooHigh, st.Counters.LossRounds, st.Counters.BwProbes)
	if util < 0.85 {
		t.Errorf("utilisation %.3f, want at least 0.85", util)
	}
	if loss > 0.03 {
		t.Errorf("loss %.2f%%, want no more than 3%%", 100*loss)
	}
	if st.InflightLongterm == nil {
		t.Fatalf("loss never set inflight_longterm")
	}
	// The path holds a BDP of 100, the buffer's 20 and the one in service.
	if v := *st.InflightLongterm; v < 70 || v > 135 {
		t.Errorf("inflight_longterm %.0f, want about what the path holds (121)", v)
	}
}

// Two flows, the second starting ten seconds late, should end up with
// comparable shares.
func TestTwoFlowsShareTheLink(t *testing.T) {
	l := &link{rate: 1000, prop: 40 * time.Millisecond, buffer: 200}
	a, b := &flow{cc: newBBR(3, Config{})}, &flow{cc: newBBR(4, Config{}), startAt: 10 * time.Second}
	s := &sim{l: l, flows: []*flow{a, b}, measure: 40 * time.Second}
	const dur = 100 * time.Second
	s.run(dur)
	secs := (dur - s.measure).Seconds()
	ra, rb := float64(a.ackedWin)/secs, float64(b.ackedWin)/secs
	t.Logf("%.0f and %.0f packets/s, mean queue %.1f packets", ra, rb, s.meanQueue())
	if ra+rb < 0.9*l.rate {
		t.Errorf("together %.0f packets/s, want at least 900", ra+rb)
	}
	if lo, hi := math.Min(ra, rb), math.Max(ra, rb); lo < 0.3*hi {
		t.Errorf("shares %.0f and %.0f packets/s are too unequal", ra, rb)
	}
}

// When the link's rate doubles, the next probe must find it.
func TestFindsARateIncrease(t *testing.T) {
	l := &link{rate: 500, prop: 40 * time.Millisecond, buffer: 400}
	f := &flow{cc: newBBR(5, Config{})}
	s := &sim{l: l, flows: []*flow{f}, measure: 20 * time.Second}
	s.onTick = func(d time.Duration, _ time.Time) {
		if d == 10*time.Second {
			l.rate = 1000
		}
	}
	const dur = 30 * time.Second
	s.run(dur)
	util := float64(f.ackedWin) / (1000 * (dur - s.measure).Seconds())
	t.Logf("utilisation of the doubled link %.3f, bw %.0f", util, f.cc.Stats().Bw)
	if util < 0.9 {
		t.Errorf("utilisation %.3f ten seconds after the rate doubled, want at least 0.9", util)
	}
}

// Acks that reach the sender in clumps, a millisecond's worth at one instant,
// with the sender sending between them. A packet sent in the middle of a
// clump must not take "delivered so far" and "as of now" from there: the rest
// of the clump is about to be counted as delivered after it left, in no time
// at all. On a 6 ms path that made every sample up to a sixth too high.
func TestClumpedAcksDoNotOverestimate(t *testing.T) {
	l := &link{rate: 5000, prop: 6 * time.Millisecond, buffer: 400}
	f := &flow{cc: newBBR(10, Config{})}
	s := &sim{l: l, flows: []*flow{f}, measure: 3 * time.Second, clump: time.Millisecond}
	worst := 0.0
	s.onTick = func(d time.Duration, _ time.Time) {
		if d >= s.measure {
			worst = math.Max(worst, f.cc.maxBw)
		}
	}
	const dur = 20 * time.Second
	s.run(dur)
	util := float64(f.ackedWin) / (l.rate * (dur - s.measure).Seconds())
	t.Logf("utilisation %.3f, largest max_bw %.0f packets/s, mean queue %.1f packets", util, worst, s.meanQueue())
	if worst > 1.03*l.rate {
		t.Errorf("max_bw reached %.0f packets/s on a link of %.0f", worst, l.rate)
	}
	if util < 0.9 {
		t.Errorf("utilisation %.3f, want at least 0.9", util)
	}
}

// Under a rate cap below the link's rate the flow sends at the cap, its
// window settles at a small multiple of what the cap keeps in flight, and
// nothing queues.
func TestMaxRateCapsTheFlow(t *testing.T) {
	l := &link{rate: 1000, prop: 40 * time.Millisecond, buffer: 400}
	f := &flow{cc: newBBR(6, Config{MaxRate: 300})}
	s := &sim{l: l, flows: []*flow{f}, measure: 5 * time.Second}
	const dur = 30 * time.Second
	s.run(dur)
	rate := float64(f.ackedWin) / (dur - s.measure).Seconds()
	st := f.cc.Stats()
	t.Logf("%.0f packets/s, cwnd %.0f, bw %.0f, state %s, mean queue %.2f", rate, st.Cwnd, st.Bw, st.State, s.meanQueue())
	if rate < 280 || rate > 305 {
		t.Errorf("%.0f packets/s, want the cap of 300", rate)
	}
	// 300 packets/s for 41 ms is 12 in flight.
	if st.Cwnd > 45 {
		t.Errorf("cwnd %.0f ran away under the cap", st.Cwnd)
	}
	if st.MaxBw > 330 {
		t.Errorf("max_bw %.0f, want no more than the cap allows to be measured", st.MaxBw)
	}
	if q := s.meanQueue(); q > 1 {
		t.Errorf("mean queue %.2f packets under the cap", q)
	}
}

// Every packet sent is retired by its ack or its loss, so the log of sends
// does not grow, and the controller's count of packets in flight is the
// sender's.
func TestSendLogIsBounded(t *testing.T) {
	l := &link{rate: 1000, prop: 100 * time.Millisecond, buffer: 20}
	f := &flow{cc: newBBR(7, Config{})}
	s := &sim{l: l, flows: []*flow{f}}
	s.onTick = func(d time.Duration, _ time.Time) {
		if f.cc.inflight != len(f.inFlight) {
			t.Fatalf("at %v the controller counts %d in flight, the sender %d", d, f.cc.inflight, len(f.inFlight))
		}
	}
	s.run(30 * time.Second)
	if n := len(f.cc.sends) - f.cc.head; n != len(f.inFlight) {
		t.Errorf("%d send records for %d packets in flight", n, len(f.inFlight))
	}
	if len(f.cc.sends) > 4096 {
		t.Errorf("the send log holds %d records", len(f.cc.sends))
	}
}

// A timeout collapses the window to what is in flight plus one, and the
// first ack of a packet sent afterwards brings it back to what the model
// allows after a round with loss: no less than 0.7 of what it was.
func TestTimeout(t *testing.T) {
	b := newBBR(8, Config{})
	t0 := time.Unix(1_700_000_000, 0)
	at := func(i int) time.Time { return t0.Add(time.Duration(i) * time.Millisecond) }
	// A packet every millisecond, each acknowledged 20 ms later.
	const n = 3000
	for i := 0; i < n; i++ {
		if i >= 20 {
			b.OnAck(at(i), at(i-20), 20*time.Millisecond, 0, false)
		}
		b.OnSend(at(i))
	}
	st := b.Stats()
	before := st.Cwnd
	if st.State == Startup || before < 30 || st.InFlight != 20 {
		t.Fatalf("before the timeout: state %s, cwnd %.0f, %d in flight", st.State, before, st.InFlight)
	}
	// Nothing more is heard: the 20 in flight time out.
	late := at(n + 1000)
	for i := n - 20; i < n; i++ {
		b.OnLoss(late, at(i))
	}
	b.OnTimeout()
	if b.Cwnd() != 1 {
		t.Errorf("cwnd %.0f after a timeout with nothing in flight, want 1", b.Cwnd())
	}
	b.OnSend(late)
	b.OnAck(late.Add(20*time.Millisecond), late, 20*time.Millisecond, 0, true)
	if got := b.Cwnd(); got < 0.65*before {
		t.Errorf("cwnd %.0f after recovery, want at least 0.7 of the %.0f it had", got, before)
	}
	if st := b.Stats(); st.Counters.Timeouts != 1 || st.InFlight != 0 || st.InRecovery {
		t.Errorf("timeouts %d, in flight %d, in recovery %v", st.Counters.Timeouts, st.InFlight, st.InRecovery)
	}
}

func deref(p *float64) float64 {
	if p == nil {
		return math.Inf(1)
	}
	return *p
}
