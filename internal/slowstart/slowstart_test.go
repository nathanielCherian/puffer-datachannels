package slowstart

import (
	"math"
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

func at(d time.Duration) time.Time { return t0.Add(d) }

const rtt = 20 * time.Millisecond

// The cycle, call by call: double until a loss, cut to half the window the
// lost packet left at and hold for a second whatever is lost meanwhile, then
// double again from the held window.
func TestCycle(t *testing.T) {
	s := New(Config{InitCwnd: 10})
	for i := 0; i < 30; i++ {
		s.OnAck(at(time.Duration(i)*time.Millisecond), t0, rtt, 0, true)
	}
	if s.Cwnd() != 40 {
		t.Fatalf("cwnd %.1f after 30 acks, want 40", s.Cwnd())
	}

	// The lost packet left at 19.5 ms, when the window was 30 (the ack at
	// 19 ms had made it so); the loss is noticed with the window at 40.
	s.OnLoss(at(100*time.Millisecond), at(19500*time.Microsecond))
	if st := s.Stats(); st.Cwnd != 15 || st.State != "waiting" || st.Peak == nil || *st.Peak != 30 || *st.Reached != 40 {
		t.Fatalf("after a loss: cwnd %.1f, state %q, peak %v; want 15, waiting, 30", st.Cwnd, st.State, st.Peak)
	}
	// While waiting neither acks nor losses move the window, and a loss does
	// not restart the wait.
	s.OnAck(at(500*time.Millisecond), at(480*time.Millisecond), rtt, 0, true)
	s.OnLoss(at(600*time.Millisecond), at(550*time.Millisecond))
	s.OnAck(at(1099*time.Millisecond), at(1079*time.Millisecond), rtt, 0, true)
	if st := s.Stats(); st.Cwnd != 15 || st.State != "waiting" {
		t.Fatalf("while waiting: cwnd %.1f, state %q; want 15, waiting", st.Cwnd, st.State)
	}

	// A second after the cut the first ack starts the next probe.
	s.OnAck(at(1100*time.Millisecond), at(1080*time.Millisecond), rtt, 0, true)
	if st := s.Stats(); st.Cwnd != 16 || st.State != "slow start" {
		t.Fatalf("after the wait: cwnd %.1f, state %q; want 16, slow start", st.Cwnd, st.State)
	}
	// A packet sent while waiting and lost now is not this probe's doing.
	s.OnLoss(at(1110*time.Millisecond), at(1090*time.Millisecond))
	if s.Cwnd() != 16 {
		t.Fatalf("cwnd %.1f after a loss from before the probe, want 16", s.Cwnd())
	}
	// One the probe sent is.
	s.OnLoss(at(1200*time.Millisecond), at(1150*time.Millisecond))
	if st := s.Stats(); st.Cwnd != 8 || st.State != "waiting" {
		t.Fatalf("after the probe's loss: cwnd %.1f, state %q; want 8, waiting", st.Cwnd, st.State)
	}
	c := s.Stats().Counters
	if c.CongestionEvents != 2 || c.Probes != 2 || c.Losses != 4 || c.IgnoredLosses != 2 {
		t.Errorf("counters %+v", c)
	}
}

func TestWindowNotFullDoesNotGrow(t *testing.T) {
	s := New(Config{})
	for i := 0; i < 100; i++ {
		s.OnAck(at(time.Duration(i)*time.Millisecond), t0, rtt, 0, false)
	}
	if st := s.Stats(); st.Cwnd != 10 || st.Counters.AppLimitedAcks != 100 {
		t.Errorf("cwnd %.1f, app-limited acks %d; want 10, 100", st.Cwnd, st.Counters.AppLimitedAcks)
	}
}

func TestTimeoutProbesAgainFromTheSmallestWindow(t *testing.T) {
	s := New(Config{InitCwnd: 40})
	s.OnLoss(at(100*time.Millisecond), at(50*time.Millisecond))
	s.OnTimeout()
	if st := s.Stats(); st.Cwnd != 2 || st.State != "slow start" || st.Counters.Timeouts != 1 {
		t.Fatalf("after a timeout: cwnd %.1f, state %q", st.Cwnd, st.State)
	}
	// What was in flight at the timeout is reported lost; that must not end
	// the new probe, before or after its first ack.
	s.OnLoss(at(1900*time.Millisecond), at(60*time.Millisecond))
	s.OnAck(at(2*time.Second), at(2*time.Second-rtt), rtt, 0, true)
	s.OnLoss(at(2010*time.Millisecond), at(70*time.Millisecond))
	if st := s.Stats(); st.Cwnd != 3 || st.State != "slow start" {
		t.Errorf("after the next ack: cwnd %.1f, state %q; want 3, slow start", st.Cwnd, st.State)
	}
}

// On a drop-tail link (1000 packets/s, 40 ms round trip: a BDP of 40, and
// room for 60 more) the cycle should come round about once a second. Every
// probe should find what the link holds (100) as the window its first lost
// packet left at, have gone well past it by the time it notices, and wait at
// half of it: above the BDP, so the link stays busy, and below what the link
// holds, so nothing is lost while waiting.
func TestRepeatsOnALink(t *testing.T) {
	const (
		rate   = 1000.0
		prop   = 40 * time.Millisecond
		buffer = 60
		step   = 100 * time.Microsecond
		dur    = 20 * time.Second
	)
	type pkt struct {
		ackAt, sentAt time.Time
		lost          bool
	}
	s := New(Config{})
	var inFlight []pkt
	var free time.Time
	var budget float64
	acked, lost := 0, 0
	peaks, reached := []float64{}, []float64{}
	events := int64(0)
	for d := time.Duration(0); d < dur; d += step {
		now := at(d)
		for len(inFlight) > 0 && !inFlight[0].ackAt.After(now) {
			p := inFlight[0]
			full := float64(len(inFlight)) >= s.Cwnd()-2
			inFlight = inFlight[1:]
			if p.lost {
				lost++
				s.OnLoss(now, p.sentAt)
				if c := s.Stats().Counters.CongestionEvents; c > events {
					events = c
					peaks, reached = append(peaks, *s.Stats().Peak), append(reached, *s.Stats().Reached)
				}
				continue
			}
			acked++
			s.OnAck(now, p.sentAt, p.ackAt.Sub(p.sentAt), 0, full)
		}
		budget = math.Min(budget+step.Seconds()*s.PacingRate(), 2)
		for budget >= 0 && float64(len(inFlight)+1) <= s.Cwnd() {
			budget--
			if free.Before(now) {
				free = now
			}
			p := pkt{sentAt: now}
			if free.Sub(now).Seconds()*rate > buffer {
				p.lost, p.ackAt = true, free.Add(prop)
			} else {
				free = free.Add(time.Duration(float64(time.Second) / rate))
				p.ackAt = free.Add(prop)
			}
			inFlight = append(inFlight, p)
		}
	}
	c := s.Stats().Counters
	util := float64(acked) / (rate * dur.Seconds())
	t.Logf("utilisation %.3f, %d lost of %d, %d cuts, %d probes, peaks %.0f, reached %.0f", util, lost, acked+lost, c.CongestionEvents, c.Probes, peaks, reached)
	if c.CongestionEvents < 12 || c.CongestionEvents > 20 {
		t.Errorf("%d cuts in 20 s, want about one a second", c.CongestionEvents)
	}
	for i, p := range peaks {
		if p < 95 || p > 115 {
			t.Errorf("probe %d: the lost packet left at a window of %.0f, want about what the link holds (%d)", i, p, 40+buffer)
		}
		if reached[i] < 1.3*p {
			t.Errorf("probe %d: window %.0f when the loss was noticed, want well past %.0f", i, reached[i], p)
		}
	}
	if util < 0.95 {
		t.Errorf("utilisation %.3f, want at least 0.95", util)
	}
	// Each probe loses about as many packets as the link holds: what the
	// window grew by between overflowing and the sender finding out. Nothing
	// else should be lost.
	if per := float64(lost) / float64(c.CongestionEvents); per > 110 {
		t.Errorf("%.0f packets lost per probe, want about %d", per, 40+buffer)
	}
}
