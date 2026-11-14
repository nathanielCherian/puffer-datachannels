package session

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"puffer-datachannels/internal/wire"
)

// stubCC is a controller that only records what the sender tells it.
type stubCC struct {
	cwnd   float64
	srtt   time.Duration
	acks   int
	losses int
	full   []bool // windowFull of each ack
}

func (c *stubCC) OnAck(_, _ time.Time, _ time.Duration, _ float64, windowFull bool) {
	c.acks++
	c.full = append(c.full, windowFull)
}
func (c *stubCC) OnLoss(_, _ time.Time) { c.losses++ }
func (c *stubCC) OnTimeout()            {}
func (c *stubCC) Cwnd() float64         { return c.cwnd }
func (c *stubCC) PacingRate() float64   { return 1e6 }
func (c *stubCC) SRTT() time.Duration   { return c.srtt }

// ackMsg is the page's FEEDBACK for these arrivals, sent the moment the last
// one arrived (so the sender subtracts no hold time).
func ackMsg(recvMs float64, seqs ...uint32) []byte {
	b := make([]byte, 15+12*len(seqs))
	b[0] = wire.TypeFeedback
	binary.BigEndian.PutUint64(b[5:], math.Float64bits(recvMs))
	binary.BigEndian.PutUint16(b[13:], uint16(len(seqs)))
	for i, seq := range seqs {
		binary.BigEndian.PutUint32(b[15+12*i:], seq)
		binary.BigEndian.PutUint64(b[19+12*i:], math.Float64bits(recvMs))
	}
	return b
}

// A packet that was overtaken by a few others is not lost; one that a packet
// sent well after it has overtaken is; and if that one then arrives after
// all, the sender allows more reordering from then on.
func TestJunkSenderToleratesReordering(t *testing.T) {
	cc := &stubCC{cwnd: 1000, srtt: 8 * time.Millisecond} // a reordering window of 2 ms
	s := newJunkSender(func([]byte) error { return nil }, cc, false, func(string, time.Time, any) {})
	t0 := time.Unix(1_700_000_000, 0)
	const gap = 500 * time.Microsecond // 2000 packets per second
	sentAt := func(seq uint32) time.Time { return t0.Add(time.Duration(seq) * gap) }
	send := func(from, to uint32) {
		s.mu.Lock()
		for seq := from; seq < to; seq++ {
			s.sendLocked(sentAt(seq))
		}
		s.mu.Unlock()
	}
	ack := func(seqs ...uint32) {
		for _, seq := range seqs {
			s.onFeedback(ackMsg(1, seq), sentAt(seq).Add(8*time.Millisecond))
		}
	}

	send(0, 10)
	ack(0, 1, 2, 3, 4)
	// 5 is overtaken by 6..9: four later packets acked, but the newest of them
	// left only 2 ms after it.
	ack(6, 7, 8, 9)
	if s.c.Lost != 0 || cc.losses != 0 {
		t.Fatalf("a packet overtaken by 2 ms was declared lost (%d)", s.c.Lost)
	}
	ack(5)
	if s.c.Acked != 10 || s.c.Lost != 0 || s.c.Spurious != 0 || len(s.sent) != 0 {
		t.Fatalf("after the late arrival: acked %d, lost %d, spurious %d, in flight %d", s.c.Acked, s.c.Lost, s.c.Spurious, len(s.sent))
	}

	// 10 goes missing while 11..40 arrive: 40 left 15 ms after it.
	send(10, 41)
	for seq := uint32(11); seq <= 40; seq++ {
		ack(seq)
	}
	if s.c.Lost != 1 || cc.losses != 1 || len(s.sent) != 0 {
		t.Fatalf("lost %d (controller told of %d), in flight %d; want one loss", s.c.Lost, cc.losses, len(s.sent))
	}
	// It turns up after all: spurious, and the window widens to 4 ms.
	ack(10)
	s.mu.Lock()
	wnd := s.reorderWindowLocked()
	s.mu.Unlock()
	if s.c.Spurious != 1 || wnd != 4*time.Millisecond || cc.losses != 1 {
		t.Fatalf("spurious %d, reordering window %v, losses %d; want 1, 4ms, 1", s.c.Spurious, wnd, cc.losses)
	}
	// A plain repeat of an ack is neither.
	ack(40)
	if s.c.Spurious != 1 || s.c.RepeatAcks != 1 {
		t.Fatalf("a repeated ack: spurious %d, repeats %d", s.c.Spurious, s.c.RepeatAcks)
	}
}

// The controller is told whether the window was what held the sender back.
func TestJunkSenderReportsWhetherTheWindowWasFull(t *testing.T) {
	cc := &stubCC{cwnd: 10, srtt: 8 * time.Millisecond}
	s := newJunkSender(func([]byte) error { return nil }, cc, false, func(string, time.Time, any) {})
	t0 := time.Unix(1_700_000_000, 0)
	s.mu.Lock()
	for i := 0; i < 10; i++ {
		s.sendLocked(t0)
	}
	s.mu.Unlock()
	for seq := uint32(0); seq < 10; seq++ {
		s.onFeedback(ackMsg(1, seq), t0.Add(8*time.Millisecond))
	}
	// 10, 9 and 8 in flight against a window of 10 count as full; below that
	// the sender had room it was not using.
	want := []bool{true, true, true, false, false, false, false, false, false, false}
	for i := range want {
		if cc.full[i] != want[i] {
			t.Fatalf("windowFull per ack = %v, want %v", cc.full, want)
		}
	}
}
