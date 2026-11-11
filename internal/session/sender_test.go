package session

import (
	"context"
	"encoding/binary"
	"math"
	"sync"
	"testing"
	"time"

	"puffer-datachannels/internal/ratecontrol"
	"puffer-datachannels/internal/wire"
)

type recorder struct {
	ratecontrol.Fixed
	fbs []ratecontrol.Feedback
}

func (r *recorder) OnFeedback(fb ratecontrol.Feedback) { r.fbs = append(r.fbs, fb) }
func (r *recorder) TargetBitrate() int                 { return 1_000_000 }
func (r *recorder) PacingRate() int                    { return 2_000_000 }

func feedback(sendMs float64, acks ...wire.Ack) []byte {
	b := []byte{wire.TypeFeedback, 0, 0, 0, 0}
	b = binary.BigEndian.AppendUint64(b, math.Float64bits(sendMs))
	b = binary.BigEndian.AppendUint16(b, uint16(len(acks)))
	for _, a := range acks {
		b = binary.BigEndian.AppendUint32(b, a.Seq)
		b = binary.BigEndian.AppendUint64(b, math.Float64bits(a.RecvMs))
	}
	return b
}

// One frame goes out paced, the browser reports all but one packet, NACKs it,
// and the retransmission goes out with a new seq.
func TestSenderPacingFeedbackAndNack(t *testing.T) {
	var mu sync.Mutex
	var wrote [][]byte
	var times []time.Time
	ctrl := &recorder{}
	s := newSender(func(b []byte) error {
		mu.Lock()
		defer mu.Unlock()
		wrote = append(wrote, append([]byte(nil), b...))
		times = append(times, time.Now())
		return nil
	}, ctrl, senderConfig{MaxQueue: time.Second, History: time.Second, ReorderGap: 2, LossTimeout: time.Second}, false, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.run(ctx)

	const frags = 20
	frame := make([]byte, wire.MaxPayload*(frags-1)+100)
	if !s.enqueueFrame(7, frame, true, time.Now()) {
		t.Fatal("frame dropped")
	}
	// 22 kB at 2 Mbps is about 90 ms.
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	if len(wrote) != frags {
		t.Fatalf("wrote %d packets, want %d", len(wrote), frags)
	}
	if span := times[frags-1].Sub(times[0]); span < 60*time.Millisecond || span > 150*time.Millisecond {
		t.Errorf("frame left in %v, want about 90ms", span)
	}
	for i, b := range wrote {
		if seq := binary.BigEndian.Uint32(b[2:]); seq != uint32(i) {
			t.Errorf("packet %d has seq %d", i, seq)
		}
		if idx := binary.BigEndian.Uint16(b[18:]); idx != uint16(i) || b[1] != wire.FlagKey {
			t.Errorf("packet %d: fragIdx %d flags %d", i, idx, b[1])
		}
	}
	mu.Unlock()

	// Everything but seq 5 arrived.
	var acks []wire.Ack
	for i := 0; i < frags; i++ {
		if i != 5 {
			acks = append(acks, wire.Ack{Seq: uint32(i), RecvMs: 1000 + float64(i)})
		}
	}
	s.onFeedback(feedback(1020, acks...), time.Now())
	s.onFeedback(feedback(1070, acks...), time.Now()) // the repeat must change nothing
	if len(ctrl.fbs) != 2 || len(ctrl.fbs[0].Results) != frags || len(ctrl.fbs[1].Results) != 0 {
		t.Fatalf("results per feedback: %d then %d", len(ctrl.fbs[0].Results), len(ctrl.fbs[1].Results))
	}
	for i, r := range ctrl.fbs[0].Results {
		if r.Seq != uint32(i) || r.Received != (i != 5) {
			t.Errorf("result %d: seq %d received %v", i, r.Seq, r.Received)
		}
	}
	if ctrl.fbs[0].RTT <= 0 {
		t.Error("no RTT")
	}

	nack := []byte{wire.TypeNack, 0, 2, 0, 0, 0, 7, 0, 5, 0, 0, 0, 99, 0xFF, 0xFF} // frame 7 frag 5; all of frame 99 (unknown)
	s.onNack(nack)
	s.onNack(nack) // answered once only
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(wrote) != frags+1 {
		t.Fatalf("wrote %d packets after the NACK, want %d", len(wrote), frags+1)
	}
	rtx := wrote[frags]
	if rtx[1] != wire.FlagKey|wire.FlagRtx || binary.BigEndian.Uint32(rtx[2:]) != frags || binary.BigEndian.Uint16(rtx[18:]) != 5 {
		t.Errorf("retransmission: flags %d seq %d frag %d", rtx[1], binary.BigEndian.Uint32(rtx[2:]), binary.BigEndian.Uint16(rtx[18:]))
	}
	if s.c.NackMisses != 2 {
		t.Errorf("NackMisses = %d, want 2", s.c.NackMisses)
	}
}

// A frame is refused while the pacer queue is longer than MaxQueue.
func TestSenderDropsWhenQueueIsLong(t *testing.T) {
	s := newSender(func([]byte) error { return nil }, &recorder{},
		senderConfig{MaxQueue: 100 * time.Millisecond, History: time.Second}, false, nil)
	if !s.enqueueFrame(0, make([]byte, 50_000), true, time.Now()) { // 200 ms at 2 Mbps
		t.Fatal("first frame dropped")
	}
	if s.enqueueFrame(1, make([]byte, 1000), false, time.Now()) {
		t.Fatal("second frame accepted behind a 200 ms queue")
	}
}
