package ratecontrol

import (
	"testing"
	"time"
)

func TestSweep(t *testing.T) {
	c, err := New("sweep", Config{StartBitrate: 1_000_000, MinBitrate: 100_000, MaxBitrate: 6_400_000, PacingFactor: 2})
	if err != nil {
		t.Fatal(err)
	}
	s := c.(*Sweep)
	near := func(got, want int) bool { return got > want*99/100 && got < want*101/100 }
	// 6 doublings on the way up: one every 5 s.
	for sec, want := range map[int]int{0: 100_000, 5: 200_000, 15: 800_000, 30: 6_400_000, 45: 800_000, 55: 200_000, 60: 100_000, 65: 200_000} {
		if got := s.at(time.Duration(sec) * time.Second); !near(got, want) {
			t.Errorf("at %d s: %d bps, want %d", sec, got, want)
		}
	}
	if p, tgt := s.PacingRate(), s.TargetBitrate(); p < tgt*19/10 || p > tgt*21/10 {
		t.Errorf("pacing %d for a target of %d", p, tgt)
	}
}
