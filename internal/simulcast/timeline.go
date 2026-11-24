package simulcast

import (
	"sync"
	"time"
)

// timeline maps the renditions' shared 90 kHz clock onto the server's wall
// clock, so that a frame or an audio packet can be stamped with a capture time
// the way the file source stamps its own. Two units with the same PTS get the
// same capture time whichever rendition (or the audio transcoder) they came
// through, which is what lets a viewer switch renditions without a step in
// its playout schedule and keeps sound and picture together.
//
// The first frame to arrive is the anchor: its PTS is "now". After that a
// capture time is the anchor plus the PTS difference, so it keeps the source's
// spacing exactly and ignores how unevenly the transcoder delivered.
//
// Drift. The source's clock and this machine's are not the same clock. Every
// timelineWindow of media the anchor is moved, by at most timelineSlew, towards
// where the frame that arrived soonest in that window would have had zero
// delay. So "capture time" settles on "when the promptest frames reach the
// hub", and a clock that runs a little fast or slow is followed instead of
// piling up.
//
// Restarts. A source that restarts begins a new PTS timeline. When what
// arrives has been more than timelineJump away from the mapping for
// timelineJump, the mapping is anchored again on the next arrival. Until then
// such units are stamped with their arrival time: a stalled transcoder that
// then catches up looks the same for a moment, and must not move the anchor.
type timeline struct {
	mu       sync.Mutex
	have     bool
	pts      int64 // anchor: 33 bits, as transmitted; also where the current window began
	wall     time.Time
	minLate  time.Duration // smallest arrival − capture in the current window
	haveLate bool
	offSince time.Time // first arrival of the current run outside the mapping
	anchors  int64     // how many times the anchor was set
	slew     time.Duration
}

const (
	timelineJump   = time.Second
	timelineWindow = 10 * 90000 // PTS ticks
	timelineSlew   = 5 * time.Millisecond
	ptsMask        = 1<<33 - 1
)

// ptsDiff returns a − b on the 33-bit PTS circle.
func ptsDiff(a, b int64) int64 {
	d := (a - b) & ptsMask
	if d >= 1<<32 {
		d -= 1 << 33
	}
	return d
}

func ptsDuration(ticks int64) time.Duration { return time.Duration(ticks) * time.Second / 90000 }

// observe returns the capture time of a unit with this PTS that arrived now.
func (t *timeline) observe(pts int64, arrival time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.have {
		d := ptsDiff(pts, t.pts)
		at := t.wall.Add(ptsDuration(d))
		if late := arrival.Sub(at); late > -timelineJump && late < timelineJump {
			t.offSince = time.Time{}
			if !t.haveLate || late < t.minLate {
				t.minLate, t.haveLate = late, true
			}
			if d > timelineWindow {
				// The next window starts here. Moving the anchor along also keeps the
				// 33-bit PTS wrap (every 26.5 h) a short step away from it.
				adj := max(-timelineSlew, min(timelineSlew, t.minLate))
				t.pts, t.wall, t.haveLate = pts&ptsMask, at.Add(adj), false
				t.slew += adj
			}
			return at
		}
		if t.offSince.IsZero() {
			t.offSince = arrival
		}
		if arrival.Sub(t.offSince) < timelineJump {
			return arrival
		}
	}
	t.have, t.pts, t.wall, t.offSince, t.haveLate = true, pts&ptsMask, arrival, time.Time{}, false
	t.anchors++
	return arrival
}

// wallOf maps a PTS without it counting as an arrival. ok is false before the
// first frame.
func (t *timeline) wallOf(pts int64) (at time.Time, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.have {
		return time.Time{}, false
	}
	return t.wall.Add(ptsDuration(ptsDiff(pts, t.pts))), true
}

// stats returns how often the mapping was anchored again after the first
// time, and how far it has been slewed in total.
func (t *timeline) stats() (reanchors int64, slew time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return max(t.anchors-1, 0), t.slew
}
