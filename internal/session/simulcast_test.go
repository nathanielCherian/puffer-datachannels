package session

import (
	"encoding/binary"
	"testing"
	"time"

	"puffer-datachannels/internal/simulcast"
	"puffer-datachannels/internal/wire"
)

// simSession is a simulcast session with no PeerConnection: frames are fed to
// simFrame by hand and what the sender wrote is collected.
type simSession struct {
	*Session
	snd   *sender
	wrote [][]byte
	t0    time.Time
	tick  int // the instant of the newest frame fed by frame()
}

func newSimSession(t *testing.T, maxQueue time.Duration) *simSession {
	t.Helper()
	layers, err := simulcast.ParseLayers("high=127.0.0.1:1@2500k,mid=127.0.0.1:2@800k,low=127.0.0.1:3@250k")
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{hub: simulcast.NewHub(layers, simulcast.Options{AudioLayer: -1}), sessions: map[string]closer{}}
	ss := &simSession{Session: &Session{mgr: m, cfg: Config{MaxQueue: maxQueue, Headroom: 1}, ctrl: &recorder{}, sim: m.newSimState()}}
	ss.sim.sub = &simulcast.Subscriber{}
	ss.snd = newSender(func(b []byte) error {
		ss.wrote = append(ss.wrote, append([]byte(nil), b...))
		return nil
	}, ss.ctrl, senderConfig{MaxQueue: maxQueue, History: time.Second, ReorderGap: 16, LossTimeout: time.Second}, false, nil)
	ss.Session.snd.Store(ss.snd)
	return ss
}

// drain sends everything the pacer holds and returns the (frame id, keyframe)
// of each frame that went out, in order.
func (ss *simSession) drain() (ids []uint32, keys []bool) {
	ss.snd.mu.Lock()
	for p := ss.snd.popLocked(); p != nil; p = ss.snd.popLocked() {
		ss.snd.sendLocked(p)
	}
	ss.snd.mu.Unlock()
	for _, b := range ss.wrote {
		if binary.BigEndian.Uint16(b[18:]) == 0 { // first fragment
			ids = append(ids, binary.BigEndian.Uint32(b[14:]))
			keys = append(keys, b[1]&wire.FlagKey != 0)
		}
	}
	ss.wrote = nil
	return ids, keys
}

// frame feeds a frame of a layer for the next instant (instants are 1 ms of
// capture time apart).
func (ss *simSession) frame(layer int, key bool, size int) {
	if ss.t0.IsZero() {
		ss.t0 = time.Now()
	}
	ss.tick++
	ss.frameAt(layer, ss.tick, key, size)
}

// frameAt feeds a layer's frame for instant n.
func (ss *simSession) frameAt(layer, n int, key bool, size int) {
	ss.simFrame(&simulcast.Frame{Layer: layer, Key: key, Data: make([]byte, size),
		Capture: ss.t0.Add(time.Duration(n) * time.Millisecond), Arrival: time.Now()}, ss.snd)
}

// captures returns the capture time, as an instant number, of each frame the
// pacer holds.
func (ss *simSession) captures() (out []int) {
	ss.snd.mu.Lock()
	defer ss.snd.mu.Unlock()
	for _, p := range ss.snd.queue {
		if binary.BigEndian.Uint16(p.buf[18:]) == 0 {
			us := int64(binary.BigEndian.Uint64(p.buf[22:])) - ss.t0.UnixMicro()
			out = append(out, int(us/1000))
		}
	}
	return out
}

func sameIDs(got []uint32, want ...uint32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// The session forwards one layer, starts and switches on that layer's
// keyframes, and numbers what it forwards without gaps.
func TestSimulcastForwardsOneLayerAndSwitchesOnKeyframes(t *testing.T) {
	ss := newSimSession(t, time.Second)
	st := ss.sim
	st.pending = 1 // what runSimulcast would have decided: start on mid

	ss.frame(1, false, 500) // mid, but not a keyframe yet
	ss.frame(0, true, 500)  // a keyframe, but of another layer
	if ids, _ := ss.drain(); len(ids) != 0 {
		t.Fatalf("forwarded %v before a keyframe of the chosen layer", ids)
	}
	ss.frame(1, true, 3000)
	ss.frame(0, false, 500)
	ss.frame(1, false, 500)
	ss.frame(2, false, 500)
	ss.frame(1, false, 500)
	ids, keys := ss.drain()
	if !sameIDs(ids, 0, 1, 2) || !keys[0] || keys[1] {
		t.Fatalf("mid: ids %v keys %v, want 0 1 2 starting with a keyframe", ids, keys)
	}

	// A switch waits for the new layer's keyframe, and the ids carry on.
	st.mu.Lock()
	st.pending = 0
	st.mu.Unlock()
	ss.frame(0, false, 500) // high, not a keyframe: still forwarding mid
	ss.frame(1, false, 500)
	ss.frame(0, true, 5000)
	ss.frame(1, true, 3000) // mid's keyframe at the same instant: no longer ours
	ss.frame(0, false, 500)
	ids, keys = ss.drain()
	if !sameIDs(ids, 3, 4, 5) || keys[0] || !keys[1] || keys[2] {
		t.Fatalf("switch: ids %v keys %v, want 3 (mid) 4 (high keyframe) 5", ids, keys)
	}
	if st.cur != 0 || st.pending != -1 || st.switches != 1 || st.layerFrames[0] != 2 || st.layerFrames[1] != 4 {
		t.Fatalf("after switch: cur %d pending %d switches %d frames %v", st.cur, st.pending, st.switches, st.layerFrames)
	}
}

func sameInts(got []int, want ...int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// Whichever layer's copy of the keyframe instant arrives first, the viewer
// gets that instant once, from the new layer, and time never steps back.
func TestSimulcastSwitchSendsTheKeyframeInstantOnce(t *testing.T) {
	start := func() *simSession {
		ss := newSimSession(t, time.Second)
		ss.sim.pending = 1
		ss.t0 = time.Now()
		ss.frameAt(1, 1, true, 3000) // forwarding mid from instant 1
		ss.frameAt(1, 2, false, 500)
		ss.sim.pending = 2 // decided: down to low at its next keyframe (instant 5)
		ss.frameAt(1, 3, false, 500)
		ss.frameAt(2, 3, false, 500)
		ss.frameAt(1, 4, false, 500)
		return ss
	}

	// The old layer's keyframe first, and even a frame after it: held, then replaced.
	ss := start()
	ss.frameAt(1, 5, true, 3000)
	ss.frameAt(1, 6, false, 500)
	if got := ss.captures(); !sameInts(got, 1, 2, 3, 4) {
		t.Fatalf("before the new keyframe: instants %v, want the old layer's keyframe held back", got)
	}
	ss.frameAt(2, 5, true, 2000)
	ss.frameAt(2, 6, false, 500)
	ss.frameAt(1, 7, false, 500)
	if got := ss.captures(); !sameInts(got, 1, 2, 3, 4, 5, 6) || ss.sim.cur != 2 || ss.sim.heldDropped != 2 {
		t.Fatalf("old keyframe first: instants %v, cur %d, replaced %d", got, ss.sim.cur, ss.sim.heldDropped)
	}
	if ids, keys := ss.drain(); !sameIDs(ids, 0, 1, 2, 3, 4, 5) || !keys[4] {
		t.Fatalf("ids %v keys %v, want six in a row with the fifth a keyframe", ids, keys)
	}

	// The new layer's keyframe first: the switch is immediate.
	ss = start()
	ss.frameAt(2, 5, true, 2000)
	ss.frameAt(1, 5, true, 3000)
	ss.frameAt(2, 6, false, 500)
	if got := ss.captures(); !sameInts(got, 1, 2, 3, 4, 5, 6) || ss.sim.cur != 2 || ss.sim.heldDropped != 0 {
		t.Fatalf("new keyframe first: instants %v, cur %d, replaced %d", got, ss.sim.cur, ss.sim.heldDropped)
	}

	// The new layer's keyframe never comes: what was held goes out, in order,
	// and a keyframe that turns up for an instant already sent is not switched to.
	ss = start()
	ss.frameAt(1, 5, true, 3000)
	ss.frameAt(1, 6, false, 500)
	ss.sim.mu.Lock()
	ss.sim.heldSince = time.Now().Add(-2 * simSwitchHold)
	ss.sim.mu.Unlock()
	ss.frameAt(1, 7, false, 500)
	ss.frameAt(2, 5, true, 2000) // far too late
	ss.frameAt(1, 8, false, 500)
	if got := ss.captures(); !sameInts(got, 1, 2, 3, 4, 5, 6, 7, 8) || ss.sim.cur != 1 || ss.sim.pending != 2 {
		t.Fatalf("no new keyframe: instants %v, cur %d, pending %d", got, ss.sim.cur, ss.sim.pending)
	}

	// The switch is called off while frames are held: they go out.
	ss = start()
	ss.frameAt(1, 5, true, 3000)
	ss.sim.mu.Lock()
	ss.sim.pending = -1
	ss.sim.mu.Unlock()
	ss.frameAt(1, 6, false, 500)
	if got := ss.captures(); !sameInts(got, 1, 2, 3, 4, 5, 6) || ss.sim.cur != 1 {
		t.Fatalf("switch cancelled: instants %v, cur %d", got, ss.sim.cur)
	}
}

// A keyframe cannot be forced: after a request the session forwards nothing
// until the layer's next keyframe, and a request made before the browser can
// have seen the newest keyframe is not taken up.
func TestSimulcastKeyRequestWaitsForNextKeyframe(t *testing.T) {
	ss := newSimSession(t, time.Second)
	st := ss.sim
	st.pending = 2
	ss.frame(2, true, 3000) // id 0
	ss.frame(2, false, 500) // id 1

	// The keyframe has not left the pacer: the browser cannot be missing it yet.
	ss.simKeyRequest(0)
	if st.needKey {
		t.Fatal("request honoured while the keyframe was still queued")
	}
	ss.drain()

	// The browser saw up to frame 1 and gave up on something: wait.
	ss.simKeyRequest(1)
	if !st.needKey || st.keyWaits != 1 {
		t.Fatalf("needKey %v keyWaits %d after a request", st.needKey, st.keyWaits)
	}
	ss.simKeyRequest(1) // it repeats itself while it waits
	ss.frame(2, false, 500)
	ss.frame(2, false, 500)
	if ids, _ := ss.drain(); len(ids) != 0 || st.waitFrames != 2 {
		t.Fatalf("forwarded %v while waiting for a keyframe (skipped %d)", ids, st.waitFrames)
	}
	ss.frame(2, true, 3000) // id 2
	ss.frame(2, false, 500) // id 3
	ids, keys := ss.drain()
	if !sameIDs(ids, 2, 3) || !keys[0] || st.keyWaits != 1 {
		t.Fatalf("after the keyframe: ids %v keys %v keyWaits %d", ids, keys, st.keyWaits)
	}

	// A request from before that keyframe arrived (the browser had seen frame 1) is spent.
	ss.simKeyRequest(1)
	if st.needKey {
		t.Fatal("stale request honoured")
	}
	// One from after it (the keyframe itself was lost beyond repair) is not.
	ss.simKeyRequest(3)
	if !st.needKey || st.keyWaits != 2 {
		t.Fatalf("needKey %v keyWaits %d after a fresh request", st.needKey, st.keyWaits)
	}
}

// A frame the pacer refuses breaks the chain like a lost one: nothing more is
// forwarded until a keyframe, which takes the refused frame's id.
func TestSimulcastPacerRefusalWaitsForKeyframe(t *testing.T) {
	ss := newSimSession(t, 20*time.Millisecond) // 2 Mbps pacing: 5 kB fills the queue
	st := ss.sim
	st.pending = 0
	ss.frame(0, true, 20_000) // id 0; now far over the limit
	ss.frame(0, false, 500)   // refused
	if !st.needKey || st.nextID != 1 {
		t.Fatalf("needKey %v nextID %d after a refused frame", st.needKey, st.nextID)
	}
	ss.drain()
	ss.frame(0, false, 500)
	ss.frame(0, true, 2000)
	ss.frame(0, false, 500)
	ids, keys := ss.drain()
	if !sameIDs(ids, 1, 2) || !keys[0] {
		t.Fatalf("ids %v keys %v, want the keyframe as 1 and then 2", ids, keys)
	}
}

// Audio packets are numbered by capture time, so a gap in the audio is a gap
// in the ids and the browser's "id × 20 ms" timeline stays true.
func TestSimulcastAudioIDsFollowCaptureTime(t *testing.T) {
	ss := newSimSession(t, time.Second)
	ss.audioOn = true
	t0 := time.Now()
	at := func(msec int) *simulcast.AudioSample {
		return &simulcast.AudioSample{Data: []byte{1, 2, 3}, Capture: t0.Add(time.Duration(msec) * time.Millisecond)}
	}
	for _, msec := range []int{0, 20, 40, 100, 120, 100, 141} { // a 40 ms gap, then a repeat, then 1 ms of rounding
		ss.simAudio(at(msec), ss.snd)
	}
	ss.snd.mu.Lock()
	var ids []uint32
	var captures []int64
	for _, p := range ss.snd.audioQueue {
		ids = append(ids, p.frameID)
		captures = append(captures, int64(binary.BigEndian.Uint64(p.buf[22:]))-t0.UnixMicro())
	}
	ss.snd.mu.Unlock()
	if !sameIDs(ids, 0, 1, 2, 5, 6, 7) {
		t.Fatalf("audio ids %v, want 0 1 2 5 6 7", ids)
	}
	for i, id := range ids {
		if captures[i] != int64(id)*20_000 {
			t.Errorf("packet %d: capture %d µs after the first, want %d", id, captures[i], int64(id)*20_000)
		}
	}
}
