package simulcast

import (
	"bytes"
	"context"
	"math"
	"net"
	"os"
	"testing"
	"time"
)

func TestTimeline(t *testing.T) {
	var tl timeline
	t0 := time.Unix(1_700_000_000, 0)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	const base = 1 << 20

	if got := tl.observe(base, at(0)); !got.Equal(at(0)) {
		t.Fatalf("anchor = %v, want the arrival time", got)
	}
	// Capture times keep the PTS spacing whatever the arrival jitter, and are
	// the same for the same PTS (another layer's copy of the frame).
	if got := tl.observe(base+3000, at(60)); !got.Equal(at(0).Add(ptsDuration(3000))) {
		t.Fatalf("second frame = %v", got.Sub(t0))
	}
	a, b := tl.observe(base+90000, at(1010)), tl.observe(base+90000, at(1042))
	if !a.Equal(at(1000)) || !b.Equal(a) {
		t.Fatalf("same PTS: %v and %v, want both 1s", a.Sub(t0), b.Sub(t0))
	}
	if w, ok := tl.wallOf(base + 45000); !ok || !w.Equal(at(500)) {
		t.Fatalf("wallOf = %v %v", w.Sub(t0), ok)
	}

	// A stall and the burst that catches up: stamped with arrival times while
	// it lasts, and the mapping is where it was afterwards.
	if got := tl.observe(base+2*90000, at(4500)); !got.Equal(at(4500)) {
		t.Fatalf("stalled frame = %v, want its arrival", got.Sub(t0))
	}
	tl.observe(base+3*90000, at(4510))
	reanchors := func(tl *timeline) int64 { n, _ := tl.stats(); return n }
	if got := tl.observe(base+5*90000, at(5020)); !got.Equal(at(5000)) || reanchors(&tl) != 0 {
		t.Fatalf("after the stall: %v, reanchors %d", got.Sub(t0), reanchors(&tl))
	}

	// The source restarts on a new timeline: after a second of it, a new anchor.
	const fresh = 7 << 28
	if got := tl.observe(fresh, at(6000)); !got.Equal(at(6000)) || reanchors(&tl) != 0 {
		t.Fatalf("first frame of the new timeline = %v, reanchors %d", got.Sub(t0), reanchors(&tl))
	}
	tl.observe(fresh+45000, at(6500))
	if got := tl.observe(fresh+90000, at(7000)); !got.Equal(at(7000)) || reanchors(&tl) != 1 {
		t.Fatalf("re-anchor: %v, reanchors %d", got.Sub(t0), reanchors(&tl))
	}
	if got := tl.observe(fresh+2*90000, at(8003)); !got.Equal(at(8000)) {
		t.Fatalf("after re-anchor = %v", got.Sub(t0))
	}

	// The 33-bit wrap is an ordinary step.
	var w timeline
	w.observe(ptsMask-1000, at(0))
	if got := w.observe(2000, at(40)); !got.Equal(at(0).Add(ptsDuration(3001))) || reanchors(&w) != 0 {
		t.Fatalf("across the wrap = %v, reanchors %d", got.Sub(t0), reanchors(&w))
	}
}

// A source whose clock runs fast or slow is followed a few ms per window, and
// one early burst does not drag the mapping with it.
func TestTimelineFollowsDrift(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	// run feeds 30 fps for the given time with the source's clock running at
	// `rate` × real time, and returns the last frame's arrival − capture.
	run := func(tl *timeline, seconds int, rate float64) (late time.Duration) {
		for i := 0; i <= seconds*30; i++ {
			arrival := t0.Add(time.Duration(float64(i) / 30 / rate * float64(time.Second)))
			late = arrival.Sub(tl.observe(int64(i)*3000, arrival))
		}
		return late
	}
	var exact timeline
	if late := run(&exact, 120, 1); late.Abs() > time.Microsecond {
		t.Fatalf("no drift: last frame %v late", late)
	}
	// 200 ppm fast: 2 ms early per 10 s, 72 ms over 6 minutes if nothing followed it.
	var fast timeline
	if late := run(&fast, 360, 1.0002); late < -5*time.Millisecond || late > time.Millisecond {
		t.Fatalf("fast source: last frame %v late, want within a few ms of 0", late)
	}
	var slow timeline
	if late := run(&slow, 360, 0.9998); late > 5*time.Millisecond || late < -time.Millisecond {
		t.Fatalf("slow source: last frame %v late, want within a few ms of 0", late)
	}
	if n, slew := fast.stats(); n != 0 || slew > -50*time.Millisecond {
		t.Fatalf("fast source: %d re-anchors, slewed %v", n, slew)
	}

	// Half a second of frames 400 ms early, once: one step of slew towards them
	// in that window, taken back in the next.
	var burst timeline
	var worst time.Duration
	for i := 0; i <= 30*30; i++ {
		arrival := t0.Add(time.Duration(i) * time.Second / 30)
		early := i >= 100 && i < 115
		if early {
			arrival = arrival.Add(-400 * time.Millisecond)
		}
		if late := arrival.Sub(burst.observe(int64(i)*3000, arrival)); !early {
			worst = max(worst, late.Abs())
		}
	}
	if _, slew := burst.stats(); worst > timelineSlew+time.Microsecond || slew != 0 {
		t.Fatalf("one early burst moved the other frames by up to %v and left the mapping %v off", worst, slew)
	}
}

func TestPackAU(t *testing.T) {
	au := []byte{
		0, 0, 0, 1, 9, 0xf0, // access unit delimiter
		0, 0, 1, 0x67, 66, 0xc0, 31, // SPS, 3-byte start code
		0, 0, 0, 1, 0x68, 1, // PPS
		0, 0, 0, 1, 0x0c, 0xff, 0xff, 0xff, 0x80, // filler
		0, 0, 1, 0x65, 7, 8, 9, // IDR slice
	}
	want := []byte{0, 0, 0, 1, 0x67, 66, 0xc0, 31, 0, 0, 0, 1, 0x68, 1, 0, 0, 0, 1, 0x65, 7, 8, 9}
	got, info := packAU(au, true)
	if !bytes.Equal(got, want) {
		t.Fatalf("packAU = %x\nwant     %x", got, want)
	}
	if !info.key || info.mediaBytes != 10 || !bytes.Equal(info.sps, []byte{0x67, 66, 0xc0, 31}) {
		t.Fatalf("info = %+v", info)
	}
}

// The PTS of each audio PES is the PTS of the one before plus the audio that
// one held: what the transcoder's gap check relies on.
func TestADTSDurationMatchesTimestamps(t *testing.T) {
	_, pes := demuxFile(t, nil)
	var audio []PES
	for _, p := range pes {
		if p.StreamType == streamTypeAAC {
			audio = append(audio, p)
		}
	}
	if len(audio) < 10 {
		t.Fatalf("only %d audio PES", len(audio))
	}
	pos := float64(audio[0].PTS)
	for i, p := range audio {
		if d := float64(p.PTS) - pos; math.Abs(d) > 2 {
			t.Fatalf("PES %d: PTS %d, expected %.1f from the audio before it", i, p.PTS, pos)
		}
		d := adtsDuration(p.Data)
		if d <= 0 {
			t.Fatalf("PES %d: not measurable", i)
		}
		pos += d
	}
	if adtsDuration([]byte{1, 2, 3, 4, 5, 6, 7, 8}) != 0 || adtsDuration(audio[0].Data[:len(audio[0].Data)-1]) != 0 {
		t.Fatal("garbage or a truncated frame measured as audio")
	}
}

// The same transport stream sent to two layers' ports: a subscriber gets every
// frame of both, stripped, and equal timestamps have equal capture times.
func TestHubDeliversLayersOnOneTimeline(t *testing.T) {
	data, err := os.ReadFile("testdata/low_head.ts")
	if err != nil {
		t.Fatal(err)
	}
	h := NewHub([]LayerConfig{{Name: "a", Addr: "127.0.0.1:0", BitrateBps: 2}, {Name: "b", Addr: "127.0.0.1:0", BitrateBps: 1}}, Options{AudioLayer: -1})
	conns, err := h.Listen()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx, conns)
	sub := h.Subscribe()

	out, err := net.Dial("udp", conns[0].LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	out2, err := net.Dial("udp", conns[1].LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer out2.Close()
	const datagram = 7 * tsPacketSize
	for off := 0; off < len(data); off += datagram {
		end := min(off+datagram, len(data))
		if _, err := out.Write(data[off:end]); err != nil {
			t.Fatal(err)
		}
		if _, err := out2.Write(data[off:end]); err != nil {
			t.Fatal(err)
		}
		if off/datagram%8 == 7 {
			time.Sleep(time.Millisecond) // stay inside the socket buffer
		}
	}

	// 119 frames per layer (the last one is only emitted when the next begins).
	capture := map[int64][2]time.Time{}
	count := [2]int{}
	deadline := time.After(5 * time.Second)
	for count[0]+count[1] < 2*119 {
		select {
		case f := <-sub.Video:
			if count[f.Layer] == 0 && !f.Key {
				t.Fatalf("layer %d: first frame is not a keyframe", f.Layer)
			}
			for _, n := range splitNALs(f.Data) {
				if typ := n[0] & 0x1f; typ == nalAUD || typ == nalFiller {
					t.Fatalf("layer %d: NAL type %d was forwarded", f.Layer, typ)
				}
			}
			c := capture[f.PTS]
			c[f.Layer] = f.Capture
			capture[f.PTS] = c
			count[f.Layer]++
		case <-deadline:
			t.Fatalf("got %v frames, want 119 of each layer", count)
		}
	}
	// The whole clip went out at once. Its first second is "early" and is handed
	// on at its own pace; the rest is too far ahead to be on this timeline at
	// all and is stamped (and handed on) as it came.
	const frames = 30
	for i := 0; i <= frames; i++ {
		if c := capture[int64(i)*1500]; !c[0].Equal(c[1]) {
			t.Fatalf("frame %d: capture %v on one layer, %v on the other", i, c[0], c[1])
		}
	}
	// 60 fps: frame 30 was captured half a second after frame 0, however fast it was sent.
	if got, want := capture[frames*1500][0].Sub(capture[0][0]), ptsDuration(frames*1500); got != want {
		t.Fatalf("frame %d captured %v after frame 0, want %v", frames, got, want)
	}
	st := h.Status()
	if st[0].Width != 320 || st[0].Height != 180 || !st[0].Live || st[1].Frames != 119 || math.Abs(st[0].FPS-60) > 0.01 || st[0].Held < frames {
		t.Fatalf("status = %+v", st)
	}
}
