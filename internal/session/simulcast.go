package session

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"

	mediasrc "puffer-datachannels/internal/media"
	"puffer-datachannels/internal/simulcast"
)

// Simulcast sessions forward the shared, pre-encoded renditions from a
// simulcast.Hub instead of running their own decoder and encoder. Everything
// after the encoder is the same as for the file source: the sender fragments,
// paces and retransmits, and the rate controller sees every packet. What the
// controller's target decides here is not an encoder bitrate but which
// rendition to forward. Switches happen on a keyframe of the new rendition;
// the renditions have their keyframes at the same timestamps, and the hub
// stamps equal timestamps with equal capture times, so the browser sees one
// continuous run of frame ids and capture times whose picture size changes.
//
// The renditions' copies of one instant do not arrive together, and which
// comes first is not fixed. Once a switch is decided, the current rendition's
// next keyframe is therefore held back instead of forwarded: the new
// rendition's keyframe for that instant is about to arrive and takes its
// place. Forwarding both would send the old rendition's largest frame for
// nothing, usually just as the budget shrank, and if the new rendition lagged
// by more than a frame the capture times would step backwards. If the other
// keyframe has not come within simSwitchHold, what was held is forwarded
// after all and the switch waits for the next keyframe.
//
// What a shared encoder cannot do is produce a keyframe on request. Where the
// file source forces an IDR (the browser asked, or the pacer refused a
// frame), a simulcast session stops forwarding until the rendition's next
// periodic keyframe: the browser could not decode anything in between anyway.
// The renditions' keyframe interval therefore bounds startup, how soon a
// switch takes effect, and recovery from a loss that retransmission did not
// repair.

// Sources of OfferRequest.Source.
const (
	SourceFile      = "file"      // decode and encode the video file for this session alone
	SourceSimulcast = "simulcast" // forward the shared renditions
)

// ErrNoSource: the requested source is not there to be served.
var ErrNoSource = errors.New("source unavailable")

// SetSimulcast enables simulcast sessions backed by hub.
func (m *Manager) SetSimulcast(hub *simulcast.Hub) { m.hub = hub }

// Sources lists what an offer may ask for, the default first.
func (m *Manager) Sources() []string {
	var out []string
	if m.info != nil {
		out = append(out, SourceFile)
	}
	if m.hub != nil {
		out = append(out, SourceSimulcast)
	}
	return out
}

// simulcastInfo describes the simulcast source in the media.Info shape the
// page and the logs already understand. Width, height and frame rate are
// those of the highest rendition that is live.
func (m *Manager) simulcastInfo() (*mediasrc.Info, error) {
	if m.hub == nil {
		return nil, fmt.Errorf("%w: this server has no simulcast source (--simulcast)", ErrNoSource)
	}
	for _, l := range m.hub.Status() {
		if !l.Live || l.Width == 0 {
			continue
		}
		fps := l.FPS
		if fps <= 0 {
			fps = 30
		}
		info := &mediasrc.Info{Path: SourceSimulcast, Width: l.Width, Height: l.Height,
			FPSNum: int(math.Round(fps * 1000)), FPSDen: 1000, FPS: fps, HasAudio: m.hub.HasAudio(), VideoCodec: "h264"}
		if info.HasAudio {
			info.AudioCodec = "opus"
		}
		return info, nil
	}
	return nil, fmt.Errorf("%w: no rendition of the simulcast source is live (is the transcoder sending to the --simulcast addresses?)", ErrNoSource)
}

func (m *Manager) newSimState() *simState {
	layers := m.hub.Layers()
	rates := make([]int, len(layers))
	for i, l := range layers {
		rates[i] = l.BitrateBps
	}
	return &simState{layers: layers, sel: simulcast.NewSelector(rates), cur: -1, pending: -1,
		lastKeyID: -1, audioLast: -1, layerFrames: make([]int64, len(layers))}
}

// simState is the per-session simulcast forwarding state.
type simState struct {
	sub    *simulcast.Subscriber
	layers []simulcast.LayerConfig
	sel    *simulcast.Selector

	mu          sync.Mutex // guards everything below; taken before the sender's lock, never after
	cur         int        // layer being forwarded, -1 before the first keyframe
	pending     int        // layer to switch to at its next keyframe, -1 = none
	needKey     bool       // forward nothing until the current layer's next keyframe
	budget      int
	target      int
	nextID      uint32 // frame id of the next forwarded frame
	lastKeyID   int64  // frame id of the newest keyframe handed to the sender, -1 = none
	switches    int64
	keyWaits    int64 // times forwarding stopped to wait for a keyframe
	waitFrames  int64 // frames of the current layer not forwarded while waiting
	startedAt   time.Time
	firstFrame  time.Time
	layerFrames []int64
	lastCapture time.Time // capture time of the newest frame handed to the sender

	// Frames of the current layer, from a keyframe on, held while a switch is
	// pending (see the comment at the top of the file).
	held        []*simulcast.Frame
	heldSince   time.Time
	heldDropped int64 // held frames the new layer's keyframe replaced

	// Audio packet ids follow capture time: id n is the packet captured
	// n × 20 ms after the first, so a gap in the audio is a gap in the ids.
	audioAnchor time.Time
	audioLast   int64
}

func (st *simState) name(i int) string {
	if i < 0 || i >= len(st.layers) {
		return "none"
	}
	return st.layers[i].Name
}

// describe is the "simulcast" entry of meta.json and the hello message (nil
// for file sessions).
func (s *Session) simDescribe() map[string]any {
	st := s.sim
	if st == nil {
		return nil
	}
	return map[string]any{"layers": s.mgr.hub.Status(),
		"upMargin": st.sel.UpMargin, "upHoldMs": float64(st.sel.UpHold.Milliseconds())}
}

// runSimulcast starts forwarding once the media channel is open.
// target is the controller's target as the session starts.
func (s *Session) runSimulcast(dc *webrtc.DataChannel, snd *sender, target int) {
	st := s.sim
	st.sub = s.mgr.hub.Subscribe()
	go func() {
		<-s.ctx.Done()
		s.mgr.hub.Unsubscribe(st.sub)
	}()
	now := time.Now()
	st.mu.Lock()
	st.startedAt = now
	st.target, st.budget = target, s.videoBitrateFor(target)
	st.pending = st.sel.Decide(now, st.budget, -1)
	first, budget := st.pending, st.budget
	st.mu.Unlock()
	s.log.Log("encoder", "reconfig", map[string]any{"target": target, "videoTarget": budget, "applied": st.layers[first].BitrateBps,
		"initial": true, "source": SourceSimulcast, "layer": st.name(first), "headroom": s.cfg.Headroom})
	s.event("media", fmt.Sprintf("started: simulcast, waiting for a %s keyframe", st.name(first)))
	if lowest := st.layers[len(st.layers)-1]; budget < lowest.BitrateBps {
		s.event("layerTooLarge", fmt.Sprintf("the video budget, %d kbps, is below the lowest rendition (%s, %d kbps): frames will be dropped",
			budget/1000, lowest.Name, lowest.BitrateBps/1000))
	}
	s.encMu.Lock()
	s.encWin.since = now
	s.encMu.Unlock()
	go snd.run(s.ctx)
	go s.snapshotLoop(dc, snd)
	go s.simLoop(snd)
}

// simLoop forwards frames and audio and re-evaluates the layer every 100 ms.
func (s *Session) simLoop(snd *sender) {
	st := s.sim
	decide := time.NewTicker(100 * time.Millisecond)
	defer decide.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case f := <-st.sub.Video:
			s.simFrame(f, snd)
		case a := <-st.sub.Audio:
			if s.audioOn {
				s.simAudio(a, snd)
			}
		case now := <-decide.C:
			s.simDecide(now, snd)
		}
	}
}

// simDecide re-runs layer selection against the controller's target.
func (s *Session) simDecide(now time.Time, snd *sender) {
	st := s.sim
	snd.mu.Lock()
	target := s.ctrl.TargetBitrate()
	snd.mu.Unlock()
	budget := s.videoBitrateFor(target)

	st.mu.Lock()
	st.target, st.budget = target, budget
	desired := st.sel.Decide(now, budget, st.cur)
	var ev string
	switch {
	case st.cur < 0:
		st.pending = desired
	case desired != st.cur && desired != st.pending:
		ev = fmt.Sprintf("%s -> %s at next keyframe (budget %d kbps)", st.name(st.cur), st.name(desired), budget/1000)
		st.pending = desired
	case desired == st.cur && st.pending >= 0:
		ev = fmt.Sprintf("cancel switch to %s (budget %d kbps)", st.name(st.pending), budget/1000)
		st.pending = -1
	}
	st.mu.Unlock()
	if ev != "" {
		s.event("layerPending", ev)
	}
}

// simSwitchHold is how long the current layer's keyframe waits for the
// keyframe of the layer being switched to.
const simSwitchHold = 100 * time.Millisecond

// simFrame forwards f if it belongs to the layer being forwarded, switching
// layers when f is the keyframe a pending switch was waiting for.
func (s *Session) simFrame(f *simulcast.Frame, snd *sender) {
	st := s.sim
	st.mu.Lock()
	defer st.mu.Unlock()
	now := time.Now()
	if st.sub.Overflow.Swap(false) && st.cur >= 0 && !st.needKey {
		st.needKey = true
		st.keyWaits++
		s.event("layer", "subscriber queue overflowed; waiting for a keyframe")
	}
	// What was held for a switch that did not come goes out after all.
	if len(st.held) > 0 && (st.pending < 0 || now.Sub(st.heldSince) > simSwitchHold) {
		held := st.held
		st.held = nil
		for _, h := range held {
			s.simForward(h, snd, now)
		}
	}
	switch {
	case st.cur < 0:
		if f.Layer != st.pending || !f.Key {
			return
		}
		st.cur, st.pending, st.firstFrame = st.pending, -1, now
		s.event("layer", fmt.Sprintf("forwarding %s (first keyframe after %.0f ms)", st.name(st.cur), ms(now.Sub(st.startedAt))))
		s.simLogSwitch()
	case st.pending >= 0 && f.Layer == st.pending && f.Key && f.Capture.After(st.lastCapture):
		s.event("layer", fmt.Sprintf("%s -> %s", st.name(st.cur), st.name(st.pending)))
		st.cur, st.pending, st.needKey = st.pending, -1, false
		st.switches++
		st.heldDropped += int64(len(st.held))
		st.held = nil
		s.simLogSwitch()
	case st.pending >= 0 && f.Layer == st.cur && (f.Key || len(st.held) > 0):
		if len(st.held) == 0 {
			st.heldSince = now
		}
		st.held = append(st.held, f)
		return
	}
	if f.Layer == st.cur {
		s.simForward(f, snd, now)
	}
}

// simLogSwitch records the layer now being forwarded the way the file source
// records an encoder reconfiguration. Called with st.mu held.
func (s *Session) simLogSwitch() {
	st := s.sim
	s.log.Log("encoder", "reconfig", map[string]any{"target": st.target, "videoTarget": st.budget,
		"applied": st.layers[st.cur].BitrateBps, "source": SourceSimulcast, "layer": st.name(st.cur)})
}

// simForward hands a frame of the current layer to the sender, unless the
// session is waiting for a keyframe and this is not one. Called with st.mu held.
func (s *Session) simForward(f *simulcast.Frame, snd *sender, now time.Time) {
	st := s.sim
	if st.needKey && !f.Key {
		st.waitFrames++
		return
	}
	st.needKey = false
	id := st.nextID
	queued := snd.enqueueFrame(id, f.Data, f.Key, f.Capture)
	if queued {
		st.nextID++
		st.layerFrames[st.cur]++
		st.lastCapture = f.Capture
		if f.Key {
			st.lastKeyID = int64(id)
		}
		s.countFrame(len(f.Data), f.Key, 0)
	} else {
		// The frames after this one reference it. Nothing more of this layer
		// is decodable until its next keyframe; the id is used again for that.
		st.needKey = true
		st.keyWaits++
		s.event("frameDropped", fmt.Sprintf("frame %d (%s): pacer queue over %v; waiting for a keyframe", id, st.name(st.cur), s.cfg.MaxQueue))
	}
	// The file source's frame record, with nothing encoded here (encMs 0), plus
	// the layer, the source timestamp, how far behind the timeline the frame
	// reached the hub (negative: ahead, and held there until its time) and how
	// long it was in the hub and this session.
	s.log.LogAt("encoder", now, "frame", map[string]any{
		"id": id, "bytes": len(f.Data), "key": f.Key, "forced": false, "layer": st.name(st.cur), "pts": f.PTS,
		"encMs": 0.0, "hubMs": ms(now.Sub(f.Arrival)), "lateMs": ms(f.Arrival.Sub(f.Capture)),
		"cfgBps": st.layers[st.cur].BitrateBps, "queued": queued,
	})
}

// simAudio forwards one Opus packet, numbering it by its capture time.
func (s *Session) simAudio(a *simulcast.AudioSample, snd *sender) {
	st := s.sim
	st.mu.Lock()
	if st.audioAnchor.IsZero() {
		st.audioAnchor = a.Capture
	}
	n := int64(math.Round(float64(a.Capture.Sub(st.audioAnchor)) / float64(simulcast.AudioFrame)))
	if n <= st.audioLast {
		st.mu.Unlock()
		return // the audio transcoder restarted on an overlapping stretch
	}
	st.audioLast = n
	capture := st.audioAnchor.Add(time.Duration(n) * simulcast.AudioFrame)
	st.mu.Unlock()

	lag := ms(time.Since(capture))
	snd.enqueueAudio(uint32(n), a.Data, capture)
	s.encMu.Lock()
	s.audWin.Packets++
	s.audWin.Bytes += int64(len(a.Data))
	s.audWin.LagSum += lag
	s.audWin.LagMax = max(s.audWin.LagMax, lag)
	s.encMu.Unlock()
}

// simKeyRequest handles the browser's KEYREQ; newest is the highest frame id
// it had seen when it asked. The request is spent if a keyframe newer than
// that is already on its way: the browser repeats itself until one arrives,
// and each repeat taken at face value would cost another keyframe interval.
func (s *Session) simKeyRequest(newest uint32) {
	st := s.sim
	snd := s.snd.Load()
	st.mu.Lock()
	wait := st.cur >= 0 && !st.needKey && int64(newest) >= st.lastKeyID &&
		!(st.lastKeyID >= 0 && snd != nil && snd.videoQueued(uint32(st.lastKeyID)))
	if wait {
		st.needKey = true
		st.keyWaits++
	}
	layer := st.name(st.cur)
	st.mu.Unlock()
	if wait {
		s.event("keyframeRequest", fmt.Sprintf("browser asked for a keyframe at frame %d: waiting for the next one of %s", newest, layer))
	}
}

// simLayerBitrate is the nominal bitrate of the layer being forwarded.
func (s *Session) simLayerBitrate() int {
	st := s.sim
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.cur < 0 {
		return 0
	}
	return st.layers[st.cur].BitrateBps
}

// simSnapshot is the "simulcast" section of snapshots.
func (s *Session) simSnapshot() map[string]any {
	st := s.sim
	st.mu.Lock()
	defer st.mu.Unlock()
	out := map[string]any{
		"layer": st.name(st.cur), "layerIndex": st.cur, "pending": nil, "budgetBps": st.budget, "targetBps": st.target,
		"waitingForKey": st.needKey || st.cur < 0, "switches": st.switches, "keyframeWaits": st.keyWaits,
		"framesSkippedWaitingForKey": st.waitFrames, "framesReplacedAtSwitch": st.heldDropped,
	}
	if st.cur >= 0 {
		out["layerBps"] = st.layers[st.cur].BitrateBps
	}
	if st.pending >= 0 {
		out["pending"] = st.name(st.pending)
	}
	if st.sub != nil {
		out["subscriberDrops"] = st.sub.Dropped.Load()
	}
	frames := map[string]int64{}
	for i, l := range st.layers {
		frames[l.Name] = st.layerFrames[i]
	}
	out["framesByLayer"] = frames
	if !st.firstFrame.IsZero() {
		out["startupMs"] = ms(st.firstFrame.Sub(st.startedAt))
	}
	return out
}
