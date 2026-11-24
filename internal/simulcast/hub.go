// Package simulcast ingests pre-encoded H.264 renditions (MPEG-TS over UDP,
// one port per rendition, as produced by the sfu transcoder or
// scripts/simulcast-renditions.sh) and shares them between all viewer
// sessions. Nothing here encodes video: every session forwards the same
// encoded frames and only chooses which rendition to forward. Ported from
// puffer-webrtc-v2 (internal/simulcast); what differs is that every frame and
// audio packet leaves here with a capture time (timeline.go), because on a
// DataChannel that timestamp, not RTP, is what the browser schedules by.
package simulcast

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4/pkg/media/oggreader"
)

// LayerConfig describes one rendition: where it arrives and its nominal bitrate.
type LayerConfig struct {
	Name       string `json:"name"`
	Addr       string `json:"addr"`       // UDP host:port the transcoder sends MPEG-TS to
	BitrateBps int    `json:"bitrateBps"` // nominal (encoder target) bitrate; drives layer selection
}

// ParseLayers parses "high=127.0.0.1:5001@2500k,mid=127.0.0.1:5002@800k,...".
// The result is sorted by bitrate, highest first.
func ParseLayers(spec string) ([]LayerConfig, error) {
	var layers []LayerConfig
	seen := map[string]bool{}
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, rest, ok := strings.Cut(item, "=")
		addr, rate, ok2 := strings.Cut(rest, "@")
		if !ok || !ok2 || name == "" || addr == "" {
			return nil, fmt.Errorf("simulcast layer %q: want name=host:port@bitrate", item)
		}
		bps, err := parseBitrate(rate)
		if err != nil {
			return nil, fmt.Errorf("simulcast layer %q: %v", item, err)
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("simulcast layer %q: %v", item, err)
		}
		if seen[name] {
			return nil, fmt.Errorf("simulcast layer %q defined twice", name)
		}
		seen[name] = true
		layers = append(layers, LayerConfig{Name: name, Addr: addr, BitrateBps: bps})
	}
	if len(layers) == 0 {
		return nil, errors.New("no simulcast layers")
	}
	sort.SliceStable(layers, func(i, j int) bool { return layers[i].BitrateBps > layers[j].BitrateBps })
	return layers, nil
}

func parseBitrate(s string) (int, error) {
	s = strings.TrimSpace(s)
	mult := 1.0
	switch {
	case strings.HasSuffix(s, "k"), strings.HasSuffix(s, "K"):
		mult, s = 1e3, s[:len(s)-1]
	case strings.HasSuffix(s, "M"), strings.HasSuffix(s, "m"):
		mult, s = 1e6, s[:len(s)-1]
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("bad bitrate %q", s)
	}
	return int(v * mult), nil
}

// Frame is one encoded access unit of one layer. It is shared by all
// subscribers and must not be modified.
type Frame struct {
	Layer   int       // index into Hub.Layers() (0 = highest bitrate)
	PTS     int64     // 90 kHz, unwrapped; the same clock for every layer
	Data    []byte    // Annex-B access unit, without access unit delimiters and filler
	Key     bool      // contains an IDR
	Capture time.Time // the frame's place on the shared timeline; equal for equal PTS
	Arrival time.Time // when the hub had all of it; subscribers get it at Capture if that is later
}

// AudioSample is one 20 ms Opus packet, transcoded once from the source's AAC.
type AudioSample struct {
	Data    []byte
	Capture time.Time // when its first sample was captured, on the video's timeline
}

// AudioFrame is the duration of one Opus packet; AudioRate its sample rate.
const (
	AudioFrame = 20 * time.Millisecond
	AudioRate  = 48000
)

// Subscriber receives every frame of every layer plus the audio.
type Subscriber struct {
	Video   chan *Frame
	Audio   chan *AudioSample
	Dropped atomic.Int64 // frames not delivered because Video was full
	// Overflow is set when a frame was dropped; the session must wait for a keyframe.
	Overflow atomic.Bool
}

// Options configures a Hub.
type Options struct {
	AudioLayer   int    // layer whose AAC is transcoded to Opus; -1 = no audio
	AudioBitrate int    // Opus bitrate (bps)
	FFmpeg       string // ffmpeg binary for the audio transcode
}

// Hub owns the ingest of all layers and the subscriber list.
type Hub struct {
	layers []*layer
	opt    Options
	tl     timeline

	mu   sync.RWMutex
	subs map[*Subscriber]struct{}

	audioIn       chan audioChunk
	audioPages    atomic.Int64
	audioDropped  atomic.Int64 // AAC not transcoded (transcoder stalled) or Opus not placed on the timeline
	audioRestarts atomic.Int64
	audioHeld     atomic.Int64 // Opus packets ready ahead of the timeline, delivered at their capture time
	audioLive     atomic.Bool
}

// audioChunk is the payload of one AAC PES: whole ADTS frames, and the PTS of
// the first.
type audioChunk struct {
	data []byte
	pts  int64
}

type layer struct {
	idx int
	cfg LayerConfig

	mu          sync.Mutex
	datagrams   int64
	frames      int64
	keyframes   int64
	mediaBytes  int64
	lastPTS     int64
	ptsBase     int64 // added to the 33-bit PTS to unwrap it
	havePTS     bool
	lastArrival time.Time
	lastKey     time.Time
	sps         SPSInfo
	frameDur    int64 // last plausible PTS delta (90 kHz)
	ccErrors    int64 // copied from the demuxer after each datagram
	droppedPES  int64
	win         rateWindow
	held        int64 // frames that arrived ahead of the timeline and waited for their time
	overflow    int64 // frames dropped because out was full
	demux       *Demuxer
	out         chan *Frame // demuxed frames on their way to the subscribers
}

// NewHub creates a hub; call Run to start ingest.
func NewHub(layers []LayerConfig, opt Options) *Hub {
	h := &Hub{opt: opt, subs: map[*Subscriber]struct{}{}, audioIn: make(chan audioChunk, 256)}
	if opt.FFmpeg == "" {
		h.opt.FFmpeg = "ffmpeg"
	}
	if opt.AudioLayer >= len(layers) || opt.AudioBitrate <= 0 {
		h.opt.AudioLayer = -1
	}
	for i, c := range layers {
		h.layers = append(h.layers, &layer{idx: i, cfg: c, frameDur: 3000, out: make(chan *Frame, 512)})
	}
	return h
}

// Layers returns the layer configs, highest bitrate first.
func (h *Hub) Layers() []LayerConfig {
	out := make([]LayerConfig, len(h.layers))
	for i, l := range h.layers {
		out[i] = l.cfg
	}
	return out
}

// HasAudio reports whether the hub produces Opus.
func (h *Hub) HasAudio() bool { return h.opt.AudioLayer >= 0 }

// AudioBitrate is the Opus bitrate (0 without audio).
func (h *Hub) AudioBitrate() int {
	if !h.HasAudio() {
		return 0
	}
	return h.opt.AudioBitrate
}

// Subscribe registers a new receiver of all layers and audio.
func (h *Hub) Subscribe() *Subscriber {
	s := &Subscriber{Video: make(chan *Frame, 512), Audio: make(chan *AudioSample, 256)}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

// Unsubscribe stops delivery to s.
func (h *Hub) Unsubscribe(s *Subscriber) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
}

// Subscribers returns the number of active subscribers.
func (h *Hub) Subscribers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}

// Listen binds every layer's UDP address. Run then reads from them.
func (h *Hub) Listen() ([]*net.UDPConn, error) {
	conns := make([]*net.UDPConn, len(h.layers))
	for i, l := range h.layers {
		addr, err := net.ResolveUDPAddr("udp", l.cfg.Addr)
		if err == nil {
			conns[i], err = net.ListenUDP("udp", addr)
		}
		if err != nil {
			for _, o := range conns[:i] {
				o.Close()
			}
			return nil, fmt.Errorf("simulcast layer %s: listen %s: %w", l.cfg.Name, l.cfg.Addr, err)
		}
		_ = conns[i].SetReadBuffer(8 << 20)
	}
	return conns, nil
}

// Run ingests from the sockets Listen returned until ctx is done.
func (h *Hub) Run(ctx context.Context, conns []*net.UDPConn) {
	go func() {
		<-ctx.Done()
		for _, c := range conns {
			c.Close()
		}
	}()
	if h.opt.AudioLayer >= 0 {
		go h.audioLoop(ctx)
	}
	var wg sync.WaitGroup
	for i, l := range h.layers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.ingest(ctx, l, conns[i])
		}()
		go h.deliver(ctx, l)
	}
	wg.Wait()
}

// deliver hands a layer's frames to the subscribers, none before its capture
// time. A frame cannot be ahead of the timeline unless the transcoder sent a
// run of them faster than real time (ffmpeg does where a looped file wraps
// around, and after any stall). Passed on as they came, such a run reaches the
// browser as frames that arrived "before they were captured", which its jitter
// buffer takes as the new normal and then pays for when it is over. Held back
// to their own timestamps they are simply on time. Reading from the socket is
// not held up: this is a different goroutine from ingest.
func (h *Hub) deliver(ctx context.Context, l *layer) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		var f *Frame
		select {
		case <-ctx.Done():
			return
		case f = <-l.out:
		}
		if d := time.Until(f.Capture); d > time.Millisecond {
			l.mu.Lock()
			l.held++
			l.mu.Unlock()
			timer.Reset(min(d, timelineJump))
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
		h.mu.RLock()
		for s := range h.subs {
			select {
			case s.Video <- f:
			default:
				s.Dropped.Add(1)
				s.Overflow.Store(true)
			}
		}
		h.mu.RUnlock()
	}
}

func (h *Hub) ingest(ctx context.Context, l *layer, conn *net.UDPConn) {
	l.demux = NewDemuxer(func(p PES) { h.onPES(l, p, time.Now()) })
	buf := make([]byte, 65536)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("simulcast %s: read: %v", l.cfg.Name, err)
			}
			return
		}
		for off := 0; off+tsPacketSize <= n; off += tsPacketSize {
			_ = l.demux.Feed(buf[off : off+tsPacketSize])
		}
		l.mu.Lock()
		l.datagrams++
		l.ccErrors, l.droppedPES = l.demux.CCErrors, l.demux.DroppedPES
		l.mu.Unlock()
	}
}

func (h *Hub) onPES(l *layer, p PES, now time.Time) {
	switch p.StreamType {
	case streamTypeAAC:
		if l.idx == h.opt.AudioLayer && p.PTS >= 0 && len(p.Data) > 0 {
			select {
			case h.audioIn <- audioChunk{append([]byte(nil), p.Data...), p.PTS}:
			default: // transcoder stalled; drop rather than block video ingest
				h.audioDropped.Add(1)
			}
		}
		return
	case streamTypeH264:
	default:
		return
	}
	if p.PTS < 0 || len(p.Data) == 0 {
		return
	}
	data, info := packAU(p.Data, true)
	if len(data) == 0 {
		return
	}
	f := &Frame{Layer: l.idx, Data: data, Key: info.key, Capture: h.tl.observe(p.PTS, now), Arrival: now}

	l.mu.Lock()
	pts := p.PTS + l.ptsBase
	if l.havePTS {
		switch d := pts - l.lastPTS; {
		case d < -(1 << 32): // 33-bit wrap
			l.ptsBase += 1 << 33
			pts += 1 << 33
		case d > 0 && d < 90000:
			l.frameDur = d
		}
	}
	l.lastPTS, l.havePTS = pts, true
	f.PTS = pts
	l.frames++
	l.mediaBytes += int64(info.mediaBytes)
	l.lastArrival = now
	l.win.add(now, info.mediaBytes)
	if info.key {
		l.keyframes++
		l.lastKey = now
	}
	if info.sps != nil {
		if s, err := ParseSPS(info.sps); err == nil {
			l.sps = s
		}
	}
	l.mu.Unlock()

	select {
	case l.out <- f:
	default:
		// Nothing has taken frames for a long time. Every subscriber then misses
		// this one, and is told so.
		l.mu.Lock()
		l.overflow++
		l.mu.Unlock()
		h.mu.RLock()
		for s := range h.subs {
			s.Dropped.Add(1)
			s.Overflow.Store(true)
		}
		h.mu.RUnlock()
	}
}

// LayerStatus is a snapshot of one layer.
type LayerStatus struct {
	Name          string  `json:"name"`
	NominalBps    int     `json:"nominalBps"`
	MeasuredBps   float64 `json:"measuredBps"` // last ~1 s, forwarded bytes (no AUD/filler)
	Width         int     `json:"width"`
	Height        int     `json:"height"`
	Profile       int     `json:"profile"`
	Level         int     `json:"level"`
	FPS           float64 `json:"fps"`
	Frames        int64   `json:"frames"`
	Keyframes     int64   `json:"keyframes"`
	Datagrams     int64   `json:"datagrams"`
	CCErrors      int64   `json:"ccErrors"`
	DroppedPES    int64   `json:"droppedPes"`
	Held          int64   `json:"held"` // frames that arrived ahead of the timeline and were delivered at their capture time
	MsSinceFrame  any     `json:"msSinceFrame"`
	MsSinceKey    any     `json:"msSinceKeyframe"`
	Live          bool    `json:"live"` // a frame arrived in the last second
	LastPTS       int64   `json:"lastPts"`
	FrameDuration int64   `json:"frameDuration90k"`
}

// Status returns a snapshot of every layer, highest bitrate first.
func (h *Hub) Status() []LayerStatus {
	now := time.Now()
	out := make([]LayerStatus, len(h.layers))
	for i, l := range h.layers {
		l.mu.Lock()
		st := LayerStatus{
			Name: l.cfg.Name, NominalBps: l.cfg.BitrateBps, MeasuredBps: l.win.rate(now),
			Width: l.sps.Width, Height: l.sps.Height, Profile: l.sps.Profile, Level: l.sps.Level,
			Frames: l.frames, Keyframes: l.keyframes, Datagrams: l.datagrams,
			MsSinceFrame: sinceMs(now, l.lastArrival), MsSinceKey: sinceMs(now, l.lastKey),
			Live:    !l.lastArrival.IsZero() && now.Sub(l.lastArrival) < time.Second,
			LastPTS: l.lastPTS, FrameDuration: l.frameDur, CCErrors: l.ccErrors, DroppedPES: l.droppedPES, Held: l.held,
		}
		if l.frameDur > 0 {
			st.FPS = 90000 / float64(l.frameDur)
		}
		l.mu.Unlock()
		out[i] = st
	}
	return out
}

// Snapshot is the hub section of snapshots and /info.
func (h *Hub) Snapshot() map[string]any {
	reanchors, slew := h.tl.stats()
	return map[string]any{
		"layers":            h.Status(),
		"subscribers":       h.Subscribers(),
		"timelineReanchors": reanchors,
		"timelineSlewMs":    float64(slew.Microseconds()) / 1000,
		"audio":             h.HasAudio(),
		"audioLive":         h.audioLive.Load(),
		"audioPages":        h.audioPages.Load(),
		"audioRestarts":     h.audioRestarts.Load(),
		"audioDropped":      h.audioDropped.Load(),
		"audioHeld":         h.audioHeld.Load(),
	}
}

func sinceMs(now, t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return float64(now.Sub(t).Microseconds()) / 1000
}

// rateWindow measures a byte rate over the last second in 100 ms buckets.
type rateWindow struct {
	buckets [10]int
	stamps  [10]int64 // bucket start (unix 100 ms units)
}

func (w *rateWindow) add(now time.Time, n int) {
	slot := now.UnixMilli() / 100
	i := slot % 10
	if w.stamps[i] != slot {
		w.stamps[i], w.buckets[i] = slot, 0
	}
	w.buckets[i] += n
}

// rate returns bits per second over the last full second.
func (w *rateWindow) rate(now time.Time) float64 {
	slot := now.UnixMilli() / 100
	total := 0
	for i := range w.buckets {
		if age := slot - w.stamps[i]; age >= 1 && age <= 10 {
			total += w.buckets[i]
		}
	}
	return float64(total * 8)
}

// ---------------------------------------------------------------------------
// Audio: the AAC of one layer through one shared ffmpeg, out as Opus.
//
// ffmpeg is given bare ADTS on a pipe and returns Ogg pages, so the PTS does
// not travel through it. It does not need to: the transcoder's output position
// counts samples from the first one it was given, so an Opus packet's PTS is
// the PTS of the first AAC frame fed to this ffmpeg plus its position. That
// only holds while the input is gapless, so the feeder checks every PES
// against the samples it has fed, and a gap (lost datagrams, a source that
// restarted) ends this ffmpeg; the next one starts a fresh count.
// ---------------------------------------------------------------------------

var errAudioGap = errors.New("AAC timestamps jumped")

// audioLoop keeps a transcoder running, restarting it if it exits.
func (h *Hub) audioLoop(ctx context.Context) {
	for ctx.Err() == nil {
		err := h.runAudioTranscoder(ctx)
		h.audioLive.Store(false)
		if ctx.Err() != nil {
			return
		}
		h.audioRestarts.Add(1)
		log.Printf("simulcast audio transcoder stopped: %v (restarting)", err)
		wait := time.Second
		if errors.Is(err, errAudioGap) {
			wait = 0
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (h *Hub) runAudioTranscoder(ctx context.Context) error {
	// What queued up while no transcoder ran is not contiguous with what comes next.
	for drained := false; !drained; {
		select {
		case <-h.audioIn:
		default:
			drained = true
		}
	}
	cctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	cmd := exec.CommandContext(cctx, h.opt.FFmpeg, "-hide_banner", "-loglevel", "error",
		"-fflags", "nobuffer", "-probesize", "2048", "-analyzeduration", "0",
		"-f", "aac", "-i", "pipe:0",
		"-c:a", "libopus", "-b:a", strconv.Itoa(h.opt.AudioBitrate), "-ar", strconv.Itoa(AudioRate), "-ac", "2",
		"-frame_duration", "20", "-application", "lowdelay",
		"-page_duration", "1", "-flush_packets", "1", "-f", "ogg", "pipe:1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.WaitDelay = time.Second
	if err := cmd.Start(); err != nil {
		return err
	}
	var firstPTS atomic.Int64 // PTS of the first sample fed; -1 until then
	firstPTS.Store(-1)
	go func() {
		defer stdin.Close()
		var fed float64 // 90 kHz ticks of audio written so far
		track := true   // false once a PES could not be measured
		for {
			select {
			case <-cctx.Done():
				return
			case c := <-h.audioIn:
				first := firstPTS.Load()
				if first < 0 {
					first = c.pts
					firstPTS.Store(first)
				} else if d := ptsDiff(c.pts, first+int64(fed)); track && (d > 4500 || d < -4500) {
					cancel(fmt.Errorf("%w by %d ms", errAudioGap, d/90))
					return
				}
				if d := adtsDuration(c.data); d > 0 {
					fed += d
				} else {
					track = false
				}
				if _, err := stdin.Write(c.data); err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	readErr := h.readOpus(cctx, stdout, &firstPTS)
	if cause := context.Cause(cctx); cause != nil && ctx.Err() == nil {
		readErr = cause
	}
	cancel(nil)
	waitErr := cmd.Wait()
	if errors.Is(readErr, errAudioGap) {
		return readErr
	}
	if msg := strings.TrimSpace(stderr.String()); msg != "" {
		return fmt.Errorf("%v / %v: %s", readErr, waitErr, msg)
	}
	if readErr != nil {
		return readErr
	}
	return waitErr
}

func (h *Hub) readOpus(ctx context.Context, r io.Reader, firstPTS *atomic.Int64) error {
	ogg, hdr, err := oggreader.NewWith(r)
	if err != nil {
		return fmt.Errorf("ogg header: %w", err)
	}
	const frameSamples = uint64(AudioRate * AudioFrame / time.Second)
	preSkip := int64(hdr.PreSkip)
	var granule uint64 // samples decoded before this packet, pre-skip included
	for {
		page, ph, err := ogg.ParseNextPage()
		if err != nil {
			return err
		}
		if bytes.HasPrefix(page, []byte("OpusTags")) {
			continue
		}
		before := granule
		granule = ph.GranulePosition
		first := firstPTS.Load()
		if granule != before+frameSamples || first < 0 {
			h.audioDropped.Add(1) // not one 20 ms packet per page: it cannot be timed
			continue
		}
		// 48 kHz samples to 90 kHz ticks is ×15/8.
		at, ok := h.tl.wallOf(first + (int64(before)-preSkip)*15/8)
		now := time.Now()
		if !ok || at.After(now.Add(timelineJump)) || at.Before(now.Add(-3*timelineJump)) {
			h.audioDropped.Add(1) // from before the video timeline was anchored again
			continue
		}
		// As for video (deliver): nothing leaves before its capture time. The
		// pipe from ffmpeg holds far more than the second this can wait.
		if d := at.Sub(now); d > time.Millisecond {
			h.audioHeld.Add(1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
			}
		}
		h.audioPages.Add(1)
		h.audioLive.Store(true)
		a := &AudioSample{Data: page, Capture: at}
		h.mu.RLock()
		for s := range h.subs {
			select {
			case s.Audio <- a:
			default:
			}
		}
		h.mu.RUnlock()
	}
}

var adtsRates = [16]int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}

// adtsDuration returns how much audio a run of whole ADTS frames holds, in
// 90 kHz ticks, or 0 if b is not that.
func adtsDuration(b []byte) float64 {
	var ticks float64
	for len(b) > 0 {
		if len(b) < 7 || b[0] != 0xff || b[1]&0xf0 != 0xf0 {
			return 0
		}
		rate := adtsRates[b[2]>>2&0x0f]
		size := int(b[3]&3)<<11 | int(b[4])<<3 | int(b[5])>>5
		if rate == 0 || size < 7 || size > len(b) {
			return 0
		}
		ticks += float64(1024*(int(b[6]&3)+1)) * 90000 / float64(rate)
		b = b[size:]
	}
	return ticks
}
