// Package media probes the source file and runs the ffmpeg decode pipeline
// (raw I420 video and Opus audio, looped in real time). Adapted from
// github.com/nathanielCherian/webrtc-test (internal/session/pipeline.go).
package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4/pkg/media/oggreader"
)

// MediaInfo describes the source file, as probed by ffprobe.
type Info struct {
	Path        string  `json:"path"`
	Width       int     `json:"width"`  // even-rounded output width
	Height      int     `json:"height"` // even-rounded output height
	FPSNum      int     `json:"fpsNum"`
	FPSDen      int     `json:"fpsDen"`
	FPS         float64 `json:"fps"`
	DurationSec float64 `json:"durationSec"`
	HasAudio    bool    `json:"hasAudio"`
	VideoCodec  string  `json:"videoCodec"`
	AudioCodec  string  `json:"audioCodec,omitempty"`
}

// Probe inspects a media file with ffprobe, falling back to parsing
// `ffmpeg -i` output when ffprobe is not installed.
func Probe(ctx context.Context, path string) (*Info, error) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		return probeWithFFmpeg(ctx, path)
	}
	out, err := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-print_format", "json",
		"-show_streams", "-show_format", path).Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe %s: %w", path, err)
	}
	var res struct {
		Streams []struct {
			CodecType    string `json:"codec_type"`
			CodecName    string `json:"codec_name"`
			Width        int    `json:"width"`
			Height       int    `json:"height"`
			AvgFrameRate string `json:"avg_frame_rate"`
			RFrameRate   string `json:"r_frame_rate"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		return nil, fmt.Errorf("parse ffprobe output: %w", err)
	}
	info := &Info{Path: path}
	info.DurationSec, _ = strconv.ParseFloat(res.Format.Duration, 64)
	foundVideo := false
	for _, s := range res.Streams {
		switch s.CodecType {
		case "video":
			if foundVideo {
				continue
			}
			foundVideo = true
			info.VideoCodec = s.CodecName
			info.Width = s.Width &^ 1
			info.Height = s.Height &^ 1
			num, den := parseRate(s.AvgFrameRate)
			if num == 0 {
				num, den = parseRate(s.RFrameRate)
			}
			if num == 0 {
				num, den = 30, 1
			}
			info.FPSNum, info.FPSDen = num, den
			info.FPS = float64(num) / float64(den)
		case "audio":
			if !info.HasAudio {
				info.HasAudio = true
				info.AudioCodec = s.CodecName
			}
		}
	}
	if !foundVideo {
		return nil, errors.New("no video stream in " + path)
	}
	return info, nil
}

var (
	reDuration = regexp.MustCompile(`Duration: (\d+):(\d+):(\d+(?:\.\d+)?)`)
	reVideo    = regexp.MustCompile(`Stream #0:\d+.*?: Video: (\w+).*?, (\d{2,5})x(\d{2,5})`)
	reFPS      = regexp.MustCompile(`([\d.]+)(k?) (?:fps|tbr)`)
	reAudio    = regexp.MustCompile(`Stream #0:\d+.*?: Audio: (\w+)`)
)

func probeWithFFmpeg(ctx context.Context, path string) (*Info, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return nil, errors.New("neither ffprobe nor ffmpeg is on PATH (the server needs ffmpeg to decode)")
	}
	// ffmpeg exits non-zero without an output file; the stream summary is on stderr.
	out, _ := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-i", path).CombinedOutput()
	text := string(out)
	info := &Info{Path: path}
	if m := reDuration.FindStringSubmatch(text); m != nil {
		h, _ := strconv.ParseFloat(m[1], 64)
		mi, _ := strconv.ParseFloat(m[2], 64)
		sec, _ := strconv.ParseFloat(m[3], 64)
		info.DurationSec = h*3600 + mi*60 + sec
	}
	var videoLine string
	for _, line := range strings.Split(text, "\n") {
		if videoLine == "" && reVideo.MatchString(line) {
			videoLine = line
		}
		if m := reAudio.FindStringSubmatch(line); m != nil && !info.HasAudio {
			info.HasAudio, info.AudioCodec = true, m[1]
		}
	}
	m := reVideo.FindStringSubmatch(videoLine)
	if m == nil {
		return nil, fmt.Errorf("no video stream in %s: %s", path, strings.TrimSpace(text))
	}
	info.VideoCodec = m[1]
	w, _ := strconv.Atoi(m[2])
	h, _ := strconv.Atoi(m[3])
	info.Width, info.Height = w&^1, h&^1
	info.FPSNum, info.FPSDen = 30, 1
	if f := reFPS.FindStringSubmatch(videoLine); f != nil {
		fps, _ := strconv.ParseFloat(f[1], 64)
		if f[2] == "k" {
			fps *= 1000
		}
		if fps > 0 && fps <= 240 {
			// Represent common NTSC rates exactly (29.97 -> 30000/1001).
			if r := math.Round(fps); math.Abs(fps-r*1000/1001) < 0.01 && math.Abs(fps-r) > 0.01 {
				info.FPSNum, info.FPSDen = int(r)*1000, 1001
			} else {
				info.FPSNum, info.FPSDen = int(math.Round(fps*1000)), 1000
			}
		}
	}
	info.FPS = float64(info.FPSNum) / float64(info.FPSDen)
	return info, nil
}

func parseRate(s string) (int, int) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		return 0, 0
	}
	n, err1 := strconv.Atoi(parts[0])
	d, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || n <= 0 || d <= 0 {
		return 0, 0
	}
	return n, d
}

// Pipeline runs one ffmpeg process that decodes the source (looping forever,
// in real time) into raw I420 video on stdout and, if asked, Opus audio (one
// 20 ms packet per Ogg page) on fd 3.
type Pipeline struct {
	info   *Info
	cmd    *exec.Cmd
	video  io.ReadCloser
	audio  *os.File // nil without audio
	stderr *tailBuffer

	FramesRead atomic.Int64
}

// AudioFrame is the duration of one Opus packet; AudioRate its sample rate.
const (
	AudioFrame = 20 * time.Millisecond
	AudioRate  = 48000
)

// StartPipeline launches ffmpeg. audioBitrate 0 means video only.
func StartPipeline(ctx context.Context, info *Info, audioBitrate int) (*Pipeline, error) {
	vf := fmt.Sprintf("scale=%d:%d,fps=%d/%d,format=yuv420p", info.Width, info.Height, info.FPSNum, info.FPSDen)
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin",
		"-re", "-stream_loop", "-1", "-i", info.Path,
		"-map", "0:v:0", "-vf", vf, "-f", "rawvideo", "-pix_fmt", "yuv420p", "pipe:1"}
	if audioBitrate > 0 {
		// aresample=async keeps the sample count in step with the timestamps (it
		// pads or trims where the file loops), so audio position stays comparable
		// with the video frame count for as long as the session runs.
		args = append(args, "-map", "0:a:0", "-af", "aresample=async=1:first_pts=0",
			"-c:a", "libopus", "-b:a", strconv.Itoa(audioBitrate), "-ar", strconv.Itoa(AudioRate), "-ac", "2",
			"-frame_duration", "20", "-application", "lowdelay",
			"-page_duration", "1", "-flush_packets", "1", "-f", "ogg", "pipe:3")
	}
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	video, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	p := &Pipeline{info: info, cmd: cmd, video: video, stderr: &tailBuffer{max: 4096}}
	cmd.Stderr = p.stderr
	cmd.WaitDelay = 2 * time.Second
	var aw *os.File
	if audioBitrate > 0 {
		if p.audio, aw, err = os.Pipe(); err != nil {
			return nil, err
		}
		cmd.ExtraFiles = []*os.File{aw} // fd 3 in the child
	}
	err = cmd.Start()
	if aw != nil {
		aw.Close() // the child has its own copy
	}
	if err != nil {
		if p.audio != nil {
			p.audio.Close()
		}
		return nil, fmt.Errorf("start ffmpeg: %w", err)
	}
	return p, nil
}

// AudioReader returns the Opus stream's Ogg pages and its pre-skip: the
// number of samples at the start of the decoded stream that precede media
// time zero.
func (p *Pipeline) AudioReader() (*oggreader.OggReader, int, error) {
	r, hdr, err := oggreader.NewWith(p.audio)
	if err != nil {
		return nil, 0, p.wrapErr(err)
	}
	return r, int(hdr.PreSkip), nil
}

func (p *Pipeline) wrapErr(err error) error {
	if msg := strings.TrimSpace(p.stderr.String()); msg != "" {
		return fmt.Errorf("%w (ffmpeg: %s)", err, msg)
	}
	return err
}

// ReadVideoFrame reads exactly one I420 frame into buf.
func (p *Pipeline) ReadVideoFrame(buf []byte) error {
	if _, err := io.ReadFull(p.video, buf); err != nil {
		return p.wrapErr(err)
	}
	p.FramesRead.Add(1)
	return nil
}

// Close kills ffmpeg.
func (p *Pipeline) Close() {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	_ = p.cmd.Wait()
	if p.audio != nil {
		p.audio.Close()
	}
}

// tailBuffer keeps the last max bytes written (ffmpeg stderr).
type tailBuffer struct {
	mu  sync.Mutex
	b   []byte
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.b = append(t.b, p...)
	if len(t.b) > t.max {
		t.b = t.b[len(t.b)-t.max:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.b)
}
