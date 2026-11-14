// Package logs writes one directory per session, with one JSON-lines file per
// concern (events, rtcp, cc, encoder, snapshots, owd, client) plus meta.json.
//
// Every line is {"t": <server unix ms>, "kind": <string>, "data": <any>} unless a
// stream documents otherwise (client.jsonl adds "tClient").
package logs

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record is one line of a stream.
type Record struct {
	T    float64 `json:"t"`
	Kind string  `json:"kind"`
	Data any     `json:"data"`
}

// NowMs is the timestamp format used throughout the logs: unix milliseconds
// with microsecond resolution.
func NowMs() float64 { return float64(time.Now().UnixMicro()) / 1000.0 }

// Ms converts a time.Time to the log timestamp format.
func Ms(t time.Time) float64 { return float64(t.UnixMicro()) / 1000.0 }

// Session is one session's log directory.
type Session struct {
	Dir string

	mu      sync.Mutex
	streams map[string]*stream
	closed  bool
	done    chan struct{}
}

type stream struct {
	f   *os.File
	w   *bufio.Writer
	enc *json.Encoder
}

// NewSession creates <root>/<UTC time>_<id>/ and starts a periodic flusher.
func NewSession(root, id string) (*Session, error) {
	dir := filepath.Join(root, time.Now().UTC().Format("20060102T150405Z")+"_"+id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Session{Dir: dir, streams: map[string]*stream{}, done: make(chan struct{})}
	go s.flushLoop()
	return s, nil
}

// WriteMeta writes <dir>/meta.json (pretty-printed, overwritten each call).
func (s *Session) WriteMeta(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.Dir, "meta.json"), b, 0o644)
}

// Log appends {"t": now, "kind": kind, "data": data} to <name>.jsonl.
func (s *Session) Log(name, kind string, data any) {
	s.Write(name, Record{T: NowMs(), Kind: kind, Data: data})
}

// LogAt is Log with an explicit event time.
func (s *Session) LogAt(name string, t time.Time, kind string, data any) {
	s.Write(name, Record{T: Ms(t), Kind: kind, Data: data})
}

// Write appends any JSON value as one line of <name>.jsonl.
func (s *Session) Write(name string, rec any) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	st := s.streams[name]
	if st == nil {
		f, err := os.OpenFile(filepath.Join(s.Dir, name+".jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return
		}
		w := bufio.NewWriterSize(f, 256*1024)
		st = &stream{f: f, w: w, enc: json.NewEncoder(w)}
		s.streams[name] = st
	}
	_ = st.enc.Encode(rec)
}

func (s *Session) flushLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			s.Flush()
		}
	}
}

// Flush writes buffered data to disk.
func (s *Session) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.streams {
		_ = st.w.Flush()
	}
}

// Close flushes and closes every stream; later writes are dropped.
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.done)
	for _, st := range s.streams {
		_ = st.w.Flush()
		_ = st.f.Close()
	}
}
