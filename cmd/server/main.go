// Command server streams a pre-recorded video to browsers over a WebRTC
// DataChannel instead of an RTP media track: the server packetises, paces and
// retransmits frames itself, and the page reassembles, buffers and decodes
// them (WebCodecs). See README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io/fs"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"

	"puffer-datachannels/internal/copa"
	"puffer-datachannels/internal/media"
	"puffer-datachannels/internal/ratecontrol"
	"puffer-datachannels/internal/session"
	"puffer-datachannels/internal/x264"
	"puffer-datachannels/web"
)

// Every flag can also be set by an environment variable: --foo-bar => PUFFER_FOO_BAR.
func envName(flagName string) string {
	return "PUFFER_" + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

func main() {
	var (
		httpAddr     = flag.String("http-addr", ":8080", "HTTP(S) listen address")
		tlsCert      = flag.String("tls-cert", "", "TLS certificate file (enables HTTPS; WebCodecs needs HTTPS anywhere but localhost)")
		tlsKey       = flag.String("tls-key", "", "TLS key file")
		videoPath    = flag.String("video", "media/standin.mp4", "source video file (looped)")
		publicIP     = flag.String("public-ip", "", "public IP(s) to advertise in ICE host candidates, comma-separated")
		udpPort      = flag.Int("udp-port", 50000, "single UDP port for all sessions (0 = ephemeral ports)")
		udpBind      = flag.String("udp-bind", "", "IP address to bind the UDP port to (default: all interfaces)")
		stunURLs     = flag.String("stun", "", "STUN server URL(s), comma-separated")
		cc           = flag.String("cc", "fixed", "rate controller: "+strings.Join(ratecontrol.Names(), ", "))
		bitrate      = flag.Int("bitrate", 1_500_000, "bitrate (bps): what the fixed controller sends at, and where any other starts")
		minBitrate   = flag.Int("min-bitrate", 100_000, "lowest bitrate a controller or a viewer may choose (bps)")
		maxBitrate   = flag.Int("max-bitrate", 8_000_000, "highest bitrate a controller or a viewer may choose (bps)")
		pacingFactor = flag.Float64("pacing-factor", 2.0, "pacer drain rate as a multiple of the bitrate")
		headroom     = flag.Float64("headroom", 0.9, "fraction of the bitrate given to audio and the video encoder (the rest covers headers and retransmissions)")
		audio        = flag.Bool("audio", true, "send the source's audio (Opus)")
		audioBitrate = flag.Int("audio-bitrate", 64_000, "Opus bitrate (bps); it comes out of --bitrate")
		keyintSec    = flag.Float64("keyint", 0, "seconds between periodic keyframes (0 = only when the browser asks)")
		preset       = flag.String("x264-preset", "veryfast", "x264 preset")
		threads      = flag.Int("x264-threads", 0, "x264 threads (0 = auto)")
		maxQueue     = flag.Duration("max-queue", 500*time.Millisecond, "drop a new frame (and send a keyframe next) when the pacer queue is longer than this")
		history      = flag.Duration("rtx-history", 2*time.Second, "how long sent frames stay available for NACK retransmission")
		fbInterval   = flag.Duration("feedback-interval", 50*time.Millisecond, "how often the browser reports packet arrivals")
		sctpMinCwnd  = flag.Uint("sctp-min-cwnd", 64<<20, "floor for SCTP's congestion window in bytes; large disables SCTP congestion control, 0 leaves it on")
		emulateLoss  = flag.Float64("emulate-loss", 0, "testing: drop this fraction (0..1) of outgoing media-sized UDP packets")
		emulateDelay = flag.Duration("emulate-delay", 0, "testing: hold every outgoing UDP packet this long")
		snapInt      = flag.Duration("snapshot-interval", 250*time.Millisecond, "server snapshot period (snapshots.jsonl and the page)")
		logPackets   = flag.Bool("log-packets", true, "log every sent packet and every feedback to cc.jsonl")
		logDir       = flag.String("log-dir", "logs", "root directory for per-session log directories")
		maxSessions  = flag.Int("max-sessions", 8, "maximum concurrent sessions (a viewer is one ffmpeg decode and one x264 encode)")

		ccAlgorithm  = flag.String("cc-algorithm", "copa", "cc page: congestion controller when the page does not choose: "+strings.Join(session.Algorithms(), ", "))
		ccMaxRate    = flag.Int("cc-max-rate", 20_000_000, "cc page: the most a session may send (bps); the page can ask for less")
		ccMaxDur     = flag.Duration("cc-max-duration", 10*time.Minute, "cc page: close a session after this long (0 = never)")
		copaMode     = flag.String("copa-mode", "default", "cc page, Copa: default (δ fixed), auto (the paper's mode switching) or competitive")
		copaDelta    = flag.Float64("copa-delta", 0.5, "cc page, Copa: δ in default mode; Copa keeps about 1/δ packets queued")
		emulateRate  = flag.Int("emulate-rate", 0, "testing: a bottleneck of this many bps for everything the server sends, all sessions together")
		emulateQueue = flag.Duration("emulate-queue", 200*time.Millisecond, "testing: the --emulate-rate bottleneck's drop-tail buffer, as time to drain it")
	)
	flag.VisitAll(func(f *flag.Flag) {
		if v, ok := os.LookupEnv(envName(f.Name)); ok {
			if err := f.Value.Set(v); err != nil {
				log.Fatalf("env %s: %v", envName(f.Name), err)
			}
		}
	})
	flag.Parse()
	if _, err := ratecontrol.New(*cc, ratecontrol.Config{}); err != nil {
		log.Fatal(err)
	}
	if !copa.ValidMode(copa.Mode(*copaMode)) || *copaDelta < 0.01 || *copaDelta > 2 {
		log.Fatal("--copa-mode must be default, auto or competitive, and --copa-delta within 0.01..2")
	}
	if !session.ValidAlgorithm(*ccAlgorithm) || *ccMaxRate < *minBitrate {
		log.Fatalf("--cc-algorithm must be one of %v, and --cc-max-rate at least --min-bitrate", session.Algorithms())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	info, err := media.Probe(ctx, *videoPath)
	if err != nil {
		log.Fatalf("probe video: %v (generate a stand-in with `make standin`)", err)
	}
	log.Printf("source %s: %dx%d @ %.3f fps, %.1fs, audio: %v", info.Path, info.Width, info.Height, info.FPS, info.DurationSec, info.HasAudio)

	se := webrtc.SettingEngine{}
	session.Settings(&se, uint32(*sctpMinCwnd))
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	if *udpPort > 0 {
		bindIP := net.ParseIP(*udpBind)
		if bindIP != nil && bindIP.IsLoopback() {
			se.SetIncludeLoopbackCandidate(true)
		}
		udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: bindIP, Port: *udpPort})
		if err != nil {
			log.Fatalf("listen udp %d: %v", *udpPort, err)
		}
		var conn net.PacketConn = udp
		if *emulateLoss > 0 || *emulateDelay > 0 || *emulateRate > 0 {
			conn = newImpairedConn(udp, *emulateLoss, *emulateDelay, *emulateRate, *emulateQueue)
			log.Printf("TESTING: outgoing packets delayed %v, %.1f%% of media packets dropped", *emulateDelay, *emulateLoss*100)
			if *emulateRate > 0 {
				log.Printf("TESTING: outgoing packets through a %d bps bottleneck with a %v buffer", *emulateRate, *emulateQueue)
			}
		}
		se.SetICEUDPMux(webrtc.NewICEUDPMux(nil, conn))
		log.Printf("WebRTC on UDP port %d", *udpPort)
	} else if *emulateLoss > 0 || *emulateDelay > 0 || *emulateRate > 0 {
		log.Fatal("--emulate-loss, --emulate-delay and --emulate-rate need --udp-port")
	}
	if *publicIP != "" {
		if err := se.SetICEAddressRewriteRules(webrtc.ICEAddressRewriteRule{
			External:        splitList(*publicIP),
			AsCandidateType: webrtc.ICECandidateTypeHost,
			Mode:            webrtc.ICEAddressRewriteReplace,
		}); err != nil {
			log.Fatalf("public ip: %v", err)
		}
	}
	var iceServers []webrtc.ICEServer
	if *stunURLs != "" {
		iceServers = append(iceServers, webrtc.ICEServer{URLs: splitList(*stunURLs)})
	}

	cfg := session.Config{
		Controller: *cc, StartBitrate: *bitrate, MinBitrate: *minBitrate, MaxBitrate: *maxBitrate,
		PacingFactor: *pacingFactor, Headroom: *headroom, Audio: *audio, AudioBitrate: *audioBitrate, KeyIntSec: *keyintSec,
		X264Preset: *preset, X264Threads: *threads, MaxQueue: *maxQueue, History: *history,
		FeedbackInterval: *fbInterval, SnapshotInterval: *snapInt, LogPackets: *logPackets,
		LogDir: *logDir, ConnectTimeout: 30 * time.Second, MaxSessions: *maxSessions, ICEServers: iceServers,
		CCAlgorithm: *ccAlgorithm, CCMaxRate: *ccMaxRate, CCMaxDuration: *ccMaxDur, CopaMode: *copaMode, CopaDelta: *copaDelta,
	}
	hostname, _ := os.Hostname()
	mgr := session.NewManager(cfg, info, se, map[string]any{
		"hostname": hostname, "numCPU": runtime.NumCPU(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"goVersion": runtime.Version(), "x264Build": x264.Build(), "udpPort": *udpPort,
		"sctpMinCwnd": *sctpMinCwnd, "emulateLoss": *emulateLoss, "emulateDelayMs": emulateDelay.Milliseconds(),
		"emulateRate": *emulateRate, "emulateQueueMs": emulateQueue.Milliseconds(),
	})

	mux := http.NewServeMux()
	static, _ := fs.Sub(web.FS, ".")
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /info", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"media": info, "config": cfg, "sessions": mgr.ActiveSessions()})
	})
	mux.HandleFunc("POST /offer", func(w http.ResponseWriter, r *http.Request) {
		var req session.OfferRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid offer"})
			return
		}
		resp, err := mgr.HandleOffer(r.Context(), req, r.RemoteAddr)
		writeOfferResponse(w, r, resp, err)
	})
	// The congestion-control page (web/cc.html): junk data under Copa or CUBIC.
	mux.HandleFunc("POST /cc/offer", func(w http.ResponseWriter, r *http.Request) {
		var req session.CCRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid offer"})
			return
		}
		resp, err := mgr.HandleCCOffer(r.Context(), req, r.RemoteAddr)
		writeOfferResponse(w, r, resp, err)
	})

	srv := &http.Server{Addr: *httpAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		log.Printf("shutting down")
		mgr.CloseAll()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Printf("serving on %s, controller %q at %d bps (logs in %s)", *httpAddr, *cc, *bitrate, *logDir)
	if *tlsCert != "" {
		err = srv.ListenAndServeTLS(*tlsCert, *tlsKey)
	} else {
		err = srv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func writeOfferResponse(w http.ResponseWriter, r *http.Request, resp *session.OfferResponse, err error) {
	switch {
	case errors.Is(err, session.ErrTooManySessions):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
	case errors.Is(err, session.ErrBadOffer):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case err != nil:
		log.Printf("offer from %s: %v", r.RemoteAddr, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusOK, resp)
	}
}

// impairedConn stands in for a bad downlink when there is no tc/netem. In
// the order a packet meets them:
//
//   - loss: a fraction of the large packets the server sends are dropped,
//     which in practice means media (ICE checks, the DTLS handshake, SACKs and
//     pongs are smaller);
//   - rate: a bottleneck link. Packets are sent one after another, each taking
//     its size at the link's rate, and wait their turn in a drop-tail buffer
//     that holds queueLimit's worth. The link is shared by every session, so
//     two pages compete for it;
//   - delay: every packet is then held for a fixed time, in order.
type impairedConn struct {
	net.PacketConn
	loss       float64
	delay      time.Duration
	rate       float64 // bytes per second; 0 = no bottleneck
	queueLimit time.Duration
	queue      chan delayedPacket

	mu       sync.Mutex
	linkFree time.Time // when the bottleneck finishes what it has accepted
}

type delayedPacket struct {
	b    []byte
	addr net.Addr
	due  time.Time
}

func newImpairedConn(c net.PacketConn, loss float64, delay time.Duration, rateBps int, queueLimit time.Duration) *impairedConn {
	ic := &impairedConn{PacketConn: c, loss: loss, delay: delay, rate: float64(rateBps) / 8, queueLimit: queueLimit,
		queue: make(chan delayedPacket, 1<<16)}
	if delay > 0 || ic.rate > 0 {
		go func() {
			for p := range ic.queue {
				time.Sleep(time.Until(p.due))
				_, _ = c.WriteTo(p.b, p.addr)
			}
		}()
	}
	return ic
}

func (c *impairedConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if len(b) > 300 && rand.Float64() < c.loss {
		return len(b), nil
	}
	if c.delay == 0 && c.rate == 0 {
		return c.PacketConn.WriteTo(b, addr)
	}
	// The lock is held across the enqueue so that packets enter the channel in
	// the order of their due times.
	c.mu.Lock()
	defer c.mu.Unlock()
	due := time.Now()
	if c.rate > 0 {
		if c.linkFree.Before(due) {
			c.linkFree = due
		}
		if c.linkFree.Sub(due) > c.queueLimit {
			return len(b), nil // buffer full
		}
		c.linkFree = c.linkFree.Add(time.Duration(float64(len(b)) / c.rate * float64(time.Second)))
		due = c.linkFree
	}
	select {
	case c.queue <- delayedPacket{append([]byte(nil), b...), addr, due.Add(c.delay)}:
	default: // queue full: drop
	}
	return len(b), nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
