// Package bbr is BBR version 3 as specified in draft-ietf-ccwg-bbr-07
// (Cardwell, Swett, Beshay; November 2026). Function names below are the
// draft's, so the two can be read side by side.
//
// BBR is model based. From every acknowledgement it takes a delivery-rate
// sample (§4.1: how much was delivered between this packet leaving and its
// ack arriving, over how long that took) and an RTT sample, and keeps
//
//	max_bw    the largest delivery rate of the last two ProbeBW cycles
//	min_rtt   the smallest RTT of the last 10 s
//
// whose product is the path's BDP. It paces at pacing_gain · bw and keeps at
// most cwnd_gain · BDP in flight, and a state machine moves the gains:
//
//	Startup         2.77 / 2     until the delivery rate stops growing, or loss is high
//	Drain           0.5  / 2     until what is in flight is down to the BDP
//	ProbeBW_DOWN    0.9  / 2     leave headroom, then
//	ProbeBW_CRUISE  1.0  / 2     for 2-3 s (or sooner: a Reno flow's time to recover)
//	ProbeBW_REFILL  1.0  / 2     one round, then
//	ProbeBW_UP      1.25 / 2.25  until the rate plateaus or loss is too high
//	ProbeRTT        1.0  / 0.5   200 ms, when min_rtt has not been refreshed for 10 s
//
// Loss is a signal too, which is what version 3 adds to version 1: more than
// 2% lost while probing sets inflight_longterm, a ceiling on what the path is
// trusted to hold, and a round with any loss outside a probe pulls the
// short-term bounds (bw_shortterm, inflight_shortterm) down towards what was
// just delivered, by no more than β = 0.7 a round.
//
// Like internal/copa and internal/cubic the package only does arithmetic on
// what it is told: no clock, no timers, no locks. Unlike them it must be told
// of every packet sent (OnSend), because a delivery-rate sample needs to know
// how much had been delivered when the packet left. Windows are in packets
// and rates in packets per second.
//
// Left out: the send quantum and offload budget (every packet here is one
// datagram), restarting from idle (the sender always has data), ECN, and the
// undo of a spurious loss recovery (§5.5.11.2). The draft counts loss events
// (runs of lost sequence numbers) for leaving Startup on loss; this counts
// lost packets, which is the same thing when losses are scattered and leaves
// a little sooner when they come in bursts.
package bbr

import (
	"math"
	"math/rand"
	"sort"
	"time"
)

// The draft's constants.
const (
	startupPacingGain   = 2.772588722239781 // 4 ln 2
	defaultCwndGain     = 2.0
	drainPacingGain     = 0.5
	probeDownPacingGain = 0.9
	probeUpPacingGain   = 1.25
	probeUpCwndGain     = 2.25
	probeRTTCwndGain    = 0.5
	pacingMargin        = 0.01
	lossThresh          = 0.02
	beta                = 0.7
	headroom            = 0.15
	minPipeCwnd         = 4.0
	startupFullLossCnt  = 6
	extraAckedFilterLen = 10 // rounds
	minRTTFilterLen     = 10 * time.Second
	probeRTTDuration    = 200 * time.Millisecond
)

// State is where the state machine is.
type State string

const (
	Startup       State = "Startup"
	Drain         State = "Drain"
	ProbeBWDown   State = "ProbeBW_DOWN"
	ProbeBWCruise State = "ProbeBW_CRUISE"
	ProbeBWRefill State = "ProbeBW_REFILL"
	ProbeBWUp     State = "ProbeBW_UP"
	ProbeRTT      State = "ProbeRTT"
)

// Config holds what is not fixed by the draft. Zero fields take the defaults
// in brackets.
type Config struct {
	InitCwnd float64 // packets [10]
	MaxCwnd  float64 // packets [1e6]
	// MaxRate caps the pacing rate in packets per second (0 = no cap),
	// standing in for an application with only so much to send. Samples taken
	// while the cap, not the path, was the limit are marked application
	// limited, so they can raise the bandwidth estimate but not lower it.
	MaxRate float64
	// Rand returns a number in [0, 1): how long to cruise before the next
	// probe is random, so that flows do not probe in step. [math/rand]
	Rand func() float64
}

func (c Config) withDefaults() Config {
	if c.InitCwnd <= 0 {
		c.InitCwnd = 10
	}
	if c.MaxCwnd <= 0 {
		c.MaxCwnd = 1e6
	}
	if c.Rand == nil {
		c.Rand = rand.Float64
	}
	return c
}

// sendRec is what the connection knew when a packet left (§4.1.2.2).
type sendRec struct {
	at            time.Time
	firstSendTime time.Time // send time of the packet most recently delivered before this one left
	deliveredTime time.Time // when that delivery was acknowledged
	delivered     int64     // packets delivered so far
	lost          int64     // packets lost so far
	txInFlight    int       // in flight, this packet included
	appLimited    bool
	done          bool // acknowledged or lost
}

// rateSample is the draft's RS: what one acknowledgement says.
type rateSample struct {
	deliveryRate   float64 // packets per second; 0 = no reliable sample
	delivered      float64 // packets delivered while this one was in flight
	priorDelivered int64
	txInFlight     float64
	lost           float64 // lost since this packet left
	isAppLimited   bool
	rtt            time.Duration
}

// BBR is one flow's controller.
type BBR struct {
	cfg Config

	// The connection (the draft's C).
	cwnd          float64
	pacingRate    float64 // before MaxRate
	inflight      int
	delivered     int64
	lost          int64
	deliveredTime time.Time
	firstSendTime time.Time
	appLimited    int64 // 0, or the delivered count that ends the app-limited phase
	cwndLimited   bool
	srtt, latest  time.Duration
	sends         []sendRec // every packet not yet acknowledged or lost, in send order
	head          int

	inRecovery    bool
	recoveryStart time.Time
	priorCwnd     float64

	state      State
	pacingGain float64
	cwndGain   float64

	// Round counting (§5.5.1).
	nextRoundDelivered int64
	roundStart         bool
	roundCount         int64

	// The model.
	maxBwFilter       [2]float64 // the previous ProbeBW cycle and this one
	maxBw, bw         float64
	bwShortterm       float64 // +Inf when not set
	inflightShortterm float64 // +Inf when not set
	inflightLongterm  float64 // +Inf until loss says otherwise
	bdp, maxInflight  float64

	minRtt        time.Duration // 0 = no sample yet
	minRttStamp   time.Time
	minRttExpired bool

	extraAcked              float64
	extraAckedFilter        [2]float64
	extraAckedRounds        int
	extraAckedIntervalStart time.Time
	extraAckedDelivered     float64

	// Full pipe detection (§5.3.1.2).
	fullBw        float64
	fullBwCount   int
	fullBwNow     bool
	fullBwReached bool
	drainStart    int64

	// Congestion signals of the current round (§5.5.10).
	bwLatest, inflightLatest float64
	lossRoundDelivered       int64
	lossRoundStart           bool
	lossInRound              bool
	lossEventsInRound        int

	// ProbeBW (§5.3.3).
	cycleStamp          time.Time
	bwProbeWait         time.Duration
	roundsSinceProbeUp  int64
	bwProbeUpRounds     int
	bwProbeUpAcked      float64
	probeUpAckedPerInc  float64
	checkProbeLoss      bool
	prevProbeTooHigh    bool
	prevProbePrecaution bool
	isBwProbeStopping   bool
	probeRttCwnd        float64
	probeRttDoneStamp   time.Time
	probeRttRoundDone   bool
	lastNow             time.Time
	c                   Counters
}

// Counters count what the controller did, for logs.
type Counters struct {
	Acks, Losses, Timeouts int64
	Rounds                 int64
	BwProbes               int64 // times ProbeBW_UP was entered
	ProbeRTTs              int64
	InflightTooHigh        int64 // times loss while probing set inflight_longterm
	LossRounds             int64 // rounds that pulled the short-term bounds down
	AppLimitedAcks         int64
}

// New returns a controller in Startup.
func New(cfg Config) *BBR {
	cfg = cfg.withDefaults()
	b := &BBR{
		cfg: cfg, cwnd: cfg.InitCwnd,
		// InitPacingRate, before there is an RTT: as if it were 1 ms.
		pacingRate:       startupPacingGain * cfg.InitCwnd / 0.001,
		inflightLongterm: math.Inf(1), probeUpAckedPerInc: math.Inf(1), probeRttCwnd: math.Inf(1),
	}
	b.resetShortTermModel()
	b.enterStartup()
	return b
}

// Cwnd is the congestion window in packets (not an integer).
func (b *BBR) Cwnd() float64 { return b.cwnd }

// SRTT is the smoothed RTT (zero before the first sample). BBR itself does
// not use it; the sender's loss detection does.
func (b *BBR) SRTT() time.Duration { return b.srtt }

// PacingRate is how fast the sender may release packets, in packets per
// second: pacing_gain · bw less 1%, and no more than MaxRate.
func (b *BBR) PacingRate() float64 {
	if b.cfg.MaxRate > 0 {
		return math.Min(b.pacingRate, b.cfg.MaxRate)
	}
	return b.pacingRate
}

// OnSend records one packet leaving at now (OnPacketSent, §4.1.2.2). The
// sender passes the same time to OnAck or OnLoss as sentAt.
func (b *BBR) OnSend(now time.Time) {
	b.lastNow = now
	if b.inflight == 0 {
		b.firstSendTime, b.deliveredTime = now, now
	}
	b.inflight++
	b.sends = append(b.sends, sendRec{
		at: now, firstSendTime: b.firstSendTime, deliveredTime: b.deliveredTime,
		delivered: b.delivered, lost: b.lost, txInFlight: b.inflight, appLimited: b.appLimited != 0,
	})
}

// take finds the record of a packet sent at sentAt and retires it. Packets
// that left in the same instant are interchangeable.
func (b *BBR) take(sentAt time.Time) (sendRec, bool) {
	s := b.sends[b.head:]
	i := sort.Search(len(s), func(i int) bool { return !s[i].at.Before(sentAt) })
	for ; i < len(s) && s[i].at.Equal(sentAt); i++ {
		if s[i].done {
			continue
		}
		s[i].done = true
		rec := s[i]
		for b.head < len(b.sends) && b.sends[b.head].done {
			b.head++
		}
		if b.head > 1024 && b.head > len(b.sends)/2 {
			b.sends = append(b.sends[:0], b.sends[b.head:]...)
			b.head = 0
		}
		return rec, true
	}
	return sendRec{}, false
}

// OnAck processes the acknowledgement of one packet (UpdateOnACK, §5.2.3).
// now is when the ack arrived, sentAt when the packet left and rtt the round
// trip it measured. windowFull says whether the window was what held the
// sender back (C.is_cwnd_limited). The window when the packet left is not
// used.
func (b *BBR) OnAck(now, sentAt time.Time, rtt time.Duration, _ float64, windowFull bool) {
	b.lastNow = now
	rec, ok := b.take(sentAt)
	if b.inflight > 0 {
		b.inflight--
	}
	if !ok || rtt <= 0 {
		return
	}
	b.c.Acks++
	b.latest = rtt
	if b.srtt == 0 {
		b.srtt = rtt
		// InitPacingRate again, now that there is an RTT.
		b.pacingRate = startupPacingGain * b.cfg.InitCwnd / rtt.Seconds()
	} else {
		b.srtt += (rtt - b.srtt) / 8
	}
	b.cwndLimited = windowFull
	// CheckIfApplicationLimited: the rate cap is the application running out
	// of data. It is the limit when it binds and the window does not.
	if b.cfg.MaxRate > 0 && b.pacingRate > b.cfg.MaxRate && !windowFull {
		b.markAppLimited()
		b.c.AppLimitedAcks++
	}

	// UpdateRateSample and GenerateRateSample (§4.1.2.3).
	b.delivered++
	b.deliveredTime = now
	if b.appLimited != 0 && b.delivered > b.appLimited {
		b.appLimited = 0
	}
	rs := rateSample{
		priorDelivered: rec.delivered, isAppLimited: rec.appLimited, rtt: rtt,
		delivered:  float64(b.delivered - rec.delivered),
		txInFlight: float64(rec.txInFlight), lost: float64(b.lost - rec.lost),
	}
	if rec.at.After(b.firstSendTime) {
		b.firstSendTime = rec.at
	}
	// The longer of how long it took to send that much and how long to have
	// it acknowledged; anything shorter than min_rtt is not to be believed.
	interval := max(rec.at.Sub(rec.firstSendTime), now.Sub(rec.deliveredTime))
	if interval > 0 && interval >= b.minRtt {
		rs.deliveryRate = rs.delivered / interval.Seconds()
	}

	b.updateModelAndState(now, rs)

	if b.inRecovery && !sentAt.Before(b.recoveryStart) {
		// Recovery is over once something sent after it began is delivered.
		b.inRecovery = false
		b.restoreCwnd()
	}

	// UpdateControlParameters.
	b.setPacingRate()
	b.setCwnd()
}

func (b *BBR) updateModelAndState(now time.Time, rs rateSample) {
	b.updateLatestDeliverySignals(rs)
	b.updateCongestionSignals(rs)
	b.updateACKAggregation(now)
	b.checkFullBWReached(rs)
	b.checkStartupDone(rs)
	b.checkDrainDone(now)
	b.updateProbeBWCyclePhase(now, rs)
	b.updateMinRTT(now, rs)
	b.checkProbeRTT(now)
	b.advanceLatestDeliverySignals(rs)
	b.boundBWForModel()
}

// OnLoss reports one lost packet and when it was sent (HandleLostPacket,
// §5.5.10.2).
func (b *BBR) OnLoss(now, sentAt time.Time) {
	b.lastNow = now
	rec, ok := b.take(sentAt)
	if b.inflight > 0 {
		b.inflight--
	}
	b.c.Losses++
	b.lost++
	if !b.inRecovery {
		// OnEnterFastRecovery. The window is not cut here: the model bounds it.
		b.saveCwnd()
		b.inRecovery, b.recoveryStart = true, now
	}
	// NoteLoss.
	if !b.lossInRound {
		b.lossRoundDelivered = b.delivered
	}
	b.lossInRound = true
	b.lossEventsInRound++
	if !ok || !b.checkProbeLoss {
		return // not a packet sent while probing for bandwidth
	}
	rs := rateSample{txInFlight: float64(rec.txInFlight), lost: float64(b.lost - rec.lost), isAppLimited: rec.appLimited}
	if isInflightTooHigh(rs) {
		// InflightAtLoss: where, on the way up to this packet, losses crossed
		// the threshold.
		inflightPrev := rs.txInFlight - 1
		lostPrev := rs.lost - 1
		lostPrefix := (lossThresh*inflightPrev - lostPrev) / (1 - lossThresh)
		rs.txInFlight = inflightPrev + lostPrefix
		b.handleInflightTooHigh(now, rs)
	}
}

// OnTimeout is for the sender to call when nothing at all has been
// acknowledged for a retransmission timeout (OnEnterRTO, §5.6.4.4): the
// window collapses to what is in flight plus one, and comes back when a
// packet sent afterwards is acknowledged.
func (b *BBR) OnTimeout() {
	b.c.Timeouts++
	b.saveCwnd()
	if !b.isProbingBW() && math.IsInf(b.inflightShortterm, 1) {
		// Not in the draft; Linux does it. The round that follows will take
		// its short-term bound from the window, and that must be the window
		// from before the collapse, or the flow is held to four packets until
		// the next probe.
		b.inflightShortterm = math.Max(b.cwnd, b.priorCwnd)
	}
	b.cwnd = float64(b.inflight) + 1
	b.inRecovery, b.recoveryStart = true, b.lastNow
}

func (b *BBR) saveCwnd() {
	if !b.inRecovery && b.state != ProbeRTT {
		b.priorCwnd = b.cwnd
	} else {
		b.priorCwnd = math.Max(b.priorCwnd, b.cwnd)
	}
}

func (b *BBR) restoreCwnd() { b.cwnd = math.Max(b.cwnd, b.priorCwnd) }

func (b *BBR) markAppLimited() {
	b.appLimited = max(b.delivered+int64(b.inflight), 1)
}

// ---------------------------------------------------------------------------
// Rounds, the bandwidth filter and the per-round congestion signals.
// ---------------------------------------------------------------------------

func (b *BBR) startRound() { b.nextRoundDelivered = b.delivered }

// updateRound: a round trip has passed when a packet sent after the last
// round began is acknowledged.
func (b *BBR) updateRound(rs rateSample) {
	if rs.priorDelivered < b.nextRoundDelivered {
		b.roundStart = false
		return
	}
	b.startRound()
	b.roundCount++
	b.roundsSinceProbeUp++
	b.roundStart = true
	b.c.Rounds++
	// The extra_acked filter's clock is rounds: two buckets of half its
	// length each (one round each until the pipe has been filled).
	half := 1
	if b.fullBwReached {
		half = extraAckedFilterLen / 2
	}
	if b.extraAckedRounds++; b.extraAckedRounds >= half {
		b.extraAckedRounds = 0
		b.extraAckedFilter[0], b.extraAckedFilter[1] = b.extraAckedFilter[1], 0
	}
}

func (b *BBR) updateMaxBw(rs rateSample) {
	b.updateRound(rs)
	if rs.deliveryRate > 0 && (rs.deliveryRate >= b.maxBw || !rs.isAppLimited) {
		b.maxBwFilter[1] = math.Max(b.maxBwFilter[1], rs.deliveryRate)
		b.maxBw = math.Max(b.maxBwFilter[0], b.maxBwFilter[1])
	}
}

// advanceMaxBwFilter starts a new ProbeBW cycle: the estimate now forgets
// the cycle before the last.
func (b *BBR) advanceMaxBwFilter() {
	if b.maxBwFilter[1] == 0 {
		return // nothing measured in this cycle to replace the last with
	}
	b.maxBwFilter[0], b.maxBwFilter[1] = b.maxBwFilter[1], 0
	b.maxBw = b.maxBwFilter[0]
}

func (b *BBR) updateLatestDeliverySignals(rs rateSample) {
	b.lossRoundStart = false
	b.bwLatest = math.Max(b.bwLatest, rs.deliveryRate)
	b.inflightLatest = math.Max(b.inflightLatest, rs.delivered)
	if rs.priorDelivered >= b.lossRoundDelivered {
		b.lossRoundDelivered = b.delivered
		b.lossRoundStart = true
	}
}

func (b *BBR) advanceLatestDeliverySignals(rs rateSample) {
	if b.lossRoundStart {
		b.bwLatest = rs.deliveryRate
		b.inflightLatest = rs.delivered
		b.lossEventsInRound = 0
	}
}

func (b *BBR) resetCongestionSignals() {
	b.lossInRound = false
	b.lossEventsInRound = 0
	b.bwLatest, b.inflightLatest = 0, 0
}

func (b *BBR) updateCongestionSignals(rs rateSample) {
	b.updateMaxBw(rs)
	if !b.lossRoundStart {
		return // once per round
	}
	// AdaptLowerBoundsFromCongestion: a round with loss, outside a probe,
	// pulls the short-term bounds towards what was delivered in it.
	if !b.isProbingBW() && b.lossInRound {
		if math.IsInf(b.bwShortterm, 1) {
			b.bwShortterm = b.maxBw
		}
		if math.IsInf(b.inflightShortterm, 1) {
			b.inflightShortterm = b.cwnd
		}
		b.bwShortterm = math.Max(b.bwLatest, beta*b.bwShortterm)
		b.inflightShortterm = math.Max(b.inflightLatest, beta*b.inflightShortterm)
		b.c.LossRounds++
	}
	b.lossInRound = false
}

func (b *BBR) resetShortTermModel() {
	b.bwShortterm, b.inflightShortterm = math.Inf(1), math.Inf(1)
}

func (b *BBR) boundBWForModel() { b.bw = math.Min(b.maxBw, b.bwShortterm) }

// updateACKAggregation estimates how far acks run ahead of the rate in
// bursts (§5.5.9), so that the window leaves room to keep sending between
// them.
func (b *BBR) updateACKAggregation(now time.Time) {
	if b.extraAckedIntervalStart.IsZero() {
		b.extraAckedIntervalStart = now
	}
	expected := b.bw * now.Sub(b.extraAckedIntervalStart).Seconds()
	if b.extraAckedDelivered <= expected {
		b.extraAckedDelivered = 0
		b.extraAckedIntervalStart = now
		expected = 0
	}
	b.extraAckedDelivered++
	extra := math.Min(b.extraAckedDelivered-expected, b.cwnd)
	b.extraAckedFilter[1] = math.Max(b.extraAckedFilter[1], extra)
	b.extraAcked = math.Max(b.extraAckedFilter[0], b.extraAckedFilter[1])
}

// ---------------------------------------------------------------------------
// Startup and Drain.
// ---------------------------------------------------------------------------

func (b *BBR) enterStartup() {
	b.state, b.pacingGain, b.cwndGain = Startup, startupPacingGain, defaultCwndGain
}

func (b *BBR) resetFullBW() {
	b.fullBw, b.fullBwCount, b.fullBwNow = 0, 0, false
}

// checkFullBWReached: three rounds in which the delivery rate grew by less
// than a quarter mean the pipe is full.
func (b *BBR) checkFullBWReached(rs rateSample) {
	if b.fullBwNow || !b.roundStart || rs.isAppLimited {
		return
	}
	if rs.deliveryRate >= b.fullBw*1.25 {
		b.resetFullBW()
		b.fullBw = rs.deliveryRate
		return
	}
	b.fullBwCount++
	b.fullBwNow = b.fullBwCount >= 3
	if b.fullBwNow {
		b.fullBwReached = true
	}
}

func (b *BBR) checkStartupDone(rs rateSample) {
	// CheckStartupHighLoss: a whole round in recovery with enough losses, at
	// more than the loss threshold.
	if !b.fullBwReached && b.lossRoundStart && b.inRecovery &&
		b.lossEventsInRound >= startupFullLossCnt && isInflightTooHigh(rs) {
		b.fullBwReached = true
		b.inflightLongterm = math.Max(b.bdp, b.inflightLatest)
	}
	if b.state == Startup && b.fullBwReached {
		b.state, b.pacingGain, b.cwndGain = Drain, drainPacingGain, defaultCwndGain
		b.drainStart = b.roundCount
	}
}

func (b *BBR) checkDrainDone(now time.Time) {
	if b.state == Drain && (float64(b.inflight) <= b.inflightFor(b.bw, 1) || b.roundCount > b.drainStart+3) {
		b.cwndGain = defaultCwndGain
		b.startProbeBWDown(now)
	}
}

// ---------------------------------------------------------------------------
// ProbeBW.
// ---------------------------------------------------------------------------

func (b *BBR) startProbeBWDown(now time.Time) {
	b.resetCongestionSignals()
	b.probeUpAckedPerInc = math.Inf(1)
	// PickProbeWait.
	b.roundsSinceProbeUp = 0
	if b.cfg.Rand() >= 0.5 {
		b.roundsSinceProbeUp = 1
	}
	b.bwProbeWait = 2*time.Second + time.Duration(b.cfg.Rand()*float64(time.Second))
	b.cycleStamp = now
	b.isBwProbeStopping = true
	b.startRound()
	b.state, b.pacingGain, b.cwndGain = ProbeBWDown, probeDownPacingGain, defaultCwndGain
}

func (b *BBR) startProbeBWCruise() {
	b.state, b.pacingGain, b.cwndGain = ProbeBWCruise, 1, defaultCwndGain
}

func (b *BBR) startProbeBWRefill() {
	b.resetShortTermModel()
	b.bwProbeUpRounds, b.bwProbeUpAcked = 0, 0
	b.prevProbePrecaution = false
	b.isBwProbeStopping = false
	b.startRound()
	b.state, b.pacingGain, b.cwndGain = ProbeBWRefill, 1, defaultCwndGain
}

func (b *BBR) startProbeBWUp(rs rateSample) {
	b.startRound()
	b.resetFullBW()
	b.fullBw = rs.deliveryRate
	b.state, b.pacingGain, b.cwndGain = ProbeBWUp, probeUpPacingGain, probeUpCwndGain
	b.raiseInflightLongtermSlope()
	b.c.BwProbes++
}

func (b *BBR) inProbeBW() bool {
	return b.state == ProbeBWDown || b.state == ProbeBWCruise || b.state == ProbeBWRefill || b.state == ProbeBWUp
}

func (b *BBR) isProbingBW() bool {
	return b.state == Startup || b.state == ProbeBWRefill || b.state == ProbeBWUp
}

func (b *BBR) updateProbeBWCyclePhase(now time.Time, rs rateSample) {
	if !b.fullBwReached {
		return
	}
	if b.adaptLongTermModel(rs) || !b.inProbeBW() {
		return
	}
	switch b.state {
	case ProbeBWDown:
		if b.isTimeToProbeBW(now) {
			return
		}
		// IsTimeToCruise: there is headroom, and no more than a BDP in flight.
		if in := float64(b.inflight); in <= b.inflightWithHeadroom() && in <= b.inflightFor(b.maxBw, 1) {
			b.startProbeBWCruise()
		}
	case ProbeBWCruise:
		b.isTimeToProbeBW(now)
	case ProbeBWRefill:
		if b.roundStart {
			b.checkProbeLoss = true
			b.startProbeBWUp(rs)
		}
	case ProbeBWUp:
		if b.isTimeToGoDown(rs) {
			b.prevProbeTooHigh = false
			b.startProbeBWDown(now)
		}
	}
}

// isTimeToProbeBW: after 2-3 s, or after as many rounds as a Reno flow with
// this window would take to grow back from a loss (at most 63), so that BBR
// probes no less often than Reno would.
func (b *BBR) isTimeToProbeBW(now time.Time) bool {
	renoRounds := int64(math.Min(b.targetInflight(), 63))
	if now.After(b.cycleStamp.Add(b.bwProbeWait)) || b.roundsSinceProbeUp >= renoRounds {
		b.startProbeBWRefill()
		return true
	}
	return false
}

func (b *BBR) targetInflight() float64 { return math.Min(b.bdp, b.cwnd) }

func (b *BBR) isTimeToGoDown(rs rateSample) bool {
	// Precautionary probing: the last probe met high loss, so stop this one
	// at the ceiling it set.
	if b.prevProbeTooHigh && float64(b.inflight) >= b.inflightLongterm {
		b.prevProbePrecaution = true
		return true
	}
	if b.cwndLimited && b.cwnd >= b.inflightLongterm {
		// The ceiling, not the path, is what limits the rate: no plateau yet.
		b.resetFullBW()
		b.fullBw = rs.deliveryRate
	} else if b.fullBwNow {
		return true
	}
	return false
}

func (b *BBR) inflightWithHeadroom() float64 {
	if math.IsInf(b.inflightLongterm, 1) {
		return math.Inf(1)
	}
	return math.Max(b.inflightLongterm-math.Max(1, headroom*b.inflightLongterm), minPipeCwnd)
}

func (b *BBR) raiseInflightLongtermSlope() {
	growth := float64(int64(1) << b.bwProbeUpRounds)
	b.bwProbeUpRounds = min(b.bwProbeUpRounds+1, 30)
	b.probeUpAckedPerInc = math.Max(b.cwnd/growth, 1)
}

// probeInflightLongtermUpward raises the ceiling while probing at it without
// high loss: by one packet in the first round, two in the next, four...
func (b *BBR) probeInflightLongtermUpward() {
	if !b.cwndLimited || b.cwnd < b.inflightLongterm {
		return
	}
	b.bwProbeUpAcked++
	if b.bwProbeUpAcked >= b.probeUpAckedPerInc {
		delta := math.Floor(b.bwProbeUpAcked / b.probeUpAckedPerInc)
		b.bwProbeUpAcked -= delta * b.probeUpAckedPerInc
		b.inflightLongterm += delta
	}
	if b.roundStart {
		b.raiseInflightLongtermSlope()
	}
}

// adaptLongTermModel reports whether it changed the state.
func (b *BBR) adaptLongTermModel(rs rateSample) bool {
	if b.isBwProbeStopping && b.roundStart {
		// The last packets of the probe have been accounted for.
		b.checkProbeLoss, b.isBwProbeStopping = false, false
		if b.inProbeBW() && !rs.isAppLimited {
			b.advanceMaxBwFilter()
		}
		if b.inProbeBW() && b.prevProbePrecaution && !b.prevProbeTooHigh {
			// The precautionary probe met no high loss: probe again at once.
			b.startProbeBWRefill()
			return true
		}
	}
	if !isInflightTooHigh(rs) {
		if math.IsInf(b.inflightLongterm, 1) {
			return false
		}
		if rs.txInFlight > b.inflightLongterm {
			b.inflightLongterm = rs.txInFlight
		}
		if b.state == ProbeBWUp {
			b.probeInflightLongtermUpward()
		}
	}
	return false
}

func isInflightTooHigh(rs rateSample) bool { return rs.lost > rs.txInFlight*lossThresh }

func (b *BBR) handleInflightTooHigh(now time.Time, rs rateSample) {
	b.prevProbeTooHigh = true
	b.checkProbeLoss = false // once per probe
	if !rs.isAppLimited {
		b.inflightLongterm = math.Max(rs.txInFlight, b.targetInflight()*beta)
		b.c.InflightTooHigh++
	}
	if b.state == ProbeBWUp {
		b.startProbeBWDown(now)
	}
}

// ---------------------------------------------------------------------------
// min_rtt and ProbeRTT.
// ---------------------------------------------------------------------------

func (b *BBR) updateMinRTT(now time.Time, rs rateSample) {
	b.minRttExpired = b.minRtt != 0 && now.After(b.minRttStamp.Add(minRTTFilterLen))
	if b.minRttExpired {
		b.probeRttCwnd = b.bdpMultiple(probeRTTCwndGain)
	}
	if b.minRtt == 0 || rs.rtt < b.minRtt || b.minRttExpired {
		b.minRtt, b.minRttStamp = rs.rtt, now
	}
}

func (b *BBR) checkProbeRTT(now time.Time) {
	if b.state != ProbeRTT && b.minRttExpired {
		b.saveCwnd()
		b.state, b.pacingGain, b.cwndGain = ProbeRTT, 1, probeRTTCwndGain
		b.probeRttDoneStamp = time.Time{}
		b.isBwProbeStopping = true
		b.startRound()
		b.c.ProbeRTTs++
	}
	if b.state != ProbeRTT {
		return
	}
	// HandleProbeRTT. Rate samples taken here are low on purpose.
	b.markAppLimited()
	if b.probeRttDoneStamp.IsZero() {
		if float64(b.inflight) <= b.probeRTTCwndNow() {
			// Hold it there for 200 ms and at least one round.
			b.probeRttDoneStamp = now.Add(probeRTTDuration)
			b.probeRttRoundDone = false
			b.startRound()
		}
		return
	}
	if b.roundStart {
		b.probeRttRoundDone = true
	}
	if b.probeRttRoundDone && now.After(b.probeRttDoneStamp) {
		b.minRttStamp = now // the next ProbeRTT is 10 s from here
		b.restoreCwnd()
		// ExitProbeRTT.
		b.resetShortTermModel()
		if b.fullBwReached {
			b.startProbeBWDown(now)
			b.startProbeBWCruise()
		} else {
			b.enterStartup()
		}
	}
}

func (b *BBR) probeRTTCwndNow() float64 {
	return math.Max(math.Min(b.bdpMultiple(probeRTTCwndGain), b.probeRttCwnd), minPipeCwnd)
}

// ---------------------------------------------------------------------------
// Control parameters: pacing rate and window.
// ---------------------------------------------------------------------------

func (b *BBR) setPacingRate() {
	rate := b.pacingGain * b.bw * (1 - pacingMargin)
	// Until the pipe has been filled the rate only goes up, so that a run of
	// low samples does not stall Startup.
	if b.fullBwReached || rate > b.pacingRate {
		b.pacingRate = rate
	}
}

func (b *BBR) bdpMultiple(gain float64) float64 {
	if b.minRtt == 0 {
		return b.cfg.InitCwnd
	}
	b.bdp = b.bw * b.minRtt.Seconds()
	return gain * b.bdp
}

// quantizationBudget: never fewer than four packets, and two more while
// probing up.
func (b *BBR) quantizationBudget(inflight float64) float64 {
	inflight = math.Max(inflight, minPipeCwnd)
	if b.state == ProbeBWUp {
		inflight += 2
	}
	return inflight
}

// inflightFor is the draft's Inflight(): gain BDPs at the given bandwidth.
func (b *BBR) inflightFor(bw, gain float64) float64 {
	if b.minRtt == 0 {
		return b.quantizationBudget(b.cfg.InitCwnd)
	}
	return b.quantizationBudget(gain * bw * b.minRtt.Seconds())
}

func (b *BBR) setCwnd() {
	b.maxInflight = b.quantizationBudget(b.bdpMultiple(b.cwndGain) + b.extraAcked)
	if b.fullBwReached {
		b.cwnd = math.Min(b.cwnd+1, b.maxInflight)
	} else if b.cwnd < b.maxInflight || float64(b.delivered) < b.cfg.InitCwnd {
		b.cwnd++
	}
	b.cwnd = math.Max(b.cwnd, minPipeCwnd)
	if b.state == ProbeRTT {
		b.cwnd = math.Min(b.cwnd, b.probeRTTCwndNow())
	}
	// BoundCwndForModel.
	limit := math.Inf(1)
	if b.inProbeBW() && b.state != ProbeBWCruise {
		limit = b.inflightLongterm
	} else if b.state == ProbeRTT || b.state == ProbeBWCruise {
		limit = b.inflightWithHeadroom()
	}
	limit = math.Max(math.Min(limit, b.inflightShortterm), minPipeCwnd)
	b.cwnd = math.Min(math.Min(b.cwnd, limit), b.cfg.MaxCwnd)
}

// Stats is the controller's state, for logs and the page. Rates are in
// packets per second and times in milliseconds.
type Stats struct {
	State             State    `json:"state"`
	SlowStart         bool     `json:"slowStart"` // Startup
	Cwnd              float64  `json:"cwnd"`
	Bw                float64  `json:"bw"` // min(maxBw, bwShortterm): what the rate is set from
	MaxBw             float64  `json:"maxBw"`
	BwShortterm       *float64 `json:"bwShortterm,omitempty"`       // absent while not set
	InflightLongterm  *float64 `json:"inflightLongterm,omitempty"`  // absent until loss sets it
	InflightShortterm *float64 `json:"inflightShortterm,omitempty"` // absent while not set
	BDP               float64  `json:"bdp"`
	MaxInflight       float64  `json:"maxInflight"`
	ExtraAcked        float64  `json:"extraAcked"`
	PacingGain        float64  `json:"pacingGain"`
	CwndGain          float64  `json:"cwndGain"`
	RTTMinMs          float64  `json:"rttMinMs"`
	SRTTMs            float64  `json:"srttMs"`
	LatestRTTMs       float64  `json:"latestRttMs"`
	CurrentRate       float64  `json:"currentRate"` // bw
	PacingRate        float64  `json:"pacingRate"`
	InFlight          int      `json:"inFlight"`
	Round             int64    `json:"round"`
	FullBwReached     bool     `json:"fullBwReached"`
	AppLimited        bool     `json:"appLimited"`
	InRecovery        bool     `json:"inRecovery"`
	Counters          Counters `json:"counters"`
}

func (b *BBR) Stats() Stats {
	s := Stats{
		State: b.state, SlowStart: b.state == Startup, Cwnd: b.cwnd, Bw: b.bw, MaxBw: b.maxBw,
		BDP: b.bdp, MaxInflight: b.maxInflight, ExtraAcked: b.extraAcked,
		PacingGain: b.pacingGain, CwndGain: b.cwndGain,
		RTTMinMs: msf(b.minRtt), SRTTMs: msf(b.srtt), LatestRTTMs: msf(b.latest),
		CurrentRate: b.bw, PacingRate: b.PacingRate(), InFlight: b.inflight, Round: b.roundCount,
		FullBwReached: b.fullBwReached, AppLimited: b.appLimited != 0, InRecovery: b.inRecovery, Counters: b.c,
	}
	finite := func(v float64) *float64 {
		if math.IsInf(v, 1) {
			return nil
		}
		return &v
	}
	s.BwShortterm, s.InflightLongterm, s.InflightShortterm = finite(b.bwShortterm), finite(b.inflightLongterm), finite(b.inflightShortterm)
	return s
}

func msf(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
