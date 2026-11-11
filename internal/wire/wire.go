// Package wire is the datagram format spoken on the "media" DataChannel
// (unordered, maxRetransmits 0). Every message is one SCTP DATA chunk and so
// one UDP packet. Everything is big-endian. web/wire.js is the browser side.
//
// Server -> browser
//
//	DATA  [0]=1 [1] flags [2:6] seq u32 [6:14] sendUs i64 [14:18] frameId u32
//	      [18:20] fragIdx u16 [20:22] fragCount u16 [22:30] captureUs i64 [30:] payload
//	PONG  [0]=3 [1:5] id u32 [5:13] t1 f64 (echoed) [13:21] t2 i64 [21:29] t3 i64
//
// seq counts every DATA packet put on the wire, retransmissions included, so
// feedback about a seq is about exactly one transmission. sendUs is the server
// clock (Unix µs) when the packet left the pacer; captureUs is when the frame
// entered the encoder. A frame is the Annex-B access unit split into
// fragCount payloads.
//
// Browser -> server
//
//	PING      [0]=2  [1:5] id u32 [5:13] t1 f64 (browser ms)
//	FEEDBACK  [0]=16 [1:5] fbSeq u32 [5:13] tSend f64 (browser ms) [13:15] n u16
//	          then n x ([0:4] seq u32 [4:12] recvMs f64 (browser ms))
//	NACK      [0]=17 [1:3] n u16, then n x ([0:4] frameId u32 [4:6] fragIdx u16)
//	KEYREQ    [0]=18 [1:5] frameId u32 (newest frame the browser has seen)
//
// A NACK with fragIdx 0xFFFF asks for every fragment of the frame.
package wire

import (
	"encoding/binary"
	"errors"
	"math"
	"time"
)

const (
	TypeData     = 1
	TypePing     = 2
	TypePong     = 3
	TypeFeedback = 16
	TypeNack     = 17
	TypeKeyReq   = 18

	FlagKey = 1 << 0
	FlagRtx = 1 << 1

	DataHeaderLen = 30
	// MaxMessage keeps a message inside one SCTP DATA chunk: pion's SCTP MTU is
	// 1191 bytes, less 12 (common header) and 16 (DATA chunk header) = 1163.
	MaxMessage = 1150
	MaxPayload = MaxMessage - DataHeaderLen

	WholeFrame = 0xFFFF
)

var ErrShort = errors.New("wire: short message")

// NewData builds a DATA message. seq and sendUs are left zero: the pacer
// stamps them with Stamp when the packet actually leaves.
func NewData(frameID uint32, fragIdx, fragCount uint16, key bool, capture time.Time, payload []byte) []byte {
	b := make([]byte, DataHeaderLen+len(payload))
	b[0] = TypeData
	if key {
		b[1] = FlagKey
	}
	binary.BigEndian.PutUint32(b[14:], frameID)
	binary.BigEndian.PutUint16(b[18:], fragIdx)
	binary.BigEndian.PutUint16(b[20:], fragCount)
	binary.BigEndian.PutUint64(b[22:], uint64(capture.UnixMicro()))
	copy(b[DataHeaderLen:], payload)
	return b
}

// Stamp sets the per-transmission fields of a DATA message.
func Stamp(b []byte, seq uint32, send time.Time, rtx bool) {
	if rtx {
		b[1] |= FlagRtx
	}
	binary.BigEndian.PutUint32(b[2:], seq)
	binary.BigEndian.PutUint64(b[6:], uint64(send.UnixMicro()))
}

// Ack is one received packet as reported by the browser.
type Ack struct {
	Seq    uint32
	RecvMs float64 // browser clock, ms
}

// Feedback is a parsed FEEDBACK message.
type Feedback struct {
	FbSeq  uint32
	SendMs float64 // browser clock when the feedback was sent
	Acks   []Ack
}

func ParseFeedback(b []byte) (Feedback, error) {
	if len(b) < 15 {
		return Feedback{}, ErrShort
	}
	fb := Feedback{FbSeq: binary.BigEndian.Uint32(b[1:]), SendMs: f64(b[5:])}
	n := int(binary.BigEndian.Uint16(b[13:]))
	if len(b) < 15+12*n {
		return Feedback{}, ErrShort
	}
	fb.Acks = make([]Ack, n)
	for i := range fb.Acks {
		e := b[15+12*i:]
		fb.Acks[i] = Ack{Seq: binary.BigEndian.Uint32(e), RecvMs: f64(e[4:])}
	}
	return fb, nil
}

// NackItem asks for one fragment (or the whole frame, FragIdx == WholeFrame).
type NackItem struct {
	FrameID uint32
	FragIdx uint16
}

func ParseNack(b []byte) ([]NackItem, error) {
	if len(b) < 3 {
		return nil, ErrShort
	}
	n := int(binary.BigEndian.Uint16(b[1:]))
	if len(b) < 3+6*n {
		return nil, ErrShort
	}
	items := make([]NackItem, n)
	for i := range items {
		e := b[3+6*i:]
		items[i] = NackItem{FrameID: binary.BigEndian.Uint32(e), FragIdx: binary.BigEndian.Uint16(e[4:])}
	}
	return items, nil
}

// Pong answers a PING (13 bytes) with the server's receive and send times.
func Pong(ping []byte, received, sent time.Time) []byte {
	b := make([]byte, 0, 29)
	b = append(b, TypePong)
	b = append(b, ping[1:13]...)
	b = binary.BigEndian.AppendUint64(b, uint64(received.UnixMicro()))
	return binary.BigEndian.AppendUint64(b, uint64(sent.UnixMicro()))
}

func f64(b []byte) float64 { return math.Float64frombits(binary.BigEndian.Uint64(b)) }
