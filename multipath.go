// Package multipath provides a simple way to aggregate multiple network paths
// between a pair of hosts to form a single connection from the upper layer
// perspective, for throughput and resilience.
//
// The term connection, path and subflow used here are the same as mentioned in
// MP-TCP https://www.rfc-editor.org/rfc/rfc8684.html#name-terminology
//
// Each subflow is a bidirectional byte stream each side in the following form
// until being disrupted or the connection ends. When establishing the very
// first subflow, the client sends an all-zero connnection ID (CID) and the
// server sends the assigned CID back. Subsequent subflows use the same CID.
//
//       ----------------------------------------------------
//      |  version(1)  |  cid(16)  |  frames (...)  |
//       ----------------------------------------------------
//
// There are two types of frames. Data frame carries application data while ack
// frame carries acknowledgement to the frame just received. When one data
// frame is not acked in time, it is sent over another subflow, until all
// available subflows have been tried. Payload size and frame number uses
// variable-length integer encoding as described here:
// https://tools.ietf.org/html/draft-ietf-quic-transport-29#section-16
//
//       --------------------------------------------------------
//      |  payload size(1-8)  |  frame number (1-8)  |  payload  |
//       --------------------------------------------------------
//
//       ---------------------------------------
//      |  00000000  |  ack frame number (1-8)  |
//       ---------------------------------------
//
// Ack frames with frame number < 10 are reserved for control. For now only 0
// and 1 are used, for ping and pong frame respectively. They are for updating
// RTT on inactive subflows and detecting recovered subflows.
//
// Ping frame:
//       -------------------------
//      |  00000000  |  00000000  |
//       -------------------------
//
// Pong frame:
//       -------------------------
//      |  00000000  |  00000001  |
//       -------------------------
//
package multipath

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getlantern/golog"
	"github.com/google/uuid"
	pool "github.com/libp2p/go-buffer-pool"
)

const (
	minFrameNumber uint64 = 10
	frameTypePing  uint64 = 0
	frameTypePong  uint64 = 1

	maxFrameSizeToCalculateRTT uint64 = 1500
	leadBytesLength                   = 1 + 16 // 1 byte version + 16 bytes CID
	// Assuming an average 1KB frame size, it would be able to buffer 4MB of
	// data without back pressure before the upper layer reads them.
	recieveQueueLength = 4096
	maxVarIntLength    = 8
	probeInterval      = time.Minute
	longRTT            = time.Minute
	rttAlpha           = 0.5 // this causes EMA to reflect changes more rapidly
)

var (
	ErrUnexpectedVersion = errors.New("unexpected version")
	ErrUnexpectedCID     = errors.New("unexpected connnection ID")
	ErrClosed            = errors.New("closed connection")
	ErrFailOnAllDialers  = errors.New("fail on all dialers")
	log                  = golog.LoggerFor("multipath")
	zeroCID              connectionID
	
	// Frame pool for reusing sendFrame objects
	framePool = sync.Pool{
		New: func() interface{} {
			return &sendFrame{
				sentVia: make([]transmissionDatapoint, 0, 4), // Pre-allocate with capacity
			}
		},
	}
)

type connectionID uuid.UUID

type rxFrame struct {
	fn    uint64
	bytes []byte
}

type transmissionDatapoint struct {
	sf     *subflow
	txTime time.Time
}

type sendFrame struct {
	fn                 uint64
	sz                 uint64
	buf                []byte
	released           *int32 // 1 == true; 0 == false. Use pointer so copied object still references the same address, as buf does
	retransmissions    int
	sentVia            []transmissionDatapoint // Contains the subflows it's already been written to, and when
	beingRetransmitted uint64
	changeLock         sync.Mutex
}

func composeFrame(fn uint64, b []byte) *sendFrame {
	// Get frame from pool
	frame := framePool.Get().(*sendFrame)
	
	// Reset frame state
	frame.fn = fn
	frame.sz = uint64(len(b))
	frame.retransmissions = 0
	frame.beingRetransmitted = 0
	frame.released = new(int32)
	
	// Reuse buffer if possible, otherwise get new one
	requiredSize := maxVarIntLength + maxVarIntLength + len(b)
	if frame.buf != nil && cap(frame.buf) >= requiredSize {
		// Reuse existing buffer
		frame.buf = frame.buf[:0]
	} else {
		// Get new buffer from pool
		frame.buf = pool.Get(requiredSize)
	}
	
	// Compose frame data
	wb := bytes.NewBuffer(frame.buf[:0])
	WriteVarInt(wb, uint64(len(b)))
	WriteVarInt(wb, fn)
	if len(b) > 0 {
		wb.Write(b)
	}
	frame.buf = wb.Bytes()
	
	return frame
}

func (f *sendFrame) isDataFrame() bool {
	return f.sz > 0
}

func (f *sendFrame) release() {
	if atomic.CompareAndSwapInt32(f.released, 0, 1) {
		// Return buffer to pool
		pool.Put(f.buf)
		// Reset frame and return to frame pool
		f.reset()
		framePool.Put(f)
	}
}

// reset prepares the frame for reuse in the pool
func (f *sendFrame) reset() {
	f.fn = 0
	f.sz = 0
	f.buf = nil
	f.released = nil
	f.retransmissions = 0
	f.beingRetransmitted = 0
	// Reset sentVia slice but keep capacity
	if f.sentVia != nil {
		f.sentVia = f.sentVia[:0]
	}
}

// StatsTracker allows getting a sense of how the paths perform. Its methods
// are called when each subflow sends or receives a frame.
type StatsTracker interface {
	OnRecv(uint64)
	OnSent(uint64)
	OnRetransmit(uint64)
	UpdateRTT(time.Duration)
}

type NullTracker struct{}

func (st NullTracker) OnRecv(uint64)           {}
func (st NullTracker) OnSent(uint64)           {}
func (st NullTracker) OnRetransmit(uint64)     {}
func (st NullTracker) UpdateRTT(time.Duration) {}
