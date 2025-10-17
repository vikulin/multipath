package multipath

import (
	"sync"
	"sync/atomic"
	"time"
)

// CongestionControl implements a simple congestion control algorithm
// similar to TCP's congestion control but adapted for multipath
type CongestionControl struct {
	// Congestion window size
	cwnd uint32
	// Slow start threshold
	ssthresh uint32
	// Maximum congestion window
	maxCwnd uint32
	// Current state
	state CongestionState
	// Last congestion event
	lastCongestionEvent time.Time
	// RTT measurements for congestion detection
	rttMeasurements []time.Duration
	// Lock for thread safety
	mu sync.RWMutex
}

type CongestionState int

const (
	SlowStart CongestionState = iota
	CongestionAvoidance
	FastRecovery
)

// NewCongestionControl creates a new congestion control instance
func NewCongestionControl() *CongestionControl {
	return &CongestionControl{
		cwnd:     10, // Start with small window
		ssthresh: 100,
		maxCwnd:  1000,
		state:    SlowStart,
	}
}

// OnPacketSent is called when a packet is sent
func (cc *CongestionControl) OnPacketSent() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	// In slow start, double the window
	if cc.state == SlowStart {
		cc.cwnd = min(cc.cwnd*2, cc.maxCwnd)
		if cc.cwnd >= cc.ssthresh {
			cc.state = CongestionAvoidance
		}
	}
}

// OnPacketAcked is called when a packet is acknowledged
func (cc *CongestionControl) OnPacketAcked() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	switch cc.state {
	case SlowStart:
		// Already handled in OnPacketSent
	case CongestionAvoidance:
		// Additive increase
		cc.cwnd = min(cc.cwnd+1, cc.maxCwnd)
	case FastRecovery:
		// Exit fast recovery
		cc.state = CongestionAvoidance
	}
}

// OnPacketLost is called when a packet is lost
func (cc *CongestionControl) OnPacketLost() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.lastCongestionEvent = time.Now()

	// Multiplicative decrease
	cc.ssthresh = max(cc.cwnd/2, 2)
	cc.cwnd = cc.ssthresh
	cc.state = CongestionAvoidance
}

// OnRTTUpdate is called when RTT is updated
func (cc *CongestionControl) OnRTTUpdate(rtt time.Duration) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	// Keep track of recent RTT measurements
	cc.rttMeasurements = append(cc.rttMeasurements, rtt)
	if len(cc.rttMeasurements) > 10 {
		cc.rttMeasurements = cc.rttMeasurements[1:]
	}

	// Detect congestion based on RTT increase
	if len(cc.rttMeasurements) >= 3 {
		recent := cc.rttMeasurements[len(cc.rttMeasurements)-3:]
		avgRecent := (recent[0] + recent[1] + recent[2]) / 3
		avgOlder := time.Duration(0)
		if len(cc.rttMeasurements) >= 6 {
			older := cc.rttMeasurements[len(cc.rttMeasurements)-6 : len(cc.rttMeasurements)-3]
			avgOlder = (older[0] + older[1] + older[2]) / 3
		}

		// If recent RTT is significantly higher, consider it congestion
		if avgOlder > 0 && avgRecent > avgOlder*3/2 {
			// Call OnPacketLost directly since we already have a write lock
			cc.lastCongestionEvent = time.Now()

			// Multiplicative decrease
			cc.cwnd = cc.cwnd * 3 / 4
			if cc.cwnd < 2 {
				cc.cwnd = 2
			}

			// Transition to congestion avoidance
			cc.state = CongestionAvoidance
		}
	}
}

// GetWindowSize returns the current congestion window size
func (cc *CongestionControl) GetWindowSize() uint32 {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	return cc.cwnd
}

// GetState returns the current congestion state
func (cc *CongestionControl) GetState() CongestionState {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	return cc.state
}

// FlowControl implements flow control to prevent overwhelming the receiver
type FlowControl struct {
	// Receiver window size
	rwnd uint32
	// Current window used
	windowUsed uint32
	// Lock for thread safety
	mu sync.RWMutex
}

// NewFlowControl creates a new flow control instance
func NewFlowControl(initialWindow uint32) *FlowControl {
	return &FlowControl{
		rwnd:       initialWindow,
		windowUsed: 0,
	}
}

// CanSend checks if we can send more data
func (fc *FlowControl) CanSend(size uint32) bool {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.windowUsed+size <= fc.rwnd
}

// OnDataSent is called when data is sent
func (fc *FlowControl) OnDataSent(size uint32) {
	atomic.AddUint32(&fc.windowUsed, size)
}

// OnDataAcked is called when data is acknowledged
func (fc *FlowControl) OnDataAcked(size uint32) {
	// Prevent underflow by using atomic operations safely
	for {
		current := atomic.LoadUint32(&fc.windowUsed)
		if current >= size {
			if atomic.CompareAndSwapUint32(&fc.windowUsed, current, current-size) {
				break
			}
		} else {
			// ACK more than sent, set to 0
			atomic.StoreUint32(&fc.windowUsed, 0)
			break
		}
	}
}

// UpdateWindow updates the receiver window size
func (fc *FlowControl) UpdateWindow(newWindow uint32) {
	atomic.StoreUint32(&fc.rwnd, newWindow)
}

// GetAvailableWindow returns the available window size
func (fc *FlowControl) GetAvailableWindow() uint32 {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	if fc.windowUsed >= fc.rwnd {
		return 0
	}
	return fc.rwnd - fc.windowUsed
}

// Helper functions
func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
