package multipath

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCongestionControl tests the congestion control implementation
func TestCongestionControl(t *testing.T) {
	cc := NewCongestionControl()

	// Test initial state
	assert.Equal(t, SlowStart, cc.GetState())
	assert.Equal(t, uint32(10), cc.GetWindowSize())

	// Test slow start phase
	expectedWindow := uint32(10)
	for i := 0; i < 4; i++ { // Only 4 iterations before transition
		cc.OnPacketSent()
		expectedWindow *= 2
		if expectedWindow > 1000 {
			expectedWindow = 1000
		}
		assert.Equal(t, expectedWindow, cc.GetWindowSize())
	}

	// 5th iteration should transition to congestion avoidance
	cc.OnPacketSent()
	assert.Equal(t, uint32(160), cc.GetWindowSize()) // Window stays at 160

	// Should transition to congestion avoidance
	assert.Equal(t, CongestionAvoidance, cc.GetState())
}

// TestCongestionControlPacketLoss tests packet loss handling
func TestCongestionControlPacketLoss(t *testing.T) {
	cc := NewCongestionControl()

	// Set up congestion avoidance state
	for i := 0; i < 10; i++ {
		cc.OnPacketSent()
	}

	initialWindow := cc.GetWindowSize()

	// Simulate packet loss
	cc.OnPacketLost()

	// Window should be reduced
	assert.True(t, cc.GetWindowSize() < initialWindow)
	assert.Equal(t, CongestionAvoidance, cc.GetState())
}

// TestCongestionControlRTTUpdate tests RTT-based congestion detection
func TestCongestionControlRTTUpdate(t *testing.T) {
	cc := NewCongestionControl()

	// Add some normal RTT measurements
	cc.OnRTTUpdate(50 * time.Millisecond)
	cc.OnRTTUpdate(60 * time.Millisecond)
	cc.OnRTTUpdate(55 * time.Millisecond)

	// Add significantly higher RTT (should trigger congestion)
	cc.OnRTTUpdate(200 * time.Millisecond)
	cc.OnRTTUpdate(250 * time.Millisecond)
	cc.OnRTTUpdate(300 * time.Millisecond)

	// Should detect congestion and reduce window
	assert.Equal(t, CongestionAvoidance, cc.GetState())
}

// TestCongestionControlAckHandling tests ACK handling
func TestCongestionControlAckHandling(t *testing.T) {
	cc := NewCongestionControl()

	// Set up congestion avoidance state
	for i := 0; i < 10; i++ {
		cc.OnPacketSent()
	}

	// Simulate packet loss to enter congestion avoidance
	cc.OnPacketLost()
	windowAfterLoss := cc.GetWindowSize()

	// Send some packets and ACK them
	for i := 0; i < 5; i++ {
		cc.OnPacketSent()
		cc.OnPacketAcked()
	}

	// Window should increase in congestion avoidance
	assert.True(t, cc.GetWindowSize() > windowAfterLoss)
}

// TestFlowControl tests the flow control implementation
func TestFlowControl(t *testing.T) {
	fc := NewFlowControl(1000)

	// Test initial state
	assert.True(t, fc.CanSend(500))
	assert.False(t, fc.CanSend(1500))
	assert.Equal(t, uint32(1000), fc.GetAvailableWindow())

	// Test data sending
	fc.OnDataSent(300)
	assert.Equal(t, uint32(700), fc.GetAvailableWindow())
	assert.True(t, fc.CanSend(700))
	assert.False(t, fc.CanSend(800))

	// Test data ACK
	fc.OnDataAcked(200)
	assert.Equal(t, uint32(900), fc.GetAvailableWindow())

	// Test window update
	fc.UpdateWindow(2000)
	assert.Equal(t, uint32(1900), fc.GetAvailableWindow()) // 2000 - 100 bytes in flight
}

// TestFlowControlEdgeCases tests edge cases in flow control
func TestFlowControlEdgeCases(t *testing.T) {
	fc := NewFlowControl(100)

	// Test exact window usage
	fc.OnDataSent(100)
	assert.Equal(t, uint32(0), fc.GetAvailableWindow())
	assert.False(t, fc.CanSend(1))

	// Test ACK more than sent
	fc.OnDataAcked(150)                                   // This should not cause underflow
	assert.Equal(t, uint32(100), fc.GetAvailableWindow()) // Window becomes available again

	// Test window update
	fc.UpdateWindow(200)
	assert.Equal(t, uint32(200), fc.GetAvailableWindow())
}

// TestEnhancedStatsTracker tests the enhanced statistics tracker
func TestEnhancedStatsTracker(t *testing.T) {
	est := NewEnhancedStatsTracker("test-conn-123")

	// Test initial state
	stats := est.GetConnectionStats()
	assert.Equal(t, "test-conn-123", stats.ConnectionID)
	assert.Equal(t, uint64(0), stats.TotalBytesSent)
	assert.Equal(t, uint64(0), stats.TotalBytesRecv)
	assert.Equal(t, 0, stats.ActivePaths)

	// Test data tracking
	est.OnSent(1000)
	est.OnRecv(2000)
	est.OnRetransmit(100)

	stats = est.GetConnectionStats()
	assert.Equal(t, uint64(1000), stats.TotalBytesSent)
	assert.Equal(t, uint64(2000), stats.TotalBytesRecv)
	assert.Equal(t, uint64(1), stats.TotalRetransmits)

	// Test RTT tracking
	est.UpdateRTT(50 * time.Millisecond)
	est.UpdateRTT(60 * time.Millisecond)

	stats = est.GetConnectionStats()
	assert.True(t, stats.AvgRTT > 0)
	assert.True(t, stats.MinRTT > 0)
	assert.True(t, stats.MaxRTT > 0)
}

// TestEnhancedStatsTrackerPathStats tests path statistics tracking
func TestEnhancedStatsTrackerPathStats(t *testing.T) {
	est := NewEnhancedStatsTracker("test-conn-456")

	// Create test path stats
	pathStats := &PathStats{
		PathName:         "path-1",
		BytesSent:        1000,
		BytesRecv:        2000,
		FramesSent:       10,
		FramesRecv:       20,
		FramesRetransmit: 2,
		Successes:        8,
		Failures:         2,
		ConsecSuccesses:  5,
		AvgRTT:           50 * time.Millisecond,
		MinRTT:           30 * time.Millisecond,
		MaxRTT:           80 * time.Millisecond,
		LastActivity:     time.Now(),
		IsActive:         true,
	}

	// Update path stats
	est.UpdatePathStats("path-1", pathStats)

	// Get path stats
	retrievedStats := est.GetPathStats()
	require.Contains(t, retrievedStats, "path-1")

	retrievedPath := retrievedStats["path-1"]
	assert.Equal(t, "path-1", retrievedPath.PathName)
	assert.Equal(t, uint64(1000), retrievedPath.BytesSent)
	assert.Equal(t, uint64(2000), retrievedPath.BytesRecv)
	assert.True(t, retrievedPath.IsActive)
}

// TestEnhancedStatsTrackerFormatting tests statistics formatting
func TestEnhancedStatsTrackerFormatting(t *testing.T) {
	est := NewEnhancedStatsTracker("test-conn-789")

	// Add some data
	est.OnSent(1000)
	est.OnRecv(2000)
	est.OnRetransmit(100)
	est.UpdateRTT(50 * time.Millisecond)

	// Test connection stats formatting
	stats := est.GetConnectionStats()
	formatted := stats.FormatConnectionStats()

	assert.Contains(t, formatted, "test-conn-789")
	assert.Contains(t, formatted, "1000")
	assert.Contains(t, formatted, "2000")
	assert.Contains(t, formatted, "50ms")

	// Test path stats formatting
	pathStats := &PathStats{
		PathName:     "test-path",
		BytesSent:    1000,
		BytesRecv:    2000,
		Successes:    8,
		Failures:     2,
		AvgRTT:       50 * time.Millisecond,
		IsActive:     true,
		LastActivity: time.Now(),
	}

	est.UpdatePathStats("test-path", pathStats)

	pathStatsMap := est.GetPathStats()
	require.Contains(t, pathStatsMap, "test-path")

	formattedPath := pathStatsMap["test-path"].FormatPathStats()
	assert.Contains(t, formattedPath, "test-path")
	assert.Contains(t, formattedPath, "Active")
	assert.Contains(t, formattedPath, "1000")
	assert.Contains(t, formattedPath, "2000")
}

// TestCongestionControlStateTransitions tests state transitions
func TestCongestionControlStateTransitions(t *testing.T) {
	cc := NewCongestionControl()

	// Start in slow start
	assert.Equal(t, SlowStart, cc.GetState())

	// Send packets to grow window
	for i := 0; i < 10; i++ {
		cc.OnPacketSent()
	}

	// Should be in congestion avoidance now
	assert.Equal(t, CongestionAvoidance, cc.GetState())

	// Simulate packet loss
	cc.OnPacketLost()

	// Should still be in congestion avoidance
	assert.Equal(t, CongestionAvoidance, cc.GetState())

	// Simulate fast recovery scenario
	cc.OnPacketLost()
	// In a real implementation, this might trigger fast recovery
	// For now, we'll just verify the state is consistent
	assert.Equal(t, CongestionAvoidance, cc.GetState())
}

// TestCongestionControlWindowBounds tests window size bounds
func TestCongestionControlWindowBounds(t *testing.T) {
	cc := NewCongestionControl()

	// Test slow start phase (should reach ssthresh)
	for i := 0; i < 5; i++ {
		cc.OnPacketSent()
	}

	// Should transition to congestion avoidance when window >= ssthresh
	assert.Equal(t, CongestionAvoidance, cc.GetState())
	assert.Equal(t, uint32(160), cc.GetWindowSize()) // Window grows to 160 before transition

	// Test maximum window size in congestion avoidance
	for i := 0; i < 840; i++ { // Need 840 iterations to reach 1000 from 160
		cc.OnPacketAcked() // Use OnPacketAcked for congestion avoidance
	}

	// Window should be capped at maxCwnd
	assert.Equal(t, uint32(1000), cc.GetWindowSize())

	// Test minimum window size after loss
	cc.OnPacketLost()

	// Window should be at least 2
	assert.True(t, cc.GetWindowSize() >= 2)
}

// TestFlowControlConcurrentAccess tests concurrent access to flow control
func TestFlowControlConcurrentAccess(t *testing.T) {
	fc := NewFlowControl(10000)

	// Test concurrent access
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()

			for j := 0; j < 100; j++ {
				if fc.CanSend(100) {
					fc.OnDataSent(100)
					fc.OnDataAcked(50) // Simulate partial ACK
				}
			}
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify final state is consistent
	assert.True(t, fc.GetAvailableWindow() <= 10000)
}

// TestEnhancedStatsTrackerConcurrentAccess tests concurrent access to stats tracker
func TestEnhancedStatsTrackerConcurrentAccess(t *testing.T) {
	est := NewEnhancedStatsTracker("concurrent-test")

	// Test concurrent access
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < 100; j++ {
				est.OnSent(100)
				est.OnRecv(200)
				est.UpdateRTT(time.Duration(50+j) * time.Millisecond)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify final state
	stats := est.GetConnectionStats()
	assert.Equal(t, uint64(100000), stats.TotalBytesSent) // 10 * 100 * 100
	assert.Equal(t, uint64(200000), stats.TotalBytesRecv) // 10 * 100 * 200
	assert.True(t, stats.AvgRTT > 0)
}
