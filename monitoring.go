package multipath

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// EnhancedStatsTracker provides detailed statistics for monitoring
type EnhancedStatsTracker struct {
	// Connection-level stats
	connectionID     string
	startTime        time.Time
	totalBytesSent   uint64
	totalBytesRecv   uint64
	totalFramesSent  uint64
	totalFramesRecv  uint64
	totalRetransmits uint64

	// Per-path stats
	pathStats map[string]*PathStats

	// Performance metrics
	avgRTT time.Duration
	minRTT time.Duration
	maxRTT time.Duration

	// Lock for thread safety
	mu sync.RWMutex
}

// PathStats tracks statistics for a single path
type PathStats struct {
	PathName         string
	BytesSent        uint64
	BytesRecv        uint64
	FramesSent       uint64
	FramesRecv       uint64
	FramesRetransmit uint64
	Successes        uint64
	Failures         uint64
	ConsecSuccesses  uint64
	AvgRTT           time.Duration
	MinRTT           time.Duration
	MaxRTT           time.Duration
	LastActivity     time.Time
	IsActive         bool
}

// NewEnhancedStatsTracker creates a new enhanced stats tracker
func NewEnhancedStatsTracker(connectionID string) *EnhancedStatsTracker {
	return &EnhancedStatsTracker{
		connectionID: connectionID,
		startTime:    time.Now(),
		pathStats:    make(map[string]*PathStats),
		minRTT:       time.Hour, // Initialize with high value
	}
}

// OnRecv is called when data is received
func (est *EnhancedStatsTracker) OnRecv(bytes uint64) {
	atomic.AddUint64(&est.totalBytesRecv, bytes)
	atomic.AddUint64(&est.totalFramesRecv, 1)
}

// OnSent is called when data is sent
func (est *EnhancedStatsTracker) OnSent(bytes uint64) {
	atomic.AddUint64(&est.totalBytesSent, bytes)
	atomic.AddUint64(&est.totalFramesSent, 1)
}

// OnRetransmit is called when data is retransmitted
func (est *EnhancedStatsTracker) OnRetransmit(bytes uint64) {
	atomic.AddUint64(&est.totalRetransmits, 1)
}

// UpdateRTT is called when RTT is updated
func (est *EnhancedStatsTracker) UpdateRTT(rtt time.Duration) {
	est.mu.Lock()
	defer est.mu.Unlock()

	// Update global RTT stats
	if rtt < est.minRTT {
		est.minRTT = rtt
	}
	if rtt > est.maxRTT {
		est.maxRTT = rtt
	}

	// Simple moving average for RTT
	if est.avgRTT == 0 {
		est.avgRTT = rtt
	} else {
		est.avgRTT = (est.avgRTT + rtt) / 2
	}
}

// UpdatePathStats updates statistics for a specific path
func (est *EnhancedStatsTracker) UpdatePathStats(pathName string, stats *PathStats) {
	est.mu.Lock()
	defer est.mu.Unlock()

	stats.LastActivity = time.Now()
	est.pathStats[pathName] = stats
}

// GetConnectionStats returns overall connection statistics
func (est *EnhancedStatsTracker) GetConnectionStats() ConnectionStats {
	est.mu.RLock()
	defer est.mu.RUnlock()

	uptime := time.Since(est.startTime)
	throughput := uint64(0)
	if uptime > time.Second {
		throughput = (atomic.LoadUint64(&est.totalBytesSent) + atomic.LoadUint64(&est.totalBytesRecv)) / uint64(uptime.Seconds())
	}

	packetLossRate := float64(0)
	if atomic.LoadUint64(&est.totalFramesSent) > 0 {
		packetLossRate = float64(atomic.LoadUint64(&est.totalRetransmits)) / float64(atomic.LoadUint64(&est.totalFramesSent)) * 100
	}

	return ConnectionStats{
		ConnectionID:     est.connectionID,
		Uptime:           uptime,
		TotalBytesSent:   atomic.LoadUint64(&est.totalBytesSent),
		TotalBytesRecv:   atomic.LoadUint64(&est.totalBytesRecv),
		TotalFramesSent:  atomic.LoadUint64(&est.totalFramesSent),
		TotalFramesRecv:  atomic.LoadUint64(&est.totalFramesRecv),
		TotalRetransmits: atomic.LoadUint64(&est.totalRetransmits),
		AvgRTT:           est.avgRTT,
		MinRTT:           est.minRTT,
		MaxRTT:           est.maxRTT,
		PacketLossRate:   packetLossRate,
		ThroughputBps:    throughput,
		ActivePaths:      est.getActivePathCount(),
	}
}

// GetPathStats returns statistics for all paths
func (est *EnhancedStatsTracker) GetPathStats() map[string]*PathStats {
	est.mu.RLock()
	defer est.mu.RUnlock()

	// Return a copy to prevent external modification
	result := make(map[string]*PathStats)
	for k, v := range est.pathStats {
		// Create a copy of the stats
		statsCopy := *v
		result[k] = &statsCopy
	}
	return result
}

// getActivePathCount returns the number of active paths
func (est *EnhancedStatsTracker) getActivePathCount() int {
	activeCount := 0
	for _, stats := range est.pathStats {
		if stats.IsActive && time.Since(stats.LastActivity) < 30*time.Second {
			activeCount++
		}
	}
	return activeCount
}

// ConnectionStats represents overall connection statistics
type ConnectionStats struct {
	ConnectionID     string
	Uptime           time.Duration
	TotalBytesSent   uint64
	TotalBytesRecv   uint64
	TotalFramesSent  uint64
	TotalFramesRecv  uint64
	TotalRetransmits uint64
	AvgRTT           time.Duration
	MinRTT           time.Duration
	MaxRTT           time.Duration
	PacketLossRate   float64
	ThroughputBps    uint64
	ActivePaths      int
}

// FormatConnectionStats returns a formatted string of connection statistics
func (cs *ConnectionStats) FormatConnectionStats() string {
	return fmt.Sprintf(`
Connection ID: %s
Uptime: %v
Total Bytes: Sent=%d, Recv=%d
Total Frames: Sent=%d, Recv=%d, Retransmits=%d
RTT: Avg=%v, Min=%v, Max=%v
Packet Loss Rate: %.2f%%
Throughput: %d bytes/sec
Active Paths: %d
`,
		cs.ConnectionID,
		cs.Uptime,
		cs.TotalBytesSent, cs.TotalBytesRecv,
		cs.TotalFramesSent, cs.TotalFramesRecv, cs.TotalRetransmits,
		cs.AvgRTT, cs.MinRTT, cs.MaxRTT,
		cs.PacketLossRate,
		cs.ThroughputBps,
		cs.ActivePaths,
	)
}

// FormatPathStats returns a formatted string of path statistics
func (ps *PathStats) FormatPathStats() string {
	status := "Inactive"
	if ps.IsActive {
		status = "Active"
	}

	return fmt.Sprintf(`
Path: %s [%s]
Bytes: Sent=%d, Recv=%d
Frames: Sent=%d, Recv=%d, Retransmits=%d
Success Rate: %d/%d (%.1f%%)
RTT: Avg=%v, Min=%v, Max=%v
Last Activity: %v
`,
		ps.PathName, status,
		ps.BytesSent, ps.BytesRecv,
		ps.FramesSent, ps.FramesRecv, ps.FramesRetransmit,
		ps.Successes, ps.Successes+ps.Failures,
		float64(ps.Successes)/float64(ps.Successes+ps.Failures)*100,
		ps.AvgRTT, ps.MinRTT, ps.MaxRTT,
		ps.LastActivity,
	)
}
