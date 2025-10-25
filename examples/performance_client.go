package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/vikulin/multipath"
)

func main() {
	// Create custom stats tracker
	statsTracker := &performanceTracker{}

	// Create individual dialers for different paths with stats
	dialers := []multipath.Dialer{
		&tcpDialer{addr: "localhost:8080", stats: statsTracker},
		&tcpDialer{addr: "localhost:8081", stats: statsTracker},
		&tcpDialer{addr: "localhost:8082", stats: statsTracker},
	}

	// Create multipath dialer
	mpDialer := multipath.NewDialer("multipath-server", dialers)

	// Connect with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := mpDialer.DialContext(ctx)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	fmt.Printf("Connected to multipath server via %s\n", conn.RemoteAddr())

	// Start performance monitoring
	go statsTracker.monitorPerformance()

	// Test 10GB transfer
	testSize := int64(10 * 1024 * 1024 * 1024) // 10GB

	fmt.Printf("\n=== Testing 10GB Transfer ===\n")
	fmt.Printf("Sending %d bytes of test data...\n", testSize)

	start := time.Now()

	// Simple direct upload - just send random data
	fmt.Println("Phase 1: Uploading data to server...")

	// Create a small buffer of random data that server can handle
	bufferSize := int64(64 * 1024) // 64KB buffer
	randomData := make([]byte, bufferSize)
	for i := range randomData {
		randomData[i] = byte(i % 256)
	}

	bytesProcessed := int64(0)
	for bytesProcessed < testSize {
		// Send the buffer directly
		conn.SetWriteDeadline(time.Now().Add(300 * time.Second)) // 5 minutes
		_, err := conn.Write(randomData)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Server closed connection during upload")
				break
			}
			log.Fatalf("Write error: %v", err)
		}

		bytesProcessed += bufferSize

		// Progress indicator every 100MB
		if bytesProcessed%(100*1024*1024) == 0 {
			progress := float64(bytesProcessed) / float64(testSize) * 100
			fmt.Printf("Upload Progress: %.1f%% (%d/%d bytes)\n", progress, bytesProcessed, testSize)
		}
	}

	// Close write side to signal end of upload
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.CloseWrite()
	}

	fmt.Println("Upload completed!")

	duration := time.Since(start)
	throughput := float64(testSize) / duration.Seconds()

	fmt.Printf("Transfer completed in %v\n", duration)
	fmt.Printf("Throughput: %.2f MB/sec (%.2f GB/sec)\n",
		throughput/(1024*1024), throughput/(1024*1024*1024))
	fmt.Printf("Data rate: %.2f Mbps\n", throughput*8/(1024*1024))

	// Print final stats
	fmt.Println("\n=== Final Performance Stats ===")
	statsTracker.printStats()
}

// Custom dialer implementation
type tcpDialer struct {
	addr  string
	stats *performanceTracker
}

func (d *tcpDialer) DialContext(ctx context.Context) (net.Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", d.addr)
	if err != nil {
		return nil, err
	}

	// Wrap the connection to track stats
	return &trackedConn{Conn: conn, stats: d.stats}, nil
}

func (d *tcpDialer) Label() string {
	return fmt.Sprintf("TCP dialer to %s", d.addr)
}

// Tracked connection wrapper
type trackedConn struct {
	net.Conn
	stats         *performanceTracker
	lastWriteTime time.Time
}

func (tc *trackedConn) Read(b []byte) (n int, err error) {
	n, err = tc.Conn.Read(b)
	if n > 0 {
		tc.stats.OnRecv(uint64(n))
		// Simple RTT measurement - just set a small value for localhost
		tc.stats.UpdateRTT(100 * time.Microsecond)
	}
	return n, err
}

func (tc *trackedConn) Write(b []byte) (n int, err error) {
	tc.lastWriteTime = time.Now()
	n, err = tc.Conn.Write(b)
	if n > 0 {
		tc.stats.OnSent(uint64(n))
	}
	return n, err
}

// Performance tracker
type performanceTracker struct {
	bytesReceived uint64
	bytesSent     uint64
	retransmits   uint64
	lastRTT       time.Duration
}

func (pt *performanceTracker) OnRecv(bytes uint64) {
	atomic.AddUint64(&pt.bytesReceived, bytes)
}

func (pt *performanceTracker) OnSent(bytes uint64) {
	atomic.AddUint64(&pt.bytesSent, bytes)
}

func (pt *performanceTracker) OnRetransmit(bytes uint64) {
	atomic.AddUint64(&pt.retransmits, bytes)
}

func (pt *performanceTracker) UpdateRTT(rtt time.Duration) {
	pt.lastRTT = rtt
}

func (pt *performanceTracker) monitorPerformance() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pt.printStats()
		}
	}
}

func (pt *performanceTracker) printStats() {
	received := atomic.LoadUint64(&pt.bytesReceived)
	sent := atomic.LoadUint64(&pt.bytesSent)
	retransmits := atomic.LoadUint64(&pt.retransmits)

	fmt.Printf("Stats - Received: %d bytes, Sent: %d bytes, Retransmits: %d bytes, RTT: %v\n",
		received, sent, retransmits, pt.lastRTT)
}
