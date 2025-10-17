package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/vikulin/multipath"
)

func main() {
	// Create custom stats tracker
	statsTracker := &performanceTracker{}

	// Create individual dialers for different paths
	dialers := []multipath.Dialer{
		&tcpDialer{addr: "localhost:8080"},
		&tcpDialer{addr: "localhost:8081"},
		&tcpDialer{addr: "localhost:8082"},
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

	// Send large data for performance testing
	dataSize := 1024 * 1024 // 1MB
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	fmt.Printf("Sending %d bytes of test data...\n", dataSize)

	start := time.Now()

	// Send data
	_, err = conn.Write(testData)
	if err != nil {
		log.Fatalf("Write error: %v", err)
	}

	// Read response
	response := make([]byte, dataSize)
	_, err = conn.Read(response)
	if err != nil {
		log.Fatalf("Read error: %v", err)
	}

	duration := time.Since(start)
	throughput := float64(dataSize) / duration.Seconds()

	fmt.Printf("Transfer completed in %v\n", duration)
	fmt.Printf("Throughput: %.2f MB/sec\n", throughput/(1024*1024))

	// Print final stats
	statsTracker.printStats()
}

// Custom dialer implementation
type tcpDialer struct {
	addr string
}

func (d *tcpDialer) DialContext(ctx context.Context) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", d.addr)
}

func (d *tcpDialer) Label() string {
	return fmt.Sprintf("TCP dialer to %s", d.addr)
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
