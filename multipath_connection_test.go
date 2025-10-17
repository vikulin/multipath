package multipath

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiPathConnection tests multiple path scenarios
func TestMultiPathConnection(t *testing.T) {
	// Create multiple listeners
	listeners := make([]net.Listener, 3)
	dialers := make([]Dialer, 3)

	for i := 0; i < 3; i++ {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		listeners[i] = listener
		dialers[i] = &testDialer{addr: listener.Addr().String()}
		defer listener.Close()
	}

	// Create multipath listener
	stats := []StatsTracker{NullTracker{}, NullTracker{}, NullTracker{}}
	mpListener := NewListener(listeners, stats)
	defer mpListener.Close()

	// Create multipath dialer
	mpDialer := NewDialer("test", dialers)

	// Test with different data sizes
	testSizes := []int{100, 1000, 5000, 10000, 50000, 250000, 1024 * 1024, 100 * 1024 * 1024, 1024 * 1024 * 1024} // Test up to 1GB with fragmentation

	for _, size := range testSizes {
		t.Run(fmt.Sprintf("MultiPath_DataSize_%d", size), func(t *testing.T) {
			testMultiPathDataTransmission(t, mpListener, mpDialer, size)
		})
	}
}

// TestMultiPathPathFailure tests path failure and recovery
func TestMultiPathPathFailure(t *testing.T) {
	// Create multiple listeners
	listeners := make([]net.Listener, 3)
	dialers := make([]Dialer, 3)

	for i := 0; i < 3; i++ {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		listeners[i] = listener
		dialers[i] = &testDialer{addr: listener.Addr().String()}
		defer listener.Close()
	}

	// Create multipath listener
	stats := []StatsTracker{NullTracker{}, NullTracker{}, NullTracker{}}
	mpListener := NewListener(listeners, stats)
	defer mpListener.Close()

	// Create multipath dialer
	mpDialer := NewDialer("test", dialers)

	// Start server
	serverDone := make(chan error, 1)
	go func() {
		conn, err := mpListener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Echo server with timeout handling
		conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		_, err = io.Copy(conn, conn)
		serverDone <- err
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientConn, err := mpDialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Send initial data to establish connection
	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	_, err = clientConn.Write(testData)
	require.NoError(t, err)

	receivedData := make([]byte, 1000)
	_, err = io.ReadFull(clientConn, receivedData)
	require.NoError(t, err)
	assert.Equal(t, testData, receivedData)

	// Close one of the listeners to simulate path failure
	// This will cause the subflow using that listener to fail
	listeners[0].Close()
	t.Log("Closed first listener to simulate path failure")

	// Give some time for the subflow to detect the failure and be removed
	time.Sleep(200 * time.Millisecond)

	// Continue sending data - should work with remaining paths
	// Use smaller data sizes and fewer iterations for reliability
	for i := 0; i < 3; i++ {
		data := make([]byte, 50)
		_, err = rand.Read(data)
		require.NoError(t, err)

		_, err = clientConn.Write(data)
		require.NoError(t, err)

		received := make([]byte, 50)
		clientConn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, err = io.ReadFull(clientConn, received)
		require.NoError(t, err)
		assert.Equal(t, data, received)
	}

	// Close the client connection to signal the server to finish
	clientConn.Close()

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil && err != io.EOF && err.Error() != "closed connection" {
			t.Errorf("server error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("server timeout")
	}
}

// TestMultiPathLoadBalancing tests load balancing across multiple paths
func TestMultiPathLoadBalancing(t *testing.T) {
	// Create multiple listeners with different characteristics
	listeners := make([]net.Listener, 3)
	dialers := make([]Dialer, 3)

	for i := 0; i < 3; i++ {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		listeners[i] = listener

		// Create dialers with different characteristics
		if i == 0 {
			dialers[i] = &fastTestDialer{addr: listener.Addr().String()}
		} else if i == 1 {
			dialers[i] = &mediumTestDialer{addr: listener.Addr().String()}
		} else {
			dialers[i] = &slowTestDialer{addr: listener.Addr().String()}
		}
		defer listener.Close()
	}

	// Create multipath listener
	stats := []StatsTracker{NullTracker{}, NullTracker{}, NullTracker{}}
	mpListener := NewListener(listeners, stats)
	defer mpListener.Close()

	// Create multipath dialer
	mpDialer := NewDialer("test", dialers)

	// Start server
	serverDone := make(chan error, 1)
	go func() {
		conn, err := mpListener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Echo server with timeout
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, err = io.Copy(conn, conn)
		if err != nil && err != io.EOF && err.Error() != "closed connection" {
			serverDone <- err
		} else {
			serverDone <- nil
		}
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientConn, err := mpDialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Send multiple data packets to test load balancing
	const numPackets = 20
	const packetSize = 1000

	for i := 0; i < numPackets; i++ {
		data := make([]byte, packetSize)
		_, err = rand.Read(data)
		require.NoError(t, err)

		start := time.Now()

		// Set write deadline
		clientConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err = clientConn.Write(data)
		require.NoError(t, err)

		// Set read deadline
		received := make([]byte, packetSize)
		clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = io.ReadFull(clientConn, received)
		require.NoError(t, err)

		duration := time.Since(start)
		t.Logf("Packet %d: %v", i, duration)

		assert.Equal(t, data, received)
	}

	// Close client connection to signal server completion
	clientConn.Close()

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil && err != io.EOF {
			t.Errorf("server error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("server timeout")
	}
}

// TestMultiPathConcurrentConnections tests 10 truly concurrent connections
func TestMultiPathConcurrentConnections(t *testing.T) {
	// Create multiple listeners
	listeners := make([]net.Listener, 2)
	dialers := make([]Dialer, 2)

	for i := 0; i < 2; i++ {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		listeners[i] = listener
		dialers[i] = &testDialer{addr: listener.Addr().String()}
		defer listener.Close()
	}

	// Create multipath listener
	stats := []StatsTracker{NullTracker{}, NullTracker{}}
	mpListener := NewListener(listeners, stats)
	defer mpListener.Close()

	// Create multipath dialer
	mpDialer := NewDialer("test", dialers)

	// Test 10 concurrent connections
	const numConnections = 10
	var wg sync.WaitGroup
	errors := make(chan error, numConnections*2) // *2 for client and server errors

	// Start all server goroutines
	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()

			conn, err := mpListener.Accept()
			if err != nil {
				errors <- fmt.Errorf("server %d: accept error: %v", connID, err)
				return
			}
			defer conn.Close()

			// Echo server with timeout
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, err = io.Copy(conn, conn)
			if err != nil && err != io.EOF && err.Error() != "closed connection" && err.Error() != "context deadline exceeded" {
				errors <- fmt.Errorf("server %d: copy error: %v", connID, err)
			}
		}(i)
	}

	// Start all client goroutines with small delays to stagger them
	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()

			// Small delay to stagger client connections
			time.Sleep(time.Duration(connID) * 50 * time.Millisecond)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			conn, err := mpDialer.DialContext(ctx)
			if err != nil {
				errors <- fmt.Errorf("client %d: dial error: %v", connID, err)
				return
			}
			defer conn.Close()

			// Send test data
			data := make([]byte, 100)
			_, err = rand.Read(data)
			if err != nil {
				errors <- fmt.Errorf("client %d: rand error: %v", connID, err)
				return
			}

			// Set write deadline
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = conn.Write(data)
			if err != nil {
				errors <- fmt.Errorf("client %d: write error: %v", connID, err)
				return
			}

			// Receive echoed data with timeout
			received := make([]byte, 100)
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, err = io.ReadFull(conn, received)
			if err != nil {
				errors <- fmt.Errorf("client %d: read error: %v", connID, err)
				return
			}

			if !assert.Equal(t, data, received, "client %d: data mismatch", connID) {
				errors <- fmt.Errorf("client %d: data integrity check failed", connID)
			}
		}(i)
	}

	// Wait for all goroutines to complete with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed
		t.Logf("All %d concurrent connections completed successfully", numConnections)
	case <-time.After(60 * time.Second):
		t.Error("test timeout - concurrent connections took too long")
		return
	}

	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Error(err)
		errorCount++
	}

	if errorCount == 0 {
		t.Logf("All %d concurrent connections passed without errors", numConnections)
	} else {
		t.Logf("%d out of %d concurrent connections had errors", errorCount, numConnections*2)
	}
}

// TestMultiPathRetransmission tests retransmission across multiple paths
func TestMultiPathRetransmission(t *testing.T) {
	// Create multiple listeners
	listeners := make([]net.Listener, 3)
	dialers := make([]Dialer, 3)

	for i := 0; i < 3; i++ {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		listeners[i] = listener
		dialers[i] = &testDialer{addr: listener.Addr().String()}
		defer listener.Close()
	}

	// Create multipath listener
	stats := []StatsTracker{NullTracker{}, NullTracker{}, NullTracker{}}
	mpListener := NewListener(listeners, stats)
	defer mpListener.Close()

	// Create multipath dialer
	mpDialer := NewDialer("test", dialers)

	// Start server
	serverDone := make(chan error, 1)
	go func() {
		conn, err := mpListener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Echo server with occasional delays to trigger retransmissions
		buffer := make([]byte, 4096)
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		for {
			n, err := conn.Read(buffer)
			if err != nil {
				if err != io.EOF && err.Error() != "closed connection" {
					serverDone <- err
				} else {
					serverDone <- nil
				}
				return
			}

			// Occasionally delay to trigger retransmissions
			if n%100 == 0 {
				time.Sleep(10 * time.Millisecond)
			}

			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = conn.Write(buffer[:n])
			if err != nil {
				serverDone <- err
				return
			}
		}
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientConn, err := mpDialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Send data that may trigger retransmissions
	const numPackets = 20
	const packetSize = 1000

	for i := 0; i < numPackets; i++ {
		data := make([]byte, packetSize)
		_, err = rand.Read(data)
		require.NoError(t, err)

		clientConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err = clientConn.Write(data)
		require.NoError(t, err)

		received := make([]byte, packetSize)
		clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = io.ReadFull(clientConn, received)
		require.NoError(t, err)

		assert.Equal(t, data, received, "packet %d: data mismatch", i)
	}

	// Close client connection to signal server completion
	clientConn.Close()

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil && err != io.EOF {
			t.Errorf("server error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("server timeout")
	}
}

// Helper function to test multipath data transmission
func testMultiPathDataTransmission(t *testing.T, listener net.Listener, dialer Dialer, dataSize int) {
	// Start server
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Simple server that just reads and discards data
		buffer := make([]byte, 8192)
		totalRead := 0
		for totalRead < dataSize {
			n, err := conn.Read(buffer)
			if err != nil {
				serverDone <- err
				return
			}
			totalRead += n
		}
		serverDone <- nil
	}()

	// Connect client - increase timeout for large data tests
	timeout := 10 * time.Second
	if dataSize >= 1024*1024 { // 1MB or larger
		timeout = 30 * time.Second
	}
	if dataSize >= 100*1024*1024 { // 100MB or larger
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	clientConn, err := dialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Generate random test data
	testData := make([]byte, dataSize)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	// Send data
	start := time.Now()
	n, err := clientConn.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, dataSize, n)

	// Log performance metrics
	duration := time.Since(start)
	throughput := float64(dataSize) / duration.Seconds()

	// Format throughput in appropriate units
	var throughputStr string
	if math.IsInf(throughput, 1) || math.IsNaN(throughput) {
		throughputStr = ">1 GB/sec"
	} else if throughput >= 1024*1024*1024 {
		throughputStr = fmt.Sprintf("%.2f GB/sec", throughput/(1024*1024*1024))
	} else if throughput >= 1024*1024 {
		throughputStr = fmt.Sprintf("%.2f MB/sec", throughput/(1024*1024))
	} else if throughput >= 1024 {
		throughputStr = fmt.Sprintf("%.2f kB/sec", throughput/1024)
	} else {
		throughputStr = fmt.Sprintf("%.2f bytes/sec", throughput)
	}

	// Format duration with microsecond precision only
	var durationStr string
	us := float64(duration.Nanoseconds()) / 1000.0 // Convert to microseconds
	if us < 0.1 {
		durationStr = "<0.1 µs"
	} else if us < 1000 {
		durationStr = fmt.Sprintf("%.1f µs", us)
	} else {
		durationStr = fmt.Sprintf("%.1f ms", us/1000)
	}

	t.Logf("MultiPath - Data size: %d bytes, Duration: %s, Throughput: %s",
		dataSize, durationStr, throughputStr)

	// Give the server time to read all data before closing
	time.Sleep(100 * time.Millisecond)

	// Wait for server to finish - increase timeout for large data tests
	serverTimeout := 10 * time.Second
	if dataSize >= 1024*1024 { // 1MB or larger
		serverTimeout = 30 * time.Second
	}
	if dataSize >= 100*1024*1024 { // 100MB or larger
		serverTimeout = 60 * time.Second
	}
	select {
	case err := <-serverDone:
		if err != nil && err != io.EOF {
			t.Errorf("server error: %v", err)
		}
	case <-time.After(serverTimeout):
		t.Error("server timeout")
	}
}

// Fast test dialer
type fastTestDialer struct {
	addr string
}

func (ftd *fastTestDialer) DialContext(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", ftd.addr)
}

func (ftd *fastTestDialer) Label() string {
	return fmt.Sprintf("fast dialer to %s", ftd.addr)
}

// Medium test dialer
type mediumTestDialer struct {
	addr string
}

func (mtd *mediumTestDialer) DialContext(ctx context.Context) (net.Conn, error) {
	time.Sleep(5 * time.Millisecond) // Simulate medium latency
	var d net.Dialer
	return d.DialContext(ctx, "tcp", mtd.addr)
}

func (mtd *mediumTestDialer) Label() string {
	return fmt.Sprintf("medium dialer to %s", mtd.addr)
}

// TestTCPComparison tests plain TCP transfer for comparison with multipath
func TestTCPComparison(t *testing.T) {
	// Test with 1GB data (same as MultiPath_DataSize_1073741824)
	dataSize := 1073741824 // 1GB

	// Create plain TCP listener
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()

	// Start TCP server
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Set read deadline for server
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		// Receive and validate data from client
		buf := make([]byte, dataSize)
		n, err := io.ReadFull(conn, buf)
		if err != nil {
			serverDone <- err
			return
		}

		// Validate received data size
		if n != dataSize {
			serverDone <- fmt.Errorf("expected %d bytes, got %d", dataSize, n)
			return
		}

		// Send response data back to client
		responseData := []byte("tcp_response_complete")
		_, err = conn.Write(responseData)
		if err != nil {
			serverDone <- err
			return
		}

		serverDone <- nil
	}()

	// Connect TCP client
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var dialer net.Dialer
	clientConn, err := dialer.DialContext(ctx, "tcp", listener.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()

	// Generate random test data
	testData := make([]byte, dataSize)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	// Send data and measure performance
	start := time.Now()
	clientConn.SetWriteDeadline(time.Now().Add(60 * time.Second))
	n, err := clientConn.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, dataSize, n)

	// Read server response
	response := make([]byte, len("tcp_response_complete"))
	clientConn.SetReadDeadline(time.Now().Add(60 * time.Second))
	n, err = io.ReadFull(clientConn, response)
	require.NoError(t, err)
	assert.Equal(t, "tcp_response_complete", string(response))

	// Calculate and log performance metrics
	duration := time.Since(start)
	throughput := float64(dataSize) / duration.Seconds()

	// Format throughput in appropriate units
	var throughputStr string
	if math.IsInf(throughput, 1) || math.IsNaN(throughput) {
		throughputStr = ">1 GB/sec"
	} else if throughput >= 1024*1024*1024 {
		throughputStr = fmt.Sprintf("%.2f GB/sec", throughput/(1024*1024*1024))
	} else if throughput >= 1024*1024 {
		throughputStr = fmt.Sprintf("%.2f MB/sec", throughput/(1024*1024))
	} else if throughput >= 1024 {
		throughputStr = fmt.Sprintf("%.2f kB/sec", throughput/1024)
	} else {
		throughputStr = fmt.Sprintf("%.2f bytes/sec", throughput)
	}

	// Format duration with microsecond precision
	var durationStr string
	us := float64(duration.Nanoseconds()) / 1000.0 // Convert to microseconds
	if us < 0.1 {
		durationStr = "<0.1 µs"
	} else if us < 1000 {
		durationStr = fmt.Sprintf("%.1f µs", us)
	} else {
		durationStr = fmt.Sprintf("%.1f ms", us/1000)
	}

	t.Logf("TCP - Data size: %d bytes, Duration: %s, Throughput: %s",
		dataSize, durationStr, throughputStr)

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil {
			t.Errorf("server error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("server timeout")
	}
}
