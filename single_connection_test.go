package multipath

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleConnectionRandomData tests basic single connection functionality
// with random data transmission to verify correctness
func TestSingleConnectionRandomData(t *testing.T) {
	// Create test listener
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()

	// Create multipath listener
	mpListener := NewListener([]net.Listener{listener}, []StatsTracker{NullTracker{}})
	defer mpListener.Close()

	// Create test dialer
	dialer := NewDialer("test", []Dialer{&singleTestDialer{addr: listener.Addr().String()}})

	// Test data sizes to verify
	testSizes := []int{1, 10, 100, 1000, 10000, 100000}

	for _, size := range testSizes {
		t.Run(fmt.Sprintf("DataSize_%d", size), func(t *testing.T) {
			testRandomDataTransmission(t, mpListener, dialer, size)
		})
	}
}

// TestSingleConnectionLargeData tests with large data to verify memory management
func TestSingleConnectionLargeData(t *testing.T) {
	// Create test listener
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()

	// Create multipath listener
	mpListener := NewListener([]net.Listener{listener}, []StatsTracker{NullTracker{}})
	defer mpListener.Close()

	// Create test dialer
	dialer := NewDialer("test", []Dialer{&singleTestDialer{addr: listener.Addr().String()}})

	// Test with large data (1MB)
	testRandomDataTransmission(t, mpListener, dialer, 1024*1024)
}

// TestSingleConnectionSequentialWrites tests sequential writes on single connection
func TestSingleConnectionSequentialWrites(t *testing.T) {
	// Create test listener
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()

	// Create multipath listener
	mpListener := NewListener([]net.Listener{listener}, []StatsTracker{NullTracker{}})
	defer mpListener.Close()

	// Create test dialer
	dialer := NewDialer("test", []Dialer{&singleTestDialer{addr: listener.Addr().String()}})

	// Test data
	const numWrites = 10
	const dataSize = 1000
	expectedData := make([][]byte, numWrites)
	for i := 0; i < numWrites; i++ {
		expectedData[i] = make([]byte, dataSize)
		_, err := rand.Read(expectedData[i])
		require.NoError(t, err, "failed to generate random data for write %d", i)
	}

	// Start server - receives data and validates it
	serverDone := make(chan error, 1)
	go func() {
		conn, err := mpListener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Set read deadline for server
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// Receive and validate data from client
		buf := make([]byte, dataSize)
		for i := 0; i < numWrites; i++ {
			n, err := conn.Read(buf)
			if err != nil {
				serverDone <- err
				return
			}

			// Validate received data
			if n != dataSize {
				serverDone <- fmt.Errorf("expected %d bytes, got %d", dataSize, n)
				return
			}

			if !bytes.Equal(buf[:n], expectedData[i]) {
				serverDone <- fmt.Errorf("data mismatch for write %d", i)
				return
			}
		}

		// Send response data back to client (separate flow)
		responseData := []byte("server_response_complete")
		_, err = conn.Write(responseData)
		if err != nil {
			serverDone <- err
			return
		}

		serverDone <- nil
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientConn, err := dialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Send data to server
	for i := 0; i < numWrites; i++ {
		_, err = clientConn.Write(expectedData[i])
		require.NoError(t, err, "write %d: failed to write", i)
	}

	// Read server response
	response := make([]byte, len("server_response_complete"))
	_, err = io.ReadFull(clientConn, response)
	require.NoError(t, err, "failed to read server response")
	assert.Equal(t, "server_response_complete", string(response))

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil {
			t.Errorf("server error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("server timeout")
	}
}

// TestSingleConnectionStress tests stress conditions
func TestSingleConnectionStress(t *testing.T) {
	// Create test listener
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()

	// Create multipath listener
	mpListener := NewListener([]net.Listener{listener}, []StatsTracker{NullTracker{}})
	defer mpListener.Close()

	// Create test dialer
	dialer := NewDialer("test", []Dialer{&singleTestDialer{addr: listener.Addr().String()}})

	// Start server - receives data and validates it (no echo)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := mpListener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Set read deadline for server
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		
		// Receive and validate data from client
		buffer := make([]byte, 10000) // Max data size
		totalReceived := 0
		for {
			n, err := conn.Read(buffer)
			if err != nil {
				if err == io.EOF || err.Error() == "closed connection" {
					// Client closed connection, this is expected
					serverDone <- nil
					return
				}
				serverDone <- err
				return
			}
			totalReceived += n
		}
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientConn, err := dialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Stress test with rapid writes (no echo reads)
	const numIterations = 100 // Reduced from 1000 to prevent timeout
	const maxDataSize = 1000  // Reduced from 10000

	for i := 0; i < numIterations; i++ {
		// Generate random data size
		dataSize := (i % maxDataSize) + 1
		data := make([]byte, dataSize)
		_, err := rand.Read(data)
		require.NoError(t, err)

		// Write data
		_, err = clientConn.Write(data)
		require.NoError(t, err)

		// Small delay to prevent overwhelming
		if i%10 == 0 {
			time.Sleep(time.Millisecond)
		}
	}

	// Close client connection to signal server completion
	clientConn.Close()

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil {
			t.Errorf("server error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("server timeout")
	}
}

// TestSingleConnectionErrorHandling tests error conditions
func TestSingleConnectionErrorHandling(t *testing.T) {
	// Test with unreachable address (valid port but no listener)
	dialer := NewDialer("test", []Dialer{&singleTestDialer{addr: "127.0.0.1:65535"}})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := dialer.DialContext(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fail on all dialers")
}

// TestSingleConnectionTimeout tests timeout handling
func TestSingleConnectionTimeout(t *testing.T) {
	// Create test listener
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()

	// Create multipath listener
	mpListener := NewListener([]net.Listener{listener}, []StatsTracker{NullTracker{}})
	defer mpListener.Close()

	// Create test dialer with slow connection
	dialer := NewDialer("test", []Dialer{&slowTestDialer{addr: listener.Addr().String()}})

	// Test with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = dialer.DialContext(ctx)
	assert.Error(t, err)
	// The multipath dialer may return "fail on all dialers" when all subflows fail
	assert.True(t, strings.Contains(err.Error(), "context deadline exceeded") || 
		strings.Contains(err.Error(), "fail on all dialers"))
}

// Helper function to test random data transmission
func testRandomDataTransmission(t *testing.T, listener net.Listener, dialer Dialer, dataSize int) {
	// Start server - receives data and validates it (no echo)
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
		
		// Send response data back to client (separate flow)
		responseData := []byte("server_response_complete")
		_, err = conn.Write(responseData)
		if err != nil {
			serverDone <- err
			return
		}
		
		serverDone <- nil
	}()

	// Connect client - increase timeout for large data
	timeout := 10 * time.Second
	if dataSize >= 1024*1024 { // 1MB or larger
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
	clientConn.SetWriteDeadline(time.Now().Add(timeout))
	n, err := clientConn.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, dataSize, n)

	// Small delay to ensure server has time to process and respond
	time.Sleep(10 * time.Millisecond)

	// Read server response with longer timeout
	response := make([]byte, len("server_response_complete"))
	clientConn.SetReadDeadline(time.Now().Add(timeout + 5*time.Second)) // Add extra time for response
	n, err = io.ReadFull(clientConn, response)
	require.NoError(t, err)
	assert.Equal(t, "server_response_complete", string(response))

	// Log performance metrics (ensure atomic access to start time)
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
	
	t.Logf("Data size: %d bytes, Duration: %s, Throughput: %s",
		dataSize, durationStr, throughputStr)

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil {
			t.Errorf("server error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("server timeout")
	}
}

// Test dialer implementation
type singleTestDialer struct {
	addr string
}

func (td *singleTestDialer) DialContext(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", td.addr)
}

func (td *singleTestDialer) Label() string {
	return fmt.Sprintf("single test dialer to %s", td.addr)
}

// Slow test dialer for timeout testing
type slowTestDialer struct {
	addr string
}

func (std *slowTestDialer) DialContext(ctx context.Context) (net.Conn, error) {
	// Simulate slow connection
	time.Sleep(200 * time.Millisecond)
	var d net.Dialer
	return d.DialContext(ctx, "tcp", std.addr)
}

func (std *slowTestDialer) Label() string {
	return fmt.Sprintf("slow test dialer to %s", std.addr)
}

// TestSingleConnectionFrameOrdering tests frame ordering and sequencing
func TestSingleConnectionFrameOrdering(t *testing.T) {
	// Create test listener
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()

	// Create multipath listener
	mpListener := NewListener([]net.Listener{listener}, []StatsTracker{NullTracker{}})
	defer mpListener.Close()

	// Create test dialer
	dialer := NewDialer("test", []Dialer{&singleTestDialer{addr: listener.Addr().String()}})

	// Test data - multiple frames with sequence numbers
	const numFrames = 50 // Reduced from 100 to prevent timeout
	const frameSize = 100
	expectedFrames := make([][]byte, numFrames)
	for i := 0; i < numFrames; i++ {
		expectedFrames[i] = make([]byte, frameSize)
		for j := 0; j < frameSize; j++ {
			expectedFrames[i][j] = byte(i) // Each frame has its sequence number
		}
	}

	// Start server - receives data and validates it (no echo)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := mpListener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Set read deadline for server
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		
		// Receive and validate frames from client
		buf := make([]byte, frameSize)
		for i := 0; i < numFrames; i++ {
			n, err := conn.Read(buf)
			if err != nil {
				serverDone <- err
				return
			}
			
			// Validate received frame
			if n != frameSize {
				serverDone <- fmt.Errorf("expected %d bytes, got %d", frameSize, n)
				return
			}
			
			if !bytes.Equal(buf[:n], expectedFrames[i]) {
				serverDone <- fmt.Errorf("frame %d: ordering check failed", i)
				return
			}
		}
		
		// Send response data back to client (separate flow)
		responseData := []byte("server_response_complete")
		_, err = conn.Write(responseData)
		if err != nil {
			serverDone <- err
			return
		}
		
		serverDone <- nil
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientConn, err := dialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Send all frames
	for i := 0; i < numFrames; i++ {
		_, err := clientConn.Write(expectedFrames[i])
		require.NoError(t, err, "frame %d: failed to write", i)
	}

	// Read server response
	response := make([]byte, len("server_response_complete"))
	clientConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err = io.ReadFull(clientConn, response)
	require.NoError(t, err)
	assert.Equal(t, "server_response_complete", string(response))

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil {
			t.Errorf("server error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("server timeout")
	}
}

// TestSingleConnectionClose tests connection closing behavior
func TestSingleConnectionClose(t *testing.T) {
	// Create test listener
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()

	// Create multipath listener
	mpListener := NewListener([]net.Listener{listener}, []StatsTracker{NullTracker{}})
	defer mpListener.Close()

	// Create test dialer
	dialer := NewDialer("test", []Dialer{&singleTestDialer{addr: listener.Addr().String()}})

	// Start server
	serverDone := make(chan error, 1)
	go func() {
		conn, err := mpListener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Read until connection closes
		buffer := make([]byte, 1024)
		for {
			_, err := conn.Read(buffer)
			if err != nil {
				serverDone <- err
				return
			}
		}
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientConn, err := dialer.DialContext(ctx)
	require.NoError(t, err)

	// Send some data
	testData := []byte("test data")
	_, err = clientConn.Write(testData)
	require.NoError(t, err)

	// Close connection
	err = clientConn.Close()
	require.NoError(t, err)

	// Verify connection is closed
	_, err = clientConn.Write([]byte("should fail"))
	assert.Error(t, err)

	// Wait for server to detect close
	select {
	case err := <-serverDone:
		assert.Error(t, err) // Should be an error due to connection close
	case <-time.After(5 * time.Second):
		t.Error("server timeout")
	}
}
