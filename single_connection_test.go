package multipath

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sync"
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

// TestSingleConnectionConcurrentWrites tests concurrent writes on single connection
func TestSingleConnectionConcurrentWrites(t *testing.T) {
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

		// Echo server
		_, err = io.Copy(conn, conn)
		serverDone <- err
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientConn, err := dialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Test concurrent writes
	const numGoroutines = 10
	const dataSize = 1000
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Generate random data
			data := make([]byte, dataSize)
			_, err := rand.Read(data)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to generate random data: %v", id, err)
				return
			}

			// Write data
			_, err = clientConn.Write(data)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to write: %v", id, err)
				return
			}

			// Read echoed data
			received := make([]byte, dataSize)
			_, err = io.ReadFull(clientConn, received)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to read: %v", id, err)
				return
			}

			// Verify data integrity
			if !assert.Equal(t, data, received, "goroutine %d: data mismatch", id) {
				errors <- fmt.Errorf("goroutine %d: data integrity check failed", id)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil && err != io.EOF {
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

	// Start server
	serverDone := make(chan error, 1)
	go func() {
		conn, err := mpListener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Echo server with some processing delay
		buffer := make([]byte, 4096)
		for {
			n, err := conn.Read(buffer)
			if err != nil {
				if err != io.EOF {
					serverDone <- err
				}
				return
			}
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

	clientConn, err := dialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Stress test with rapid writes and reads
	const numIterations = 1000
	const maxDataSize = 10000

	for i := 0; i < numIterations; i++ {
		// Generate random data size
		dataSize := (i % maxDataSize) + 1
		data := make([]byte, dataSize)
		_, err := rand.Read(data)
		require.NoError(t, err)

		// Write data
		_, err = clientConn.Write(data)
		require.NoError(t, err)

		// Read echoed data
		received := make([]byte, dataSize)
		_, err = io.ReadFull(clientConn, received)
		require.NoError(t, err)

		// Verify data integrity
		assert.Equal(t, data, received, "iteration %d: data mismatch", i)

		// Small delay to prevent overwhelming
		if i%100 == 0 {
			time.Sleep(time.Millisecond)
		}
	}

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil && err != io.EOF {
			t.Errorf("server error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("server timeout")
	}
}

// TestSingleConnectionErrorHandling tests error conditions
func TestSingleConnectionErrorHandling(t *testing.T) {
	// Test with invalid address
	dialer := NewDialer("test", []Dialer{&testDialer{addr: "127.0.0.1:99999"}})

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
	assert.Contains(t, err.Error(), "context deadline exceeded")
}

// Helper function to test random data transmission
func testRandomDataTransmission(t *testing.T, listener net.Listener, dialer Dialer, dataSize int) {
	// Start server
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Echo server
		_, err = io.Copy(conn, conn)
		serverDone <- err
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	// Receive echoed data
	receivedData := make([]byte, dataSize)
	n, err = io.ReadFull(clientConn, receivedData)
	require.NoError(t, err)
	assert.Equal(t, dataSize, n)

	// Verify data integrity
	assert.Equal(t, testData, receivedData, "data integrity check failed")

	// Log performance metrics
	duration := time.Since(start)
	throughput := float64(dataSize) / duration.Seconds()
	t.Logf("Data size: %d bytes, Duration: %v, Throughput: %.2f bytes/sec",
		dataSize, duration, throughput)

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil && err != io.EOF {
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

	// Start server
	serverDone := make(chan error, 1)
	go func() {
		conn, err := mpListener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		// Echo server
		_, err = io.Copy(conn, conn)
		serverDone <- err
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientConn, err := dialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Send multiple small frames to test ordering
	const numFrames = 100
	const frameSize = 100

	for i := 0; i < numFrames; i++ {
		// Create frame with sequence number
		frameData := make([]byte, frameSize)
		for j := 0; j < frameSize; j++ {
			frameData[j] = byte(i) // Each frame has its sequence number
		}

		// Send frame
		_, err := clientConn.Write(frameData)
		require.NoError(t, err)

		// Receive echoed frame
		receivedFrame := make([]byte, frameSize)
		_, err = io.ReadFull(clientConn, receivedFrame)
		require.NoError(t, err)

		// Verify frame ordering
		assert.Equal(t, frameData, receivedFrame, "frame %d: ordering check failed", i)
	}

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil && err != io.EOF {
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
