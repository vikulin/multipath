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
	testSizes := []int{100, 1000, 10000, 100000}

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

		// Echo server
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
	listeners[0].Close()
	t.Log("Closed first listener to simulate path failure")

	// Continue sending data - should work with remaining paths
	for i := 0; i < 10; i++ {
		data := make([]byte, 100)
		_, err = rand.Read(data)
		require.NoError(t, err)

		_, err = clientConn.Write(data)
		require.NoError(t, err)

		received := make([]byte, 100)
		_, err = io.ReadFull(clientConn, received)
		require.NoError(t, err)
		assert.Equal(t, data, received)
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

		// Echo server
		_, err = io.Copy(conn, conn)
		serverDone <- err
	}()

	// Connect client
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientConn, err := mpDialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Send multiple data packets to test load balancing
	const numPackets = 50
	const packetSize = 1000

	for i := 0; i < numPackets; i++ {
		data := make([]byte, packetSize)
		_, err = rand.Read(data)
		require.NoError(t, err)

		start := time.Now()
		_, err = clientConn.Write(data)
		require.NoError(t, err)

		received := make([]byte, packetSize)
		_, err = io.ReadFull(clientConn, received)
		require.NoError(t, err)

		duration := time.Since(start)
		t.Logf("Packet %d: %v", i, duration)

		assert.Equal(t, data, received)
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

// TestMultiPathConcurrentConnections tests multiple concurrent connections
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

	// Test multiple concurrent connections
	const numConnections = 5
	var wg sync.WaitGroup
	errors := make(chan error, numConnections*2) // *2 for client and server errors

	// Start server
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

			// Echo server
			_, err = io.Copy(conn, conn)
			if err != nil && err != io.EOF {
				errors <- fmt.Errorf("server %d: copy error: %v", connID, err)
			}
		}(i)
	}

	// Start clients
	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			conn, err := mpDialer.DialContext(ctx)
			if err != nil {
				errors <- fmt.Errorf("client %d: dial error: %v", connID, err)
				return
			}
			defer conn.Close()

			// Send test data
			data := make([]byte, 1000)
			_, err = rand.Read(data)
			if err != nil {
				errors <- fmt.Errorf("client %d: rand error: %v", connID, err)
				return
			}

			_, err = conn.Write(data)
			if err != nil {
				errors <- fmt.Errorf("client %d: write error: %v", connID, err)
				return
			}

			// Receive echoed data
			received := make([]byte, 1000)
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

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
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
		for {
			n, err := conn.Read(buffer)
			if err != nil {
				if err != io.EOF {
					serverDone <- err
				}
				return
			}

			// Occasionally delay to trigger retransmissions
			if n%100 == 0 {
				time.Sleep(10 * time.Millisecond)
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

	clientConn, err := mpDialer.DialContext(ctx)
	require.NoError(t, err)
	defer clientConn.Close()

	// Send data that may trigger retransmissions
	const numPackets = 100
	const packetSize = 1000

	for i := 0; i < numPackets; i++ {
		data := make([]byte, packetSize)
		_, err = rand.Read(data)
		require.NoError(t, err)

		_, err = clientConn.Write(data)
		require.NoError(t, err)

		received := make([]byte, packetSize)
		_, err = io.ReadFull(clientConn, received)
		require.NoError(t, err)

		assert.Equal(t, data, received, "packet %d: data mismatch", i)
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
	t.Logf("MultiPath - Data size: %d bytes, Duration: %v, Throughput: %.2f bytes/sec",
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


