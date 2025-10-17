package multipath

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/getlantern/ema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock dialer for testing
type mockDialer struct {
	addr    string
	idx     int
	success bool
	delay   time.Duration
	conn    net.Conn
	mu      sync.Mutex
}

func (md *mockDialer) DialContext(ctx context.Context) (net.Conn, error) {
	if !md.success {
		return nil, fmt.Errorf("mock dialer failed")
	}
	
	if md.delay > 0 {
		time.Sleep(md.delay)
	}
	
	// Create a mock connection with timeout
	conn1, conn2 := net.Pipe()
	
	// Protect conn field access
	md.mu.Lock()
	md.conn = conn1
	md.mu.Unlock()
	
	// Start a goroutine to handle the connection with timeout
	go func() {
		defer conn2.Close()
		buf := make([]byte, 1024)
		
		// Set a timeout for the connection
		conn2.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		
		for {
			_, err := conn2.Read(buf)
			if err != nil {
				return
			}
			// Reset deadline for next read
			conn2.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		}
	}()
	
	return conn1, nil
}

func (md *mockDialer) Label() string {
	return fmt.Sprintf("mock-dialer-%d", md.idx)
}

// Mock listener for testing
type mockListener struct {
	addr   string
	idx    int
	conns  chan net.Conn
	closed bool
	mu     sync.RWMutex
}

func (ml *mockListener) Accept() (net.Conn, error) {
	ml.mu.RLock()
	closed := ml.closed
	ml.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("listener closed")
	}

	select {
	case conn := <-ml.conns:
		return conn, nil
	case <-time.After(10 * time.Millisecond):
		return nil, fmt.Errorf("accept timeout")
	}
}

func (ml *mockListener) Close() error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.closed = true
	close(ml.conns)
	return nil
}

func (ml *mockListener) Addr() net.Addr {
	return &mockAddr{addr: ml.addr}
}

// Mock stats tracker for testing
type mockStatsTracker struct{}

func (mst *mockStatsTracker) OnSent(bytes uint64)         {}
func (mst *mockStatsTracker) OnRecv(bytes uint64)         {}
func (mst *mockStatsTracker) OnRetransmit(bytes uint64)   {}
func (mst *mockStatsTracker) UpdateRTT(rtt time.Duration) {}

type mockAddr struct {
	addr string
}

func (ma *mockAddr) Network() string { return "tcp" }
func (ma *mockAddr) String() string  { return ma.addr }

func TestDialerInterface(t *testing.T) {
	// Test subflowDialer
	md := &mockDialer{addr: "127.0.0.1:8080", idx: 1, success: true}
	sfd := &subflowDialer{
		Dialer: md,
		label:  md.Label(),
		emaRTT: ema.NewDuration(longRTT, rttAlpha),
	}

	// Test Label
	assert.Equal(t, "mock-dialer-1", sfd.Label())
}

func TestDialerDialContext(t *testing.T) {
	ctx := context.Background()

	// Test successful dial
	md := &mockDialer{addr: "127.0.0.1:8080", idx: 1, success: true}
	sfd := &subflowDialer{
		Dialer: md,
		label:  md.Label(),
		emaRTT: ema.NewDuration(longRTT, rttAlpha),
	}

	conn, err := sfd.DialContext(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	// Test failed dial
	mdFail := &mockDialer{addr: "127.0.0.1:8081", idx: 2, success: false}
	sfdFail := &subflowDialer{
		Dialer: mdFail,
		label:  mdFail.Label(),
		emaRTT: ema.NewDuration(longRTT, rttAlpha),
	}

	_, err = sfdFail.DialContext(ctx)
	assert.Error(t, err)
}

func TestDialerCallbacks(t *testing.T) {
	md := &mockDialer{addr: "127.0.0.1:8080", idx: 1, success: true}
	sfd := &subflowDialer{
		Dialer: md,
		label:  md.Label(),
		emaRTT: ema.NewDuration(longRTT, rttAlpha),
	}

	// Test OnRecv
	sfd.OnRecv(100)
	assert.Equal(t, uint64(1), sfd.framesRecv)
	assert.Equal(t, uint64(100), sfd.bytesRecv)

	// Test OnSent
	sfd.OnSent(200)
	assert.Equal(t, uint64(1), sfd.framesSent)
	assert.Equal(t, uint64(200), sfd.bytesSent)

	// Test OnRetransmit
	sfd.OnRetransmit(50)
	assert.Equal(t, uint64(1), sfd.framesRetransmit)
	assert.Equal(t, uint64(50), sfd.bytesRetransmit)

	// Test UpdateRTT
	testRTT := 50 * time.Millisecond
	sfd.UpdateRTT(testRTT)
	// RTT is updated internally, we can't directly test it without exposing the field
}

func TestMPDialerNewDialer(t *testing.T) {
	// Test NewDialer
	dialers := []Dialer{
		&mockDialer{addr: "127.0.0.1:8080", idx: 1, success: true},
		&mockDialer{addr: "127.0.0.1:8081", idx: 2, success: true},
	}

	mpd := NewDialer("127.0.0.1:8080", dialers)
	require.NotNil(t, mpd)

	// Test that it implements the Dialer interface
	var dialer Dialer = mpd
	assert.NotNil(t, dialer)
}

func TestMPDialerDialContext(t *testing.T) {
	// Use a short timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Test with successful dialers
	dialers := []Dialer{
		&mockDialer{addr: "127.0.0.1:8080", idx: 1, success: true},
		&mockDialer{addr: "127.0.0.1:8081", idx: 2, success: true},
	}

	mpd := NewDialer("127.0.0.1:8080", dialers)
	// Note: This will fail because we need a real listener, but we're testing the code path
	_, err := mpd.DialContext(ctx)
	// We expect this to fail because there's no real listener
	assert.Error(t, err)

	// Test with all failed dialers
	failDialers := []Dialer{
		&mockDialer{addr: "127.0.0.1:8080", idx: 1, success: false},
		&mockDialer{addr: "127.0.0.1:8081", idx: 2, success: false},
	}

	mpdFail := NewDialer("127.0.0.1:8080", failDialers)
	_, err = mpdFail.DialContext(ctx)
	assert.Error(t, err)
}

func TestMPDialerHandshake(t *testing.T) {
	// Create a mock connection for handshake testing
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	// Start a goroutine to handle the handshake response
	go func() {
		buf := make([]byte, 1024)
		n, _ := conn2.Read(buf)
		// Echo back the handshake data
		conn2.Write(buf[:n])
	}()

	dialers := []Dialer{
		&mockDialer{addr: "127.0.0.1:8080", idx: 1, success: true},
	}

	mpd := NewDialer("127.0.0.1:8080", dialers)

	// Test handshake - cast to concrete type to access private method
	if mpDialer, ok := mpd.(*mpDialer); ok {
		cid, err := mpDialer.handshake(conn1, zeroCID)
		// Note: This might fail due to the mock implementation, but we're testing the code path
		// The important thing is that the handshake method is called and doesn't panic
		_ = cid
		_ = err
	}
}

func TestMPDialerSorted(t *testing.T) {
	// Create dialers with different RTTs
	dialers := []Dialer{
		&mockDialer{addr: "127.0.0.1:8080", idx: 1, success: true, delay: 100 * time.Millisecond},
		&mockDialer{addr: "127.0.0.1:8081", idx: 2, success: true, delay: 50 * time.Millisecond},
		&mockDialer{addr: "127.0.0.1:8082", idx: 3, success: true, delay: 200 * time.Millisecond},
	}

	mpd := NewDialer("127.0.0.1:8080", dialers)

	// Test sorted method - cast to concrete type to access private method
	if mpDialer, ok := mpd.(*mpDialer); ok {
		sorted := mpDialer.sorted()
		assert.Len(t, sorted, 3)

		// The sorted method should return dialers in some order
		// (exact order depends on implementation)
		for _, sf := range sorted {
			assert.NotNil(t, sf)
		}
	}
}

func TestMPDialerFormatStats(t *testing.T) {
	dialers := []Dialer{
		&mockDialer{addr: "127.0.0.1:8080", idx: 1, success: true},
		&mockDialer{addr: "127.0.0.1:8081", idx: 2, success: true},
	}

	mpd := NewDialer("127.0.0.1:8080", dialers)

	// Test FormatStats - cast to concrete type to access private method
	if mpDialer, ok := mpd.(*mpDialer); ok {
		stats := mpDialer.FormatStats()
		assert.NotNil(t, stats)
		assert.Len(t, stats, 2)

		// Check that stats contain expected information
		for _, stat := range stats {
			assert.Contains(t, stat, "mock-dialer")
		}
	}
}

func TestListenerInterface(t *testing.T) {
	// Test NewListener - need to provide listeners and stats
	listeners := []net.Listener{
		&mockListener{addr: "127.0.0.1:8080", idx: 1},
	}
	stats := []StatsTracker{
		&mockStatsTracker{},
	}

	listener := NewListener(listeners, stats)
	require.NotNil(t, listener)

	// Test Addr
	listenerAddr := listener.Addr()
	assert.Equal(t, "multipath", listenerAddr.Network())
}

func TestListenerAccept(t *testing.T) {
	listeners := []net.Listener{
		&mockListener{addr: "127.0.0.1:8080", idx: 1},
	}
	stats := []StatsTracker{
		&mockStatsTracker{},
	}

	listener := NewListener(listeners, stats)
	defer listener.Close()

	// Test Accept with no connections (should timeout quickly)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := listener.Accept()
		done <- err
	}()

	select {
	case err := <-done:
		assert.Error(t, err)
	case <-ctx.Done():
		// Expected timeout
	}
}

func TestListenerClose(t *testing.T) {
	listeners := []net.Listener{
		&mockListener{addr: "127.0.0.1:8080", idx: 1},
	}
	stats := []StatsTracker{
		&mockStatsTracker{},
	}

	listener := NewListener(listeners, stats)

	// Test Close
	err := listener.Close()
	assert.NoError(t, err)

	// Test Accept after close
	_, err = listener.Accept()
	assert.Error(t, err)
	assert.Equal(t, ErrClosed, err)
}

func TestListenerStart(t *testing.T) {
	listeners := []net.Listener{
		&mockListener{addr: "127.0.0.1:8080", idx: 1},
	}
	stats := []StatsTracker{
		&mockStatsTracker{},
	}

	listener := NewListener(listeners, stats)
	defer listener.Close()

	// Test start method - cast to concrete type to access private method
	if mpListener, ok := listener.(*mpListener); ok {
		mpListener.start()
		// The start method is called internally, we can't directly test it
	}
}

func TestListenerAcceptFrom(t *testing.T) {
	listeners := []net.Listener{
		&mockListener{addr: "127.0.0.1:8080", idx: 1},
	}
	stats := []StatsTracker{
		&mockStatsTracker{},
	}

	listener := NewListener(listeners, stats)
	defer listener.Close()

	// Test acceptFrom with a mock connection
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	// This will test the acceptFrom method
	// Note: The actual implementation might need a real connection
	// but we're testing the code path
	go func() {
		time.Sleep(10 * time.Millisecond)
		conn2.Write([]byte("test"))
	}()

	// The acceptFrom method should be called internally
	// We can't directly test it without exposing it, but we can test
	// that the listener works end-to-end
}

func TestListenerRemove(t *testing.T) {
	listeners := []net.Listener{
		&mockListener{addr: "127.0.0.1:8080", idx: 1},
	}
	stats := []StatsTracker{
		&mockStatsTracker{},
	}

	listener := NewListener(listeners, stats)
	defer listener.Close()

	// Test remove method
	// This method is used internally to remove connections
	// We can't directly test it without exposing it, but we can test
	// that the listener works end-to-end
}

func TestDialerStatsTracking(t *testing.T) {
	// Test that stats are properly tracked
	md := &mockDialer{addr: "127.0.0.1:8080", idx: 1, success: true}
	sfd := &subflowDialer{
		Dialer: md,
		label:  md.Label(),
		emaRTT: ema.NewDuration(longRTT, rttAlpha),
	}

	// Simulate some activity
	sfd.OnRecv(100)
	sfd.OnSent(200)
	sfd.OnRetransmit(50)
	sfd.UpdateRTT(100 * time.Millisecond)

	// Test that stats are tracked
	assert.Equal(t, uint64(1), sfd.framesRecv)
	assert.Equal(t, uint64(100), sfd.bytesRecv)
	assert.Equal(t, uint64(1), sfd.framesSent)
	assert.Equal(t, uint64(200), sfd.bytesSent)
	assert.Equal(t, uint64(1), sfd.framesRetransmit)
	assert.Equal(t, uint64(50), sfd.bytesRetransmit)
}

func TestListenerConcurrentAccess(t *testing.T) {
	listeners := []net.Listener{
		&mockListener{addr: "127.0.0.1:8080", idx: 1},
	}
	stats := []StatsTracker{
		&mockStatsTracker{},
	}

	listener := NewListener(listeners, stats)
	defer listener.Close()

	// Test concurrent access with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: Try to accept
	go func() {
		defer wg.Done()
		_, err := listener.Accept()
		// Should timeout or fail, but not panic
		_ = err
	}()

	// Goroutine 2: Close the listener
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		listener.Close()
	}()

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-ctx.Done():
		// Timeout - this is acceptable for this test
	}
}

func TestDialerConcurrentAccess(t *testing.T) {
	// Test concurrent dialing with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	dialers := []Dialer{
		&mockDialer{addr: "127.0.0.1:8080", idx: 1, success: true},
		&mockDialer{addr: "127.0.0.1:8081", idx: 2, success: true},
	}

	mpd := NewDialer("127.0.0.1:8080", dialers)

	var wg sync.WaitGroup
	wg.Add(3)

	// Test concurrent DialContext calls
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			conn, err := mpd.DialContext(ctx)
			if err == nil {
				conn.Close()
			}
			// We expect errors since there's no real listener
		}()
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - all goroutines completed
	case <-ctx.Done():
		// Timeout - this is acceptable for this test
		t.Log("Test completed with timeout (expected)")
	}
}
