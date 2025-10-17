package multipath

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock connection for testing
type mockConn struct {
	readData  []byte
	writeData []byte
	closed    bool
	mu        sync.RWMutex
}

func (mc *mockConn) Read(b []byte) (n int, err error) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	if mc.closed {
		return 0, net.ErrClosed
	}

	if len(mc.readData) == 0 {
		return 0, nil
	}

	n = copy(b, mc.readData)
	mc.readData = mc.readData[n:]
	return n, nil
}

func (mc *mockConn) Write(b []byte) (n int, err error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.closed {
		return 0, net.ErrClosed
	}

	mc.writeData = append(mc.writeData, b...)
	return len(b), nil
}

func (mc *mockConn) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.closed = true
	return nil
}

func (mc *mockConn) LocalAddr() net.Addr {
	return &mockAddr{addr: "127.0.0.1:8080"}
}

func (mc *mockConn) RemoteAddr() net.Addr {
	return &mockAddr{addr: "127.0.0.1:8081"}
}

func (mc *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (mc *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (mc *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (mc *mockConn) SetReadData(data []byte) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.readData = data
}

func (mc *mockConn) GetWriteData() []byte {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.writeData
}

func (mc *mockConn) IsClosed() bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.closed
}

func TestNewMPConn(t *testing.T) {
	// Test creating a new multipath connection
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	require.NotNil(t, conn)
	assert.NotNil(t, conn.LocalAddr())
	assert.NotNil(t, conn.RemoteAddr())
	assert.Equal(t, "multipath", conn.LocalAddr().Network())
}

func TestMPConnRead(t *testing.T) {
	// Test reading from multipath connection
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Test reading with no data (should timeout quickly)
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buffer := make([]byte, 10)
	n, err := conn.Read(buffer)
	assert.Equal(t, 0, n)
	assert.Error(t, err) // Should timeout
}

func TestMPConnWrite(t *testing.T) {
	// Test writing to multipath connection
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Add a subflow for writing
	conn.add("127.0.0.1:8081", &mockConn{}, true, time.Now(), &mockStatsTracker{})

	// Test writing data
	data := []byte("test data")
	n, err := conn.Write(data)
	assert.Equal(t, len(data), n)
	assert.NoError(t, err)
}

func TestMPConnClose(t *testing.T) {
	// Test closing multipath connection
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)

	// Test closing
	err := conn.Close()
	assert.NoError(t, err)

	// Test writing after close
	_, err = conn.Write([]byte("test"))
	assert.Error(t, err)
}

func TestMPConnDeadlines(t *testing.T) {
	// Test deadline methods
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Test setting deadlines
	deadline := time.Now().Add(time.Second)
	err := conn.SetDeadline(deadline)
	assert.NoError(t, err)

	err = conn.SetReadDeadline(deadline)
	assert.NoError(t, err)

	err = conn.SetWriteDeadline(deadline)
	assert.NoError(t, err)
}

func TestMPConnAddresses(t *testing.T) {
	// Test address methods
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Test local address
	localAddr := conn.LocalAddr()
	assert.NotNil(t, localAddr)
	assert.Equal(t, "multipath", localAddr.Network())

	// Test remote address
	remoteAddrResult := conn.RemoteAddr()
	assert.NotNil(t, remoteAddrResult)
}

func TestMPConnString(t *testing.T) {
	// Test string representation
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Test local address string
	localAddr := conn.LocalAddr()
	str := localAddr.String()
	assert.Contains(t, str, "multipath")
}

func TestMPConnAddSubflow(t *testing.T) {
	// Test adding subflows
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Test adding a new subflow
	conn.add("127.0.0.1:8081", &mockConn{}, true, time.Now(), &mockStatsTracker{})

	// Verify subflow was added (without waiting for cleanup)
	sorted := conn.sortedSubflows()
	assert.Len(t, sorted, 1)
}

func TestMPConnRemoveSubflow(t *testing.T) {
	// Test removing subflows
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Add a subflow first
	conn.add("127.0.0.1:8081", &mockConn{}, true, time.Now(), &mockStatsTracker{})

	// Get the subflow to remove
	sorted := conn.sortedSubflows()
	require.Len(t, sorted, 1)

	// Test removing the subflow
	conn.remove(sorted[0])

	// Verify subflow was removed
	sorted = conn.sortedSubflows()
	assert.Len(t, sorted, 0)
}

func TestMPConnIsPendingAck(t *testing.T) {
	// Test pending ACK check
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Test checking pending ACK
	exists := conn.isPendingAck(123)
	assert.False(t, exists)
}

func TestMPConnConcurrentAccess(t *testing.T) {
	// Test concurrent access to multipath connection
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Test concurrent reads with timeout
	var wg sync.WaitGroup
	done := make(chan bool, 1)
	
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			buffer := make([]byte, 10)
			_, err := conn.Read(buffer)
			// Should timeout since no data is available
			if err != nil {
				// Expected timeout error
				return
			}
		}(i)
	}

	// Use a goroutine to wait for completion with overall timeout
	go func() {
		wg.Wait()
		done <- true
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Test completed successfully
	case <-time.After(5 * time.Second):
		t.Error("Test timed out - possible deadlock")
	}
}

func TestMPConnRetransmitLoop(t *testing.T) {
	// Test retransmit loop (this will run in background)
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Give the retransmit loop time to start
	time.Sleep(10 * time.Millisecond)

	// Test that the connection is still functional (basic operations)
	localAddr := conn.LocalAddr()
	assert.NotNil(t, localAddr)
	assert.Equal(t, "multipath", localAddr.Network())
}

func TestMPConnSelectSubflowForRetransmit(t *testing.T) {
	// Test subflow selection for retransmission
	subflows := []*subflow{
		{to: "127.0.0.1:8080", conn: &mockConn{}},
		{to: "127.0.0.1:8081", conn: &mockConn{}},
	}

	// Test selecting subflow for retransmission
	frame := &sendFrame{fn: 1}
	abort, allTransmitted, selected := selectSubflowForRetransmit(subflows, frame, false)

	assert.False(t, abort)
	assert.False(t, allTransmitted)
	assert.NotNil(t, selected)
}

func TestMPConnSortedSubflows(t *testing.T) {
	// Test sorting subflows
	cid := connectionID{1, 2, 3, 4}
	remoteAddr := &mockAddr{addr: "127.0.0.1:8080"}

	conn := newMPConn(cid, remoteAddr)
	defer conn.Close()

	// Test sorting subflows with no subflows
	sorted := conn.sortedSubflows()
	assert.Len(t, sorted, 0)
}
