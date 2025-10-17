package multipath

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRead(t *testing.T) {
	q := newReceiveQueue(2)
	fn := uint64(minFrameNumber - 1)
	addFrame := func(s string) {
		fn++
		q.add(&rxFrame{fn: fn, bytes: []byte(s)}, nil)
	}
	shouldRead := func(s string) {
		b := make([]byte, 3)
		n, err := q.read(b)
		assert.NoError(t, err)
		assert.Equal(t, s, string(b[:n]))
	}

	addFrame("abcd")
	shouldRead("abc")
	addFrame("abcd")
	shouldRead("dab")
	shouldRead("cd")
	addFrame("abcd")
	// adding the same frame number again should have no effect
	q.add(&rxFrame{fn: fn, bytes: []byte("1234")}, nil)
	shouldRead("abc")
	shouldRead("d")

	shouldWaitBeforeRead := func(d time.Duration, s string) {
		start := time.Now()
		b := make([]byte, 3)
		n, err := q.read(b)
		assert.NoError(t, err)
		assert.Equal(t, s, string(b[:n]))
		assert.InDelta(t, time.Since(start), d, float64(50*time.Millisecond))
	}
	delay := 100 * time.Millisecond
	time.AfterFunc(delay, func() {
		addFrame("abcd")
	})
	shouldWaitBeforeRead(delay, "abc")
	time.AfterFunc(delay, func() {
		addFrame("abc")
	})
	shouldWaitBeforeRead(0, "d")
	shouldWaitBeforeRead(delay, "abc")

	// frames can be added out of order
	q.add(&rxFrame{fn: fn + 2, bytes: []byte("1234")}, nil)
	time.AfterFunc(delay, func() {
		addFrame("abcd")
	})
	shouldWaitBeforeRead(delay, "abc")
	shouldWaitBeforeRead(0, "d12")
}

func TestReadRXQEarlyClose(t *testing.T) {
	q := newReceiveQueue(10)
	fn := uint64(minFrameNumber - 1)
	addFrame := func(s string) {
		fn++
		q.add(&rxFrame{fn: fn, bytes: []byte(s)}, nil)
	}
	shouldRead := func(s string) {
		b := make([]byte, 5)
		n, err := q.read(b)
		assert.NoError(t, err)
		assert.Equal(t, s, string(b[:n]))
	}

	addFrame("Hello")
	shouldRead("Hello")
	addFrame("World")
	addFrame("Burld")
	q.close()
	time.Sleep(time.Millisecond * 101)
	shouldRead("World")
	shouldRead("Burld")
	b := make([]byte, 10)
	_, err := q.read(b)
	if err != ErrClosed {
		t.FailNow()
	}
}

// TestReceiveQueueConcurrentAccess tests concurrent access to receive queue
func TestReceiveQueueConcurrentAccess(t *testing.T) {
	rq := newReceiveQueue(100)

	// Start reader goroutine
	readDone := make(chan bool, 1)
	var readData []byte
	var readMutex sync.Mutex

	go func() {
		defer func() { readDone <- true }()

		buffer := make([]byte, 10)
		for {
			n, err := rq.read(buffer)
			if err != nil {
				if err == ErrClosed {
					return
				}
				t.Errorf("Read error: %v", err)
				return
			}

			readMutex.Lock()
			readData = append(readData, buffer[:n]...)
			readMutex.Unlock()
		}
	}()

	// Start writer goroutines
	writeDone := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { writeDone <- true }()

			for j := 0; j < 10; j++ {
				frame := &rxFrame{
					fn:    uint64(10 + id*10 + j),
					bytes: []byte{byte(id), byte(j)},
				}
				rq.add(frame, nil)
			}
		}(i)
	}

	// Wait for writers to complete
	for i := 0; i < 10; i++ {
		<-writeDone
	}

	// Close queue to signal reader to stop
	rq.close()

	// Wait for reader to complete
	<-readDone

	// Verify all data was read
	readMutex.Lock()
	defer readMutex.Unlock()
	assert.Equal(t, 200, len(readData)) // 10 writers * 10 frames * 2 bytes per frame
}

// TestReceiveQueueDeadline tests read deadline functionality
func TestReceiveQueueDeadline(t *testing.T) {
	rq := newReceiveQueue(10)

	// Set read deadline
	deadline := time.Now().Add(100 * time.Millisecond)
	rq.setReadDeadline(deadline)

	// Try to read with deadline
	buffer := make([]byte, 10)
	start := time.Now()
	_, err := rq.read(buffer)
	duration := time.Since(start)

	// Should timeout
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "deadline exceeded")
	assert.True(t, duration >= 100*time.Millisecond)
	assert.True(t, duration < 200*time.Millisecond) // Allow some tolerance
}

// TestReceiveQueueDeadlineWithData tests deadline with data available
func TestReceiveQueueDeadlineWithData(t *testing.T) {
	rq := newReceiveQueue(10)

	// Add frame
	frame1 := &rxFrame{fn: 10, bytes: []byte("hello")}
	rq.add(frame1, nil)

	// Set read deadline
	deadline := time.Now().Add(100 * time.Millisecond)
	rq.setReadDeadline(deadline)

	// Read should succeed immediately
	buffer := make([]byte, 10)
	n, err := rq.read(buffer)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(buffer[:n]))
}

// TestReceiveQueueFullBuffer tests full buffer handling
func TestReceiveQueueFullBuffer(t *testing.T) {
	rq := newReceiveQueue(3) // Small buffer

	// Fill buffer
	frame1 := &rxFrame{fn: 10, bytes: []byte("a")}
	frame2 := &rxFrame{fn: 11, bytes: []byte("b")}
	frame3 := &rxFrame{fn: 12, bytes: []byte("c")}

	assert.True(t, rq.tryAdd(frame1))
	assert.True(t, rq.tryAdd(frame2))
	assert.True(t, rq.tryAdd(frame3))

	// Buffer should be full
	assert.True(t, rq.isFull())

	// Try to add another frame - should fail
	frame4 := &rxFrame{fn: 13, bytes: []byte("d")}
	success := rq.tryAdd(frame4)
	assert.False(t, success)
}

// TestReceiveQueueBufferCorruption tests buffer corruption detection
func TestReceiveQueueBufferCorruption(t *testing.T) {
	rq := newReceiveQueue(10)

	// Manually corrupt the buffer to test corruption detection
	rq.buf[0] = rxFrame{fn: 10, bytes: []byte("hello")}
	rq.buf[1] = rxFrame{fn: 12, bytes: []byte("world")} // Skip frame 11

	// Set read pointer to start
	rq.rp = 0

	// Try to read - should detect corruption
	buffer := make([]byte, 10)
	_, err := rq.read(buffer)

	// Should detect corruption and close
	assert.Error(t, err)
	assert.Equal(t, ErrClosed, err)
}

// TestReceiveQueueEmptyRead tests reading from empty queue
func TestReceiveQueueEmptyRead(t *testing.T) {
	rq := newReceiveQueue(10)

	// Try to read from empty queue
	buffer := make([]byte, 10)

	// This should block, so we'll use a goroutine with timeout
	readDone := make(chan bool, 1)

	go func() {
		_, err := rq.read(buffer)
		readDone <- true
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}()

	// Wait a bit to ensure it's blocking
	time.Sleep(50 * time.Millisecond)

	// Add frame to unblock
	frame := &rxFrame{fn: 10, bytes: []byte("hello")}
	rq.add(frame, nil)

	// Wait for read to complete
	select {
	case <-readDone:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("Read should have completed")
	}
}


