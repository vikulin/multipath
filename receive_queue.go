package multipath

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pool "github.com/libp2p/go-buffer-pool"
)

// receiveQueue keeps received frames for the upper layer to read. It is
// maintained as a ring buffer with fixed size. It takes advantage of the fact
// that the frame number is sequential, so when a new frame arrives, it is
// placed at buf[frameNumber % size].
type receiveQueue struct {
	readFrameTip uint64
	buf          []rxFrame
	size         uint64
	// rp stands for read pointer, point to the index of the frame containing
	// data yet to be read.
	rp                    uint64
	availableFrameChannel chan bool
	readNotifyChannel     chan bool
	readDeadline          time.Time
	deadlineLock          sync.RWMutex
	closing               uint32 // 1 == true, 0 == false  -- This is used to "drain" the Queue
	fullyClosed           uint32 // 1 == true, 0 == false
	readLock              *sync.Mutex
	// Fragment reassembly
	fragments    map[uint64]*fragmentInfo // key: base frame number
	fragmentLock sync.RWMutex
}

func newReceiveQueue(size int) *receiveQueue {
	rq := &receiveQueue{
		buf:                   make([]rxFrame, size),
		size:                  uint64(size),
		rp:                    minFrameNumber % uint64(size), // frame number starts with minFrameNumber, so should the read pointer
		readFrameTip:          minFrameNumber - 1,            // Initialize to minFrameNumber - 1 so first frame can be added
		availableFrameChannel: make(chan bool, 1),
		readNotifyChannel:     make(chan bool),
		readLock:              &sync.Mutex{},
		fragments:             make(map[uint64]*fragmentInfo),
	}
	return rq
}

// addFragment handles fragment reassembly
func (rq *receiveQueue) addFragment(f *rxFrame, sf *subflow) bool {
	if len(f.bytes) < 2 {
		return false // Not a valid fragment
	}

	totalFragments := f.bytes[0]
	fragmentIndex := f.bytes[1]
	fragmentData := f.bytes[2:]

	// Calculate base frame number (assuming fragments are consecutive)
	baseFrameNum := f.fn - uint64(fragmentIndex)

	rq.fragmentLock.Lock()
	defer rq.fragmentLock.Unlock()

	// Get or create fragment info
	fragInfo, exists := rq.fragments[baseFrameNum]
	if !exists {
		fragInfo = &fragmentInfo{
			totalFragments: totalFragments,
			fragments:      make([][]byte, totalFragments),
			receivedCount:  0,
			baseFrameNum:   baseFrameNum,
		}
		rq.fragments[baseFrameNum] = fragInfo
	}

	// Store the fragment
	if int(fragmentIndex) < len(fragInfo.fragments) {
		fragInfo.fragments[fragmentIndex] = make([]byte, len(fragmentData))
		copy(fragInfo.fragments[fragmentIndex], fragmentData)
		fragInfo.receivedCount++
	}

	// Check if all fragments are received
	if fragInfo.receivedCount == totalFragments {
		// Reassemble the complete payload
		var completePayload []byte
		for _, frag := range fragInfo.fragments {
			completePayload = append(completePayload, frag...)
		}

		// Create a regular frame with the complete payload
		completeFrame := &rxFrame{
			fn:    baseFrameNum,
			bytes: completePayload,
		}

		// Add the complete frame to the regular queue (avoid recursive call)
		// We need to acquire the readLock to safely write to the buffer
		rq.readLock.Lock()
		idx := completeFrame.fn % rq.size
		rq.buf[idx] = *completeFrame
		rq.readLock.Unlock()
		
		// Notify that data is available
		select {
		case rq.availableFrameChannel <- true:
		default:
		}

		// Clean up fragment info
		delete(rq.fragments, baseFrameNum)
		return true
	}

	return false // Not all fragments received yet
}

func (rq *receiveQueue) add(f *rxFrame, sf *subflow) {
	// Check if this is a fragment that needs reassembly
	// Only treat as fragment if it has reasonable fragment metadata
	if len(f.bytes) >= 2 {
		totalFragments := f.bytes[0]
		fragmentIndex := f.bytes[1]

		// More strict fragment detection:
		// - totalFragments must be > 1 and <= 255 (reasonable range)
		// - fragmentIndex must be < totalFragments
		// - frame number should be part of a sequence (fragments are consecutive)
		// - Only treat as fragment if the first two bytes look like reasonable fragment metadata
		if totalFragments > 1 && totalFragments <= 10 && fragmentIndex < totalFragments && fragmentIndex < 10 {
			// Additional check: fragments should have meaningful data (not just 2 bytes)
			if len(f.bytes) > 2 {
				if rq.addFragment(f, sf) {
					return // Fragment was reassembled and added
				}
				return // Fragment received but not complete yet
			}
		}
	}

	select {
	case rq.availableFrameChannel <- true:
	default:
	}
	// Another thing to protect against, is that we might be
	// locally blocked on a full receiveQueue. If that is the
	// case then we don't want to return instantly from this
	// function since that will just case retransmits to fire
	// over and over again, causing mass bandwidth loss.
	// Instead let's quickly check if we have all of the data we need
	// to read, and if we do, hang until we don't have that problem anymore
	if rq.isFull() {
		for rq.isFull() {
			<-rq.readNotifyChannel
		}
	}
	select {
	case rq.availableFrameChannel <- true:
	default:
	}

	readFrameTip := atomic.LoadUint64(&rq.readFrameTip)

	if readFrameTip != 0 {
		if readFrameTip > f.fn {
			sf.ack(f.fn)
			return
		}
		// If readFrameTip == f.fn, we should still try to add it
		// as it might be a retransmission or the first frame
	}

	if f.fn > readFrameTip+rq.size && readFrameTip != 0 {
		log.Debugf("Near corruption incident?? %v vs the max peek of %v (frametip %d)", f.fn, readFrameTip+rq.size-1, readFrameTip)
		return // Nope! this will corrupt the buffer
	}

	if rq.tryAdd(f) {
		sf.ack(f.fn)
		return
	}

	// Protect against the socket being closed
	if atomic.LoadUint32(&rq.fullyClosed) == 1 {
		pool.Put(f.bytes)
		return
	}

}

func (rq *receiveQueue) isFull() bool {
	rq.readLock.Lock()
	defer rq.readLock.Unlock()

	// Count non-empty slots
	nonEmptyCount := uint64(0)
	for i := uint64(0); i < rq.size; i++ {
		if rq.buf[i].bytes != nil {
			nonEmptyCount++
		}
	}

	return nonEmptyCount == rq.size
}

func (rq *receiveQueue) tryAdd(f *rxFrame) bool {
	rq.readLock.Lock()
	idx := f.fn % rq.size
	if rq.buf[idx].bytes == nil {
		// empty slot
		rq.buf[idx] = *f
		if idx == rq.rp {
			select {
			case rq.availableFrameChannel <- true:
			default:
			}
		}
		rq.readLock.Unlock()
		return true
	} else if rq.buf[idx].fn == f.fn {
		rq.readLock.Unlock()
		// retransmission, ignore
		log.Tracef("Got a retransmit. for %d", f.fn)
		pool.Put(f.bytes)
		return true
	}
	rq.readLock.Unlock()

	if idx != 0 {
		log.Tracef("Not what I was looking for, I'm looking for frame %v", rq.buf[idx-1].fn+1)
	}
	return false
}

func (rq *receiveQueue) read(b []byte) (int, error) {
	for {
		// Check for data availability and read in a single lock acquisition
		rq.readLock.Lock()
		hasData := rq.buf[rq.rp].bytes != nil

		if hasData {
			// We have data, process it in the same lock
			totalN := 0
			cur := rq.buf[rq.rp].bytes
			for cur != nil && totalN < len(b) {
				// Get current frame tip atomically
				expectedFrameNumber := atomic.LoadUint64(&rq.readFrameTip) + 1
				currentFrameNumber := rq.buf[rq.rp].fn

				// Validate frame sequence with better error handling
				if currentFrameNumber != expectedFrameNumber && expectedFrameNumber != 1 {
					// log.Errorf("receiveQueue buffer corruption detected [%v vs %v] (The crash happened at idx = %d)", currentFrameNumber, expectedFrameNumber, rq.rp)
					// log.Tracef("All Buffers: ")
					// for idx, v := range rq.buf {
					// 	log.Tracef("\t[%d]fn %d, [%d]byte\n", idx, v.fn, len(v.bytes))
					// }
					rq.close()
					rq.readLock.Unlock()
					return 0, ErrClosed
				}

				// Copy data from current frame
				copySize := len(cur)
				if copySize > len(b)-totalN {
					copySize = len(b) - totalN
				}
				copy(b[totalN:], cur[:copySize])
				totalN += copySize

				// If we copied all the data from this frame, consume it
				if copySize == len(cur) {
					// Move to next frame
					atomic.StoreUint64(&rq.readFrameTip, currentFrameNumber)
					rq.buf[rq.rp].bytes = nil
					rq.rp = (rq.rp + 1) % rq.size
					cur = rq.buf[rq.rp].bytes
				} else {
					// We only copied part of the frame, update the frame to point to remaining data
					rq.buf[rq.rp].bytes = cur[copySize:]
					cur = rq.buf[rq.rp].bytes
				}
			}
			rq.readLock.Unlock()
			return totalN, nil
		}
		rq.readLock.Unlock()

		if atomic.LoadUint32(&rq.fullyClosed) == 1 {
			return 0, ErrClosed
		}
		if atomic.LoadUint32(&rq.closing) == 1 {
			// if we are closing, then we should check if there is anything left to send
			// before sending ErrClosed back upstream, otherwise we may close "early" with
			// some data still inside of us!
			break
		}

		if rq.dlExceeded() {
			return 0, context.DeadlineExceeded
		}

		select {
		case rq.readNotifyChannel <- true:
		default:
		}
		
		// Wait for data with timeout
		select {
		case <-rq.availableFrameChannel:
			// Data available, continue loop
		case <-time.After(50 * time.Millisecond):
			// Check deadline again after timeout
			if rq.dlExceeded() {
				return 0, context.DeadlineExceeded
			}
			// Continue waiting if deadline not exceeded
		}
	}

	// This should never be reached due to the loop above
	return 0, ErrClosed
}

func (rq *receiveQueue) setReadDeadline(dl time.Time) {
	rq.deadlineLock.Lock()
	rq.readDeadline = dl
	rq.deadlineLock.Unlock()
	if !dl.IsZero() {
		ttl := time.Until(dl)
		if ttl <= 0 {
			for {
				abort := false
				select {
				case rq.availableFrameChannel <- true:
				default:
					abort = true
				}
				if abort {
					break
				}
			}
		} else {
			time.AfterFunc(ttl, func() {
				rq.availableFrameChannel <- true
			})
		}
	}
}

func (rq *receiveQueue) dlExceeded() bool {
	rq.deadlineLock.RLock()
	deadline := rq.readDeadline
	rq.deadlineLock.RUnlock()
	return !deadline.IsZero() && !deadline.After(time.Now())
}

func (rq *receiveQueue) close() {
	atomic.StoreUint32(&rq.closing, 1)
	abort := false

	for {
		select {
		case rq.availableFrameChannel <- true:
		default:
			abort = true
		}
		if abort {
			break
		}
	}
}
