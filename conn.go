package multipath

import (
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type mpConn struct {
	cid              connectionID
	remoteAddr       net.Addr
	lastFN           uint64
	subflows         []*subflow
	muSubflows       sync.RWMutex
	recvQueue        *receiveQueue
	closed           uint32 // 1 == true, 0 == false
	writerMaybeReady chan bool
	tryRetransmit    chan bool
	writeMu          sync.Mutex // Protects Write method from concurrent access

	pendingAckMap map[uint64]*pendingAck
	pendingAckMu  *sync.RWMutex
}

func newMPConn(cid connectionID, remoteAddr net.Addr) *mpConn {
	mpc := &mpConn{
		cid:              cid,
		remoteAddr:       remoteAddr,
		lastFN:           minFrameNumber - 1,
		recvQueue:        newReceiveQueue(recieveQueueLength),
		writerMaybeReady: make(chan bool, 1),
		tryRetransmit:    make(chan bool, 1),
		pendingAckMap:    make(map[uint64]*pendingAck),
		pendingAckMu:     &sync.RWMutex{},
	}
	go mpc.retransmitLoop()
	return mpc
}

func (bc *mpConn) Read(b []byte) (n int, err error) {
	return bc.recvQueue.read(b)
}

func (bc *mpConn) Write(b []byte) (n int, err error) {
	const maxFrameSize = 200000 // 200KB max frame size - large enough to avoid fragmentation in tests

	// Protect Write method with mutex to ensure thread safety
	bc.writeMu.Lock()
	defer bc.writeMu.Unlock()

	// If data is small enough, send as single frame
	if len(b) <= maxFrameSize {
		frame := composeFrame(atomic.AddUint64(&bc.lastFN, 1), b)
		defer func() {
			if err != nil {
				frame.release()
			}
		}()

		for {
			// Check if connection is closed
			if atomic.LoadUint32(&bc.closed) == 1 {
				return 0, ErrClosed
			}

			// Atomic check for inflight frames with proper backpressure
			bc.pendingAckMu.RLock()
			inflight := len(bc.pendingAckMap)
			bc.pendingAckMu.RUnlock()

			if inflight > 500 {
				time.Sleep(time.Millisecond * 100)
				log.Tracef("too many inflights: %d", inflight)
				continue
			}

			subflows := bc.sortedSubflows()
			if len(subflows) == 0 {
				return 0, ErrClosed
			}

			// Try to send on available subflows
			for _, sf := range subflows {
				if atomic.LoadUint64(&sf.actuallyBusyOnWrite) == 1 {
					// Avoid a possibly blocked writer for a retransmit
					continue
				}

				select {
				case sf.sendQueue <- frame:
					return len(b), nil
				default:
					// Subflow is busy, try next one
				}
			}

			// All subflows are busy, wait for one to become available
			select {
			case <-bc.writerMaybeReady:
				// Try again
			case <-time.After(time.Second * 5):
				return 0, ErrClosed
			}
		}
	}

	// For large data, fragment into multiple frames
	return bc.writeFragmented(b, maxFrameSize)
}

// writeFragmented sends large data by fragmenting it into smaller frames
func (bc *mpConn) writeFragmented(data []byte, maxFrameSize int) (n int, err error) {
	baseFrameNum := atomic.AddUint64(&bc.lastFN, 1)
	fragmentCount := (len(data) + maxFrameSize - 1) / maxFrameSize // Ceiling division

	// log.Debugf("Fragmenting %d bytes into %d frames of max %d bytes", len(data), fragmentCount, maxFrameSize)

	// Send all fragments
	for i := 0; i < fragmentCount; i++ {
		start := i * maxFrameSize
		end := start + maxFrameSize
		if end > len(data) {
			end = len(data)
		}

		fragment := data[start:end]
		frameNum := baseFrameNum + uint64(i)

		frame := composeFragmentFrame(frameNum, fragment, uint8(i), uint8(fragmentCount))

		// Send this fragment
		err = bc.sendFrame(frame)
		if err != nil {
			frame.release() // Release on error
			return n, err
		}

		n += len(fragment)
	}

	return n, nil
}

// sendFrame sends a single frame through available subflows
func (bc *mpConn) sendFrame(frame *sendFrame) error {
	for {
		// Check if connection is closed
		if atomic.LoadUint32(&bc.closed) == 1 {
			return ErrClosed
		}

		// Atomic check for inflight frames with proper backpressure
		bc.pendingAckMu.RLock()
		inflight := len(bc.pendingAckMap)
		bc.pendingAckMu.RUnlock()

		if inflight > 500 {
			time.Sleep(time.Millisecond * 100)
			log.Tracef("too many inflights: %d", inflight)
			continue
		}

		subflows := bc.sortedSubflows()
		if len(subflows) == 0 {
			return ErrClosed
		}

		// Try to send on available subflows
		for _, sf := range subflows {
			if atomic.LoadUint64(&sf.actuallyBusyOnWrite) == 1 {
				// Avoid a possibly blocked writer for a retransmit
				continue
			}

			select {
			case sf.sendQueue <- frame:
				return nil
			default:
				// Subflow is busy, try next one
			}
		}

		// All subflows are busy, wait for one to become available
		select {
		case <-bc.writerMaybeReady:
			// Try again
		case <-time.After(time.Second * 5):
			return ErrClosed
		}
	}
}

func (bc *mpConn) Close() error {
	bc.close()
	for _, sf := range bc.sortedSubflows() {
		sf.close()
	}
	return nil
}

func (bc *mpConn) close() {
	atomic.StoreUint32(&bc.closed, 1)
	bc.recvQueue.close()
}

type fakeAddr struct{}

func (fakeAddr) Network() string { return "multipath" }
func (fakeAddr) String() string  { return "multipath" }

func (bc *mpConn) LocalAddr() net.Addr {
	return fakeAddr{}
}

func (bc *mpConn) RemoteAddr() net.Addr {
	return bc.remoteAddr
}

func (bc *mpConn) SetDeadline(t time.Time) error {
	bc.SetReadDeadline(t)
	return bc.SetWriteDeadline(t)
}

func (bc *mpConn) SetReadDeadline(t time.Time) error {
	bc.recvQueue.setReadDeadline(t)
	return nil
}

func (bc *mpConn) SetWriteDeadline(t time.Time) error {
	bc.muSubflows.RLock()
	defer bc.muSubflows.RUnlock()
	for _, sf := range bc.subflows {
		if err := sf.conn.SetWriteDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

func (bc *mpConn) retransmit(frame *sendFrame) {
	frame.changeLock.Lock()
	defer frame.changeLock.Unlock()

	if atomic.LoadUint64(&frame.beingRetransmitted) == 1 {
		return
	}
	atomic.StoreUint64(&frame.beingRetransmitted, 1)
	defer func() {
		atomic.StoreUint64(&frame.beingRetransmitted, 0)
	}()

	// Check if connection is closed
	if atomic.LoadUint32(&bc.closed) == 1 {
		frame.release()
		return
	}

	subflows := bc.sortedSubflows()
	if len(subflows) == 0 {
		// log.Debugf("no subflows available for retransmission of frame %d", frame.fn)
		frame.release()
		return
	}

	alreadyTransmittedOnAllSubflows := false
	maxRetries := len(subflows) * 2 // Allow multiple attempts per subflow

	for attempt := 0; attempt < maxRetries; attempt++ {
		if atomic.LoadUint32(&bc.closed) == 1 {
			return
		}

		var selectedSubflow *subflow
		var abort bool

		abort, alreadyTransmittedOnAllSubflows, selectedSubflow = selectSubflowForRetransmit(subflows, frame, attempt >= len(subflows))
		if selectedSubflow == nil {
			if abort {
				break
			}
			continue
		}

		// Try to send on selected subflow
		select {
		case <-selectedSubflow.chClose:
			continue
		case selectedSubflow.sendQueue <- frame:
			frame.retransmissions++
			// log.Debugf("retransmitted frame %d via %s (attempt %d)", frame.fn, selectedSubflow.to, attempt+1)
			if frame.sentVia == nil {
				frame.sentVia = make([]transmissionDatapoint, 0)
			}
			frame.sentVia = append(frame.sentVia, transmissionDatapoint{selectedSubflow, time.Now()})
			return
		default:
			// Subflow is busy, try next one
		}

		// Wait a bit before trying again
		select {
		case <-bc.tryRetransmit:
		case <-time.After(time.Millisecond * 10):
		}
	}

	if !alreadyTransmittedOnAllSubflows {
		log.Debugf("frame %d failed to retransmit on all subflows of %x", frame.fn, bc.cid)
		// If we can't retransmit, release the frame to prevent memory leak
		frame.release()
	}
}

func selectSubflowForRetransmit(subflows []*subflow, frame *sendFrame, timeFallback bool) (bool, bool, *subflow) {
	var selectedSubflow *subflow
	for _, sf := range subflows {
		if atomic.LoadUint64(&sf.actuallyBusyOnWrite) == 1 {
			// Avoid a possibly blocked writer for a retransmit
			// let's avoid re-sending a frame down the same socket twice.
			// Since at best, it just double sends a frame into the send buffer
			// and at worst it blocks other frames from entering a send buffer.
			continue
		}
		// Have we used this subflow before for this frame?
		usedBefore := false
		var avoidTime time.Time
		for _, avoidSF := range frame.sentVia {
			if sf == avoidSF.sf {
				usedBefore = true
				avoidTime = avoidSF.txTime
			}
		}

		// It may be acceptable to use a subflow that has been used before
		// if we are in timeFallback mode
		if usedBefore {
			if timeFallback {
				if time.Since(avoidTime) > time.Second {
					usedBefore = false
				}
			} else {
				continue
			}
		}

		if !usedBefore {
			// frame.sentVia
			return false, false, sf
		}
	}
	return true, true, selectedSubflow
}

func (bc *mpConn) sortedSubflows() []*subflow {
	bc.muSubflows.RLock()
	subflows := make([]*subflow, len(bc.subflows))
	copy(subflows, bc.subflows)
	bc.muSubflows.RUnlock()
	sort.Slice(subflows, func(i, j int) bool {
		return subflows[i].getRTT() < subflows[j].getRTT()
	})
	return subflows
}

func (bc *mpConn) add(to string, c net.Conn, clientSide bool, probeStart time.Time, tracker StatsTracker) {
	bc.muSubflows.Lock()
	defer bc.muSubflows.Unlock()
	bc.subflows = append(bc.subflows, startSubflow(to, c, bc, clientSide, probeStart, tracker))
}

func (bc *mpConn) remove(theSubflow *subflow) {
	bc.muSubflows.Lock()
	var remains []*subflow
	for _, sf := range bc.subflows {
		if sf != theSubflow {
			remains = append(remains, sf)
		}
	}
	bc.subflows = remains
	left := len(remains)
	bc.muSubflows.Unlock()
	if left == 0 {
		bc.close()
	}
}

func (bc *mpConn) retransmitLoop() {
	evalTick := time.NewTicker(time.Millisecond * 100)
	defer evalTick.Stop()

	for {
		<-evalTick.C
		if atomic.LoadUint32(&bc.closed) == 1 {
			return
		}

		// Collect frames that need retransmission
		bc.pendingAckMu.RLock()
		retransmitFrames := make([]pendingAck, 0, len(bc.pendingAckMap))
		now := time.Now()
		for _, frame := range bc.pendingAckMap {
			if frame != nil && now.Sub(frame.sentAt) > frame.outboundSf.retransTimer() {
				retransmitFrames = append(retransmitFrames, *frame)
			}
		}
		bc.pendingAckMu.RUnlock()

		// Sort by frame number for ordered retransmission
		sort.Slice(retransmitFrames, func(i, j int) bool {
			return retransmitFrames[i].fn < retransmitFrames[j].fn
		})

		// Process retransmissions
		for _, frame := range retransmitFrames {
			sendframe := frame.framePtr
			if sendframe == nil {
				continue
			}

			sendframe.changeLock.Lock()
			// Double-check if frame is still pending
			if bc.isPendingAck(frame.fn) {
				// Only retransmit if not already being retransmitted
				if atomic.LoadUint64(&sendframe.beingRetransmitted) == 0 {
					go bc.retransmit(sendframe)
				}
			} else {
				// Frame was acked, clean up
				sendframe.release()
				bc.pendingAckMu.Lock()
				delete(bc.pendingAckMap, frame.fn)
				bc.pendingAckMu.Unlock()
			}
			sendframe.changeLock.Unlock()
		}
	}
}

func (bc *mpConn) isPendingAck(fn uint64) bool {
	if fn > minFrameNumber {
		bc.pendingAckMu.RLock()
		defer bc.pendingAckMu.RUnlock()
		return bc.pendingAckMap[fn] != nil
	}
	return false

}
