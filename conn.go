package multipath

import (
	"context"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// failedSubflowInfo tracks information about failed subflows for recovery
type failedSubflowInfo struct {
	address          string
	dialer           Dialer
	lastFailure      time.Time
	recoveryAttempts int
	nextAttempt      time.Time
	originalLabel    string
}

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

	pendingAckMap map[uint64]*pendingAck
	pendingAckMu  *sync.RWMutex

	// Dynamic subflow management
	failedSubflows   map[string]*failedSubflowInfo // Track failed subflows by address
	recoveryTicker   *time.Ticker                  // Periodic recovery attempts
	muFailedSubflows sync.RWMutex                  // Mutex for failed subflows
	originalDialers  []Dialer                      // Store original dialers for reconnection
	recoveryEnabled  bool                          // Enable/disable recovery

	// Network interface monitoring
	interfaceMonitor *InterfaceMonitor // Monitor for network interface changes
	interfaceEnabled bool              // Enable/disable interface monitoring
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
		failedSubflows:   make(map[string]*failedSubflowInfo),
		recoveryEnabled:  true,
		interfaceEnabled: true,
	}

	// Initialize interface monitor
	mpc.interfaceMonitor = NewInterfaceMonitor(mpc)

	go mpc.retransmitLoop()
	go mpc.startHealthMonitoring()
	go mpc.startRecoveryMonitoring()
	go mpc.startInterfaceMonitoring()
	return mpc
}

func (bc *mpConn) Read(b []byte) (n int, err error) {
	return bc.recvQueue.read(b)
}

func (bc *mpConn) Write(b []byte) (n int, err error) {
	frame := composeFrame(atomic.AddUint64(&bc.lastFN, 1), b)

	for {
		bc.pendingAckMu.RLock()
		inflight := len(bc.pendingAckMap)
		bc.pendingAckMu.RUnlock()
		if inflight > 500 {
			time.Sleep(time.Millisecond * 100)
			log.Tracef("too many inflights")
			continue
		}

		for _, sf := range bc.sortedSubflows() {

			if atomic.LoadUint64(&sf.actuallyBusyOnWrite) == 1 {
				// Avoid a possibly blocked writer for a retransmit
				continue
			}

			select {
			case sf.sendQueue <- frame:
				return len(b), nil
			default:
			}
		}
		if len(bc.sortedSubflows()) == 0 {
			return 0, ErrClosed
		}

		<-bc.writerMaybeReady
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

	subflows := bc.sortedSubflows()

	alreadyTransmittedOnAllSubflows := false
	for {
		abort := false
		if bc.closed == 1 {
			return
		}

		var selectedSubflow *subflow

		abort, alreadyTransmittedOnAllSubflows, selectedSubflow = selectSubflowForRetransmit(subflows, frame, false)
		if selectedSubflow == nil {
			abort, alreadyTransmittedOnAllSubflows, selectedSubflow = selectSubflowForRetransmit(subflows, frame, true)
			if selectedSubflow == nil {
				abort = true
				alreadyTransmittedOnAllSubflows = true
				break
			}
		}

		select {
		case <-selectedSubflow.chClose:
			continue
		case selectedSubflow.sendQueue <- frame:
			frame.retransmissions++
			log.Debugf("retransmitted frame %d via %s", frame.fn, selectedSubflow.to)
			if frame.sentVia == nil {
				frame.sentVia = make([]transmissionDatapoint, 0)
			}
			frame.sentVia = append(frame.sentVia, transmissionDatapoint{selectedSubflow, time.Now()})
			return
		default:
		}

		if abort {
			break
		}
		<-bc.tryRetransmit
	}

	if !alreadyTransmittedOnAllSubflows {
		log.Debugf("frame %d is being retransmitted on all subflows of %x", frame.fn, bc.cid)
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
		// Primary sort by RTT (lower is better)
		rttI := subflows[i].getRTT()
		rttJ := subflows[j].getRTT()

		// If RTTs are very close (within 10%), consider success rate as tiebreaker
		if rttI > 0 && rttJ > 0 {
			rttDiff := float64(rttI-rttJ) / float64(rttI+rttJ) * 2
			if rttDiff < 0.1 && rttDiff > -0.1 {
				// RTTs are close, use success rate as tiebreaker
				successI := subflows[i].getSuccessRate()
				successJ := subflows[j].getSuccessRate()
				return successI > successJ
			}
		}

		// Default to RTT-based sorting
		return rttI < rttJ
	})
	return subflows
}

func (bc *mpConn) add(to string, c net.Conn, clientSide bool, probeStart time.Time, tracker StatsTracker) {
	bc.muSubflows.Lock()
	defer bc.muSubflows.Unlock()
	bc.subflows = append(bc.subflows, startSubflow(to, c, bc, clientSide, probeStart, tracker))
}

// addDynamicSubflow adds a new dynamic subflow with interface tracking
func (bc *mpConn) addDynamicSubflow(to string, c net.Conn, clientSide bool, probeStart time.Time, tracker StatsTracker, localAddress, interfaceName string) {
	bc.muSubflows.Lock()
	defer bc.muSubflows.Unlock()
	bc.subflows = append(bc.subflows, startDynamicSubflow(to, c, bc, clientSide, probeStart, tracker, localAddress, interfaceName))
}

// setOriginalDialers stores the original dialers for recovery purposes
func (bc *mpConn) setOriginalDialers(dialers []Dialer) {
	bc.muFailedSubflows.Lock()
	defer bc.muFailedSubflows.Unlock()
	bc.originalDialers = dialers
}

// getConnectionID returns the connection ID for this multipath connection
func (bc *mpConn) getConnectionID() connectionID {
	return bc.cid
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
	for {
		<-evalTick.C
		if atomic.LoadUint32(&bc.closed) == 1 {
			return
		}

		bc.pendingAckMu.RLock()
		RetransmitFrames := make([]pendingAck, 0)
		for fn, frame := range bc.pendingAckMap {
			if time.Since(frame.sentAt) > frame.outboundSf.retransTimer() {
				if bc.pendingAckMap[fn] != nil {
					RetransmitFrames = append(RetransmitFrames, *frame)
				}
			}
		}
		bc.pendingAckMu.RUnlock()

		sort.Slice(RetransmitFrames, func(i, j int) bool {
			return RetransmitFrames[i].fn < RetransmitFrames[j].fn
		})

		for _, frame := range RetransmitFrames {
			sendframe := frame.framePtr
			sendframe.changeLock.Lock()
			if bc.isPendingAck(frame.fn) {
				// No ack means the subflow fails or has a longer RTT
				// log.Errorf("Retransmitting! %#v", frame.fn)
				if sendframe.beingRetransmitted == 0 {
					go bc.retransmit(sendframe)
				}
				sendframe.changeLock.Unlock()
			} else {
				// It is ok to release buffer here as the frame will never
				// be retransmitted again.
				sendframe.release()
				sendframe.changeLock.Unlock()
				bc.pendingAckMu.Lock()
				delete(bc.pendingAckMap, frame.fn)
				bc.pendingAckMu.Unlock()
			}
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

// startHealthMonitoring starts a goroutine to monitor subflow health
func (bc *mpConn) startHealthMonitoring() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if atomic.LoadUint32(&bc.closed) == 1 {
			return
		}
		bc.checkSubflowHealth()
	}
}

// checkSubflowHealth checks the health of all subflows and takes action if needed
func (bc *mpConn) checkSubflowHealth() {
	bc.muSubflows.RLock()
	subflows := make([]*subflow, len(bc.subflows))
	copy(subflows, bc.subflows)
	bc.muSubflows.RUnlock()

	for _, sf := range subflows {
		// Check for stale subflows (no activity for too long)
		timeSinceActivity := time.Since(sf.lastActivity)

		// Different thresholds for dynamic vs static subflows
		staleThreshold := 12 * time.Second
		lowSuccessThreshold := 30 * time.Second

		if sf.isDynamicSubflow() {
			// Dynamic subflows get more aggressive health checks
			staleThreshold = 8 * time.Second
			lowSuccessThreshold = 20 * time.Second
		}

		if timeSinceActivity > staleThreshold {
			log.Debugf("Subflow %s appears stale (no activity for %v), marking for recovery", sf.to, timeSinceActivity)
			bc.markSubflowAsFailed(sf, "stale")
			continue
		}

		// Check for subflows with very low success rates
		successRate := sf.getSuccessRate()
		if successRate < 0.1 && timeSinceActivity > lowSuccessThreshold {
			log.Debugf("Subflow %s has very low success rate (%.2f), marking for recovery", sf.to, successRate)
			bc.markSubflowAsFailed(sf, "low_success_rate")
			continue
		}

		// Log health status for debugging
		if timeSinceActivity > 30*time.Second {
			subflowType := "static"
			if sf.isDynamicSubflow() {
				subflowType = "dynamic"
			}
			log.Tracef("Subflow %s (%s) health: success rate=%.2f, last activity=%v ago, local=%s, interface=%s",
				sf.to, subflowType, successRate, timeSinceActivity, sf.getLocalAddress(), sf.getInterfaceName())
		}
	}
}

// startRecoveryMonitoring starts the recovery monitoring goroutine
func (bc *mpConn) startRecoveryMonitoring() {
	if !bc.recoveryEnabled {
		return
	}

	bc.recoveryTicker = time.NewTicker(5 * time.Second)
	defer bc.recoveryTicker.Stop()

	for range bc.recoveryTicker.C {
		if atomic.LoadUint32(&bc.closed) == 1 {
			return
		}
		bc.attemptRecovery()
	}
}

// markSubflowAsFailed marks a subflow as failed and schedules it for recovery
func (bc *mpConn) markSubflowAsFailed(sf *subflow, reason string) {
	bc.muFailedSubflows.Lock()
	defer bc.muFailedSubflows.Unlock()

	// Find the corresponding dialer for this subflow
	var dialer Dialer
	for _, d := range bc.originalDialers {
		if d.Label() == sf.to {
			dialer = d
			break
		}
	}

	if dialer == nil {
		log.Debugf("No dialer found for failed subflow %s, cannot recover", sf.to)
		go sf.close()
		return
	}

	// Add to failed subflows for recovery
	bc.failedSubflows[sf.to] = &failedSubflowInfo{
		address:          sf.to,
		dialer:           dialer,
		lastFailure:      time.Now(),
		recoveryAttempts: 0,
		nextAttempt:      time.Now().Add(5 * time.Second), // First attempt in 5 seconds
		originalLabel:    sf.to,
	}

	log.Debugf("Marked subflow %s as failed (reason: %s), will attempt recovery in 5s", sf.to, reason)

	// Close the current subflow
	go sf.close()
}

// attemptRecovery attempts to recover failed subflows
func (bc *mpConn) attemptRecovery() {
	bc.muFailedSubflows.Lock()
	defer bc.muFailedSubflows.Unlock()

	now := time.Now()
	for _, failedInfo := range bc.failedSubflows {
		if now.Before(failedInfo.nextAttempt) {
			continue // Not time for this subflow yet
		}

		// Attempt reconnection
		go bc.reconnectSubflow(failedInfo)
	}
}

// reconnectSubflow attempts to reconnect a failed subflow
func (bc *mpConn) reconnectSubflow(failedInfo *failedSubflowInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	log.Debugf("Attempting to reconnect subflow %s (attempt %d)", failedInfo.address, failedInfo.recoveryAttempts+1)

	// Try to dial the failed subflow
	conn, err := failedInfo.dialer.DialContext(ctx)
	if err != nil {
		bc.handleReconnectionFailure(failedInfo, err)
		return
	}

	// Connection successful, add it back as a subflow
	bc.muFailedSubflows.Lock()
	delete(bc.failedSubflows, failedInfo.address)
	bc.muFailedSubflows.Unlock()

	// Create a new subflow
	probeStart := time.Now()
	tracker := &NullTracker{} // Use null tracker for recovered subflows
	bc.add(failedInfo.address, conn, true, probeStart, tracker)

	log.Debugf("Successfully recovered subflow %s", failedInfo.address)
}

// handleReconnectionFailure handles failed reconnection attempts
func (bc *mpConn) handleReconnectionFailure(failedInfo *failedSubflowInfo, err error) {
	bc.muFailedSubflows.Lock()
	defer bc.muFailedSubflows.Unlock()

	failedInfo.recoveryAttempts++
	failedInfo.lastFailure = time.Now()

	// Exponential backoff: 5s, 15s, 30s, 60s, then every 60s
	backoffDuration := 5 * time.Second
	if failedInfo.recoveryAttempts > 1 {
		backoffDuration = 15 * time.Second
	}
	if failedInfo.recoveryAttempts > 2 {
		backoffDuration = 30 * time.Second
	}
	if failedInfo.recoveryAttempts > 3 {
		backoffDuration = 60 * time.Second
	}

	failedInfo.nextAttempt = time.Now().Add(backoffDuration)

	log.Debugf("Reconnection failed for %s (attempt %d): %v, next attempt in %v",
		failedInfo.address, failedInfo.recoveryAttempts, err, backoffDuration)

	// Give up after 10 attempts (about 10 minutes)
	if failedInfo.recoveryAttempts >= 10 {
		log.Debugf("Giving up on subflow %s after %d failed attempts", failedInfo.address, failedInfo.recoveryAttempts)
		delete(bc.failedSubflows, failedInfo.address)
	}
}

// startInterfaceMonitoring starts the network interface monitoring
func (bc *mpConn) startInterfaceMonitoring() {
	if !bc.interfaceEnabled || bc.interfaceMonitor == nil {
		return
	}

	bc.interfaceMonitor.Start()
}
