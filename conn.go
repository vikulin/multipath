package multipath

import (
	"context"
	"fmt"
	"io"
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
	// Interface monitoring will be started after first subflow is added (for server connections)
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

		bc.muSubflows.RLock()
		subflows := make([]*subflow, len(bc.subflows))
		copy(subflows, bc.subflows)
		bc.muSubflows.RUnlock()

		for _, sf := range subflows {
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
		if len(subflows) == 0 {
			return 0, ErrClosed
		}

		<-bc.writerMaybeReady
	}
}

func (bc *mpConn) Close() error {
	bc.close()

	// Close all subflows
	bc.muSubflows.RLock()
	subflows := make([]*subflow, len(bc.subflows))
	copy(subflows, bc.subflows)
	bc.muSubflows.RUnlock()

	for _, sf := range subflows {
		sf.close()
	}
	return nil
}

func (bc *mpConn) close() {
	atomic.StoreUint32(&bc.closed, 1)
	bc.recvQueue.close()

	// Stop interface monitoring
	if bc.interfaceMonitor != nil {
		bc.interfaceMonitor.Stop()
	}

	// Stop recovery monitoring
	if bc.recoveryTicker != nil {
		bc.recoveryTicker.Stop()
	}
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

	bc.muSubflows.RLock()
	subflows := make([]*subflow, len(bc.subflows))
	copy(subflows, bc.subflows)
	bc.muSubflows.RUnlock()

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

func (bc *mpConn) add(to string, c net.Conn, clientSide bool, probeStart time.Time, tracker StatsTracker) {
	// Try to acquire the lock with a timeout
	lockAcquired := make(chan bool, 1)
	go func() {
		bc.muSubflows.Lock()
		lockAcquired <- true
	}()

	select {
	case <-lockAcquired:
		defer bc.muSubflows.Unlock()
		sf := startSubflow(to, c, bc, clientSide, probeStart, tracker)
		bc.subflows = append(bc.subflows, sf)

		// Start interface monitoring after first subflow is added (for server connections)
		if len(bc.subflows) == 1 && bc.interfaceEnabled && bc.interfaceMonitor != nil {
			go bc.startInterfaceMonitoring()
		}
	case <-time.After(5 * time.Second):
		return
	}
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

// performReconnectionHandshake performs the multipath handshake for a reconnecting subflow
func (bc *mpConn) performReconnectionHandshake(conn net.Conn, cid connectionID) (connectionID, error) {
	var leadBytes [leadBytesLength]byte
	// the first byte, version, is implicitly set to 0
	copy(leadBytes[1:], cid[:])
	_, err := conn.Write(leadBytes[:])
	if err != nil {
		return zeroCID, err
	}
	_, err = io.ReadFull(conn, leadBytes[:])
	if err != nil {
		return zeroCID, err
	}
	if uint8(leadBytes[0]) != 0 {
		return zeroCID, ErrUnexpectedVersion
	}
	var newCID connectionID
	copy(newCID[:], leadBytes[1:])
	if cid != zeroCID && cid != newCID {
		return zeroCID, ErrUnexpectedCID
	}
	return newCID, nil
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
		// Check if subflow is already closed
		if sf.isClosed() {
			log.Debugf("Subflow %s is already closed, skipping health check", sf.to)
			continue
		}

		// Check for stale subflows (no activity for too long)
		timeSinceActivity := time.Since(sf.lastActivity)

		// Different thresholds for dynamic vs static subflows
		staleThreshold := 30 * time.Second      // Increased from 12s to 30s for large transfers
		lowSuccessThreshold := 60 * time.Second // Increased from 30s to 60s
		criticalThreshold := 120 * time.Second  // Increased from 60s to 120s

		if sf.isDynamicSubflow() {
			// Dynamic subflows get more aggressive health checks, but still reasonable
			staleThreshold = 20 * time.Second      // Increased from 8s to 20s
			lowSuccessThreshold = 40 * time.Second // Increased from 20s to 40s
			criticalThreshold = 60 * time.Second   // Increased from 30s to 60s
		}

		// Check for critical failure (no activity for very long time)
		if timeSinceActivity > criticalThreshold {
			bc.markSubflowAsFailed(sf, "critical_failure")
			continue
		}

		// Check for stale subflows - but be more lenient during active transfers
		if timeSinceActivity > staleThreshold {
			// Check if there are pending frames for this subflow - if so, it's still active
			hasPendingFrames := false
			bc.pendingAckMu.RLock()
			for _, frame := range bc.pendingAckMap {
				if frame.outboundSf == sf {
					hasPendingFrames = true
					break
				}
			}
			bc.pendingAckMu.RUnlock()

			if !hasPendingFrames {
				bc.markSubflowAsFailed(sf, "stale")
				continue
			}
		}

		// Check for subflows with very low success rates
		successRate := sf.getSuccessRate()
		if successRate < 0.1 && timeSinceActivity > lowSuccessThreshold {
			bc.markSubflowAsFailed(sf, "low_success_rate")
			continue
		}

		// Check for subflows with high failure rates
		failureRate := sf.getFailureRate()
		if failureRate > 0.5 && timeSinceActivity > lowSuccessThreshold {
			bc.markSubflowAsFailed(sf, "high_failure_rate")
			continue
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

	// Check if already marked as failed
	if _, exists := bc.failedSubflows[sf.to]; exists {
		log.Debugf("Subflow %s already marked as failed, skipping duplicate marking", sf.to)
		return
	}

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

	// Perform handshake to join existing connection
	existingCID := bc.getConnectionID()
	if existingCID == zeroCID {
		log.Debugf("No existing connection ID found, cannot recover subflow")
		conn.Close()
		return
	}

	// Perform handshake with existing connection ID
	newCID, err := bc.performReconnectionHandshake(conn, existingCID)
	if err != nil {
		log.Debugf("Failed to handshake recovered subflow %s: %v", failedInfo.address, err)
		conn.Close()
		bc.handleReconnectionFailure(failedInfo, err)
		return
	}

	// Verify the connection ID matches
	if newCID != existingCID {
		log.Debugf("Recovered subflow handshake returned different connection ID: %v != %v", newCID, existingCID)
		conn.Close()
		bc.handleReconnectionFailure(failedInfo, fmt.Errorf("connection ID mismatch"))
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

	log.Debugf("Successfully recovered subflow %s (attempt %d)", failedInfo.address, failedInfo.recoveryAttempts+1)
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
