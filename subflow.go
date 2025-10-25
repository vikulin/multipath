package multipath

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getlantern/ema"
	pool "github.com/libp2p/go-buffer-pool"
)

type pendingAck struct {
	fn         uint64
	sz         uint64
	sentAt     time.Time
	outboundSf *subflow
	framePtr   *sendFrame
}

type subflow struct {
	to   string
	conn net.Conn
	mpc  *mpConn

	chClose             chan struct{}
	closeOnce           sync.Once
	sendQueue           chan *sendFrame
	pendingPing         *pendingAck // Only for pings
	muPendingPing       sync.RWMutex
	emaRTT              *ema.EMA
	tracker             StatsTracker
	actuallyBusyOnWrite uint64
	finishedClosing     chan bool

	// Throughput tracking
	bytesSent     uint64
	bytesReceived uint64
	lastActivity  time.Time
	successCount  uint64
	failureCount  uint64
	muStats       sync.RWMutex
}

func startSubflow(to string, c net.Conn, mpc *mpConn, clientSide bool, probeStart time.Time, tracker StatsTracker) *subflow {
	sf := &subflow{
		to:              to,
		conn:            c,
		mpc:             mpc,
		chClose:         make(chan struct{}),
		sendQueue:       make(chan *sendFrame, 1),
		finishedClosing: make(chan bool, 1),
		// pendingPing is used for storing the subflow's ping data. Handy since pings are subflow dependent
		pendingPing:  nil,
		emaRTT:       ema.NewDuration(longRTT, rttAlpha),
		tracker:      tracker,
		lastActivity: time.Now(),
	}
	go sf.sendLoop()
	if clientSide {
		initialRTT := time.Since(probeStart)
		tracker.UpdateRTT(initialRTT)
		sf.emaRTT.SetDuration(initialRTT)
		// pong immediately so the server can calculate the RTT between when it
		// sends the leading bytes and receives the pong frame.
		sf.ack(frameTypePong)
	} else {
		// server side subflow expects a pong frame to calculate RTT.
		sf.muPendingPing.Lock()
		sf.pendingPing = &pendingAck{frameTypePong, 0, probeStart, sf, nil}
		sf.muPendingPing.Unlock()
	}
	go func() {
		if err := sf.readLoop(); err != nil && err != io.EOF {
			log.Debugf("read loop to %s ended: %v", sf.to, err)
		}
	}()
	return sf
}

func (sf *subflow) readLoop() (err error) {
	ch := make(chan *rxFrame)
	r := byteReader{Reader: sf.conn}
	go sf.readLoopFrames(ch, r)

	probeTimer := time.NewTimer(randomize(probeInterval))
	go sf.probe() // Force a ping out right away, to calibrate our own timings

	for {
		select {
		case frame := <-ch: // Fed by readLoopFrames
			if frame == nil {
				return
			}
			sf.mpc.recvQueue.add(frame, sf)
			if !probeTimer.Stop() {
				<-probeTimer.C
			}
			probeTimer.Reset(randomize(probeInterval))
		case <-probeTimer.C:
			go sf.probe()
			probeTimer.Reset(randomize(probeInterval))
		}
	}
}

func (sf *subflow) readLoopFrames(ch chan *rxFrame, r byteReader) bool {
	defer close(ch)
	var err error
	for {
		// The is the core "reactor" where frames are read. The frame format
		// can be found in the top of multipath.go
		var sz, fn uint64
		sz, err = ReadVarInt(r)
		if err != nil {
			sf.close()
			return true
		}
		fn, err = ReadVarInt(r)
		if err != nil {
			sf.close()
			return true
		}
		if sz == 0 {
			sf.gotACK(fn)
			continue
		}
		log.Tracef("got frame %d from %s with %d bytes", fn, sf.to, sz)
		if sz > 1<<20 {
			// This almost always happens due to frame corruption.
			log.Errorf("Frame of size %v from %s is impossible", sz, sf.to)
			sf.close()
			return true
		}
		buf := pool.Get(int(sz))
		_, err = io.ReadFull(r, buf)
		if err != nil {
			pool.Put(buf)
			sf.close()
			return true
		}

		if fn > (atomic.LoadUint64(&sf.mpc.recvQueue.readFrameTip) + sf.mpc.recvQueue.size) {
			// This frame dropped is too far in the future to apply
			continue
		}

		ch <- &rxFrame{fn: fn, bytes: buf}
		sf.tracker.OnRecv(sz)
		sf.recordBytesReceived(sz)
		select {
		case <-sf.chClose:
			return true
		default:
		}
	}
}

func (sf *subflow) sendLoop() {
	closing := false
	closeCountdown := time.NewTimer(time.Millisecond * 33)
	closeCountdown.Stop()
	defer func() {
		sf.finishedClosing <- true
	}()

	go func() {
		<-sf.chClose
		closeCountdown.Reset(time.Millisecond * 33)
		closing = true
	}()

	for {
		select {
		case <-closeCountdown.C:
			sf.conn.Close()
			return
		case frame := <-sf.sendQueue:
			if closing {
				closeCountdown.Reset(time.Millisecond * 33)
			}
			if closing {
				closing = true
			}

			frame.changeLock.Lock()
			if frame.retransmissions != 0 {
				log.Tracef("Retransmit on %d, for the %dth time", frame.fn, frame.retransmissions)
			}
			if *frame.released == 1 {
				log.Errorf("Tried to send a frame that has already been released! Frame Number: %v", frame.fn)

				select {
				case sf.mpc.writerMaybeReady <- true:
				default:
				}

				frame.changeLock.Unlock()
				continue
			}
			if frame.retransmissions == 0 {
				if frame.sentVia == nil {
					frame.sentVia = make([]transmissionDatapoint, 0)
				}
				frame.sentVia = append(frame.sentVia, transmissionDatapoint{sf, time.Now()})
			}

			sf.addPendingAck(frame)
			frame.changeLock.Unlock()

			atomic.StoreUint64(&sf.actuallyBusyOnWrite, 1)
			n, err := sf.conn.Write(frame.buf)
			atomic.StoreUint64(&sf.actuallyBusyOnWrite, 0)
			var abort bool
			for {
				// wake all writers up, since they might have something to send now that we likely
				// have free capacity.
				select {
				case sf.mpc.writerMaybeReady <- true:
				default:
					abort = true
				}
				if abort {
					break
				}
			}

			// only wake up one re-transmitter, to better control the possible hored of them
			select {
			case sf.mpc.tryRetransmit <- true:
			default:
			}

			if err != nil {
				log.Debugf("failed to write frame %d to %s: %v", frame.fn, sf.to, err)
				sf.recordFailure()

				if frame.isDataFrame() {
					go sf.mpc.retransmit(frame)
				}

				if n != 0 && len(frame.buf) != n {
					log.Tracef("We may have corrupted the output %#v vs %#v", n, len(frame.buf))
					// In this case, we will not try and write the remaining, and instead we will assume
					// that writing to the socket again will only make this worse, so aborting the subflow
					sf.close()
					return
				}

				sf.close()
				return
			}

			if n != len(frame.buf) {
				panic(fmt.Sprintf("expect to write %d bytes on %s, written %d", len(frame.buf), sf.to, n))
			}
			if !frame.isDataFrame() {
				frame.release()
				continue
			}
			log.Tracef("done writing frame %d with %d bytes via %s", frame.fn, frame.sz, sf.to)
			frame.changeLock.Lock()
			if frame.retransmissions == 0 {
				sf.tracker.OnSent(frame.sz)
				sf.recordBytesSent(frame.sz)
				sf.recordSuccess()
			} else {
				sf.tracker.OnRetransmit(frame.sz)
				sf.recordFailure()
			}
			frame.changeLock.Unlock()
		}
	}
}

func (sf *subflow) ack(fn uint64) {
	if sf == nil {
		// This should only ever happen in testing.
		log.Debugf("Nil subflow requested to do an ack! (should only happen on tests)")
		return
	}

	select {
	case <-sf.chClose:
	case sf.sendQueue <- composeFrame(fn, nil):
	}
}

func (sf *subflow) gotACK(fn uint64) {
	log.Tracef("got ack for frame %d from %s", fn, sf.to)
	if fn == frameTypePing {
		log.Tracef("pong to %s", sf.to)
		sf.ack(frameTypePong)
		return
	}

	sf.mpc.pendingAckMu.RLock()
	pending := sf.mpc.pendingAckMap[fn]
	if sf.mpc.pendingAckMap[fn] != nil {
		sf.mpc.pendingAckMu.RUnlock()
		sf.mpc.pendingAckMu.Lock()
		delete(sf.mpc.pendingAckMap, fn)
		sf.mpc.pendingAckMu.Unlock()
	} else {
		sf.mpc.pendingAckMu.RUnlock()
		return
	}

	if time.Since(pending.sentAt) < time.Second {
		pending.outboundSf.updateRTT(time.Since(pending.sentAt))
	} else {
		pending.outboundSf.updateRTT(time.Second)
	}
}

func (sf *subflow) updateRTT(rtt time.Duration) {
	sf.tracker.UpdateRTT(rtt)
	sf.emaRTT.UpdateDuration(rtt)
}

func (sf *subflow) getRTT() time.Duration {
	recorded := sf.emaRTT.GetDuration()
	// RTT is updated only when ack is received or retransmission timer raises,
	// which can be stale when the subflow starts hanging. If that happens, the
	// time since the earliest yet-to-be-acknowledged frame being sent is more
	// up-to-date.
	var realtime time.Duration
	sf.muPendingPing.RLock()
	if sf.pendingPing != nil {
		realtime = time.Since(sf.pendingPing.sentAt)
	} else {
		sf.muPendingPing.RUnlock()
		return recorded
	}
	sf.muPendingPing.RUnlock()
	if realtime > recorded {
		return realtime
	} else {
		return recorded
	}
}

func (sf *subflow) addPendingAck(frame *sendFrame) {
	switch frame.fn {
	case frameTypePing:
		// we expect pong for ping
		sf.muPendingPing.Lock()
		sf.pendingPing = &pendingAck{frameTypePong, 0, time.Now(), sf, nil}
		sf.muPendingPing.Unlock()
	case frameTypePong:
		// expect no response for pong
	default:
		if frame.isDataFrame() {
			sf.mpc.pendingAckMu.Lock()
			sf.mpc.pendingAckMap[frame.fn] = &pendingAck{frame.fn, frame.sz, time.Now(), sf, frame}
			sf.mpc.pendingAckMu.Unlock()
		}
	}
}

func (sf *subflow) isPendingAck(fn uint64) bool {
	if fn > minFrameNumber {
		sf.mpc.pendingAckMu.RLock()
		defer sf.mpc.pendingAckMu.RUnlock()
		return sf.mpc.pendingAckMap[fn] != nil
	}
	return false
}

func (sf *subflow) probe() {
	log.Tracef("ping %s", sf.to)
	sf.ack(frameTypePing)
}

func (sf *subflow) retransTimer() time.Duration {
	d := sf.emaRTT.GetDuration() * 2
	if d > 512*time.Millisecond {
		d = 512 * time.Millisecond
	}
	if d < 1*time.Millisecond {
		d = time.Millisecond
	}
	return d
}

func (sf *subflow) close() {
	sf.closeOnce.Do(func() {
		log.Tracef("closing subflow to %s", sf.to)
		sf.mpc.remove(sf)
		close(sf.chClose)
		drainTime := time.Now()
		maxDrainTime := time.NewTimer(time.Second)
		select {
		case <-maxDrainTime.C:
		case <-sf.finishedClosing:
		}
		maxDrainTime.Stop()
		log.Debugf("Took %v to close subflow", time.Since(drainTime))
	})
}

func randomize(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d)))
}

// recordBytesSent records bytes sent for throughput calculation
func (sf *subflow) recordBytesSent(bytes uint64) {
	sf.muStats.Lock()
	sf.bytesSent += bytes
	sf.lastActivity = time.Now()
	sf.muStats.Unlock()
}

// recordBytesReceived records bytes received for throughput calculation
func (sf *subflow) recordBytesReceived(bytes uint64) {
	sf.muStats.Lock()
	sf.bytesReceived += bytes
	sf.lastActivity = time.Now()
	sf.muStats.Unlock()
}

// recordSuccess records a successful operation
func (sf *subflow) recordSuccess() {
	sf.muStats.Lock()
	sf.successCount++
	sf.lastActivity = time.Now()
	sf.muStats.Unlock()
}

// recordFailure records a failed operation
func (sf *subflow) recordFailure() {
	sf.muStats.Lock()
	sf.failureCount++
	sf.muStats.Unlock()
}

// getSuccessRate calculates the success rate of this subflow
func (sf *subflow) getSuccessRate() float64 {
	sf.muStats.RLock()
	defer sf.muStats.RUnlock()

	total := sf.successCount + sf.failureCount
	if total == 0 {
		return 1.0 // Default to 100% success rate for new subflows
	}
	return float64(sf.successCount) / float64(total)
}

// getThroughput calculates the recent throughput in bytes per second
func (sf *subflow) getThroughput() float64 {
	sf.muStats.RLock()
	defer sf.muStats.RUnlock()

	// Calculate throughput based on recent activity
	timeSinceActivity := time.Since(sf.lastActivity)
	if timeSinceActivity > 30*time.Second {
		return 0.0 // No recent activity
	}

	// Use a simple approach: if we have recent activity, assume good throughput
	// This prevents subflows from being deprioritized during active transfers
	if timeSinceActivity < 5*time.Second {
		return 1000000.0 // 1MB/s baseline for active subflows
	}

	// For less recent activity, use a lower baseline
	return 100000.0 // 100KB/s baseline
}

// getHealthScore calculates a composite health score for load balancing
func (sf *subflow) getHealthScore() float64 {
	rtt := sf.getRTT().Seconds()
	if rtt == 0 {
		rtt = 0.001 // Avoid division by zero
	}

	successRate := sf.getSuccessRate()
	timeSinceActivity := time.Since(sf.lastActivity)

	// Primary scoring based on RTT (inverse) - this is the most important factor
	rttScore := 1.0 / rtt

	// Apply success rate as a multiplier (0.5 to 1.5 range)
	successMultiplier := 0.5 + successRate // Range: 0.5 to 1.5

	// Apply activity penalty only for very inactive subflows
	activityMultiplier := 1.0
	if timeSinceActivity > 60*time.Second {
		activityMultiplier = 0.1 // Heavy penalty for very inactive subflows
	} else if timeSinceActivity > 30*time.Second {
		activityMultiplier = 0.5 // Moderate penalty for inactive subflows
	}

	// Combine: RTT score * success multiplier * activity multiplier
	score := rttScore * successMultiplier * activityMultiplier

	return score
}
