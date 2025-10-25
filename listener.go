package multipath

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

type mpListener struct {
	listeners      []net.Listener
	listenerStats  []StatsTracker
	mpConns        map[connectionID]*mpConn
	muMPConns      sync.Mutex
	chNextAccepted chan net.Conn
	startOnce      sync.Once
	chClose        chan struct{}
	closeOnce      sync.Once
}

func NewListener(listeners []net.Listener, stats []StatsTracker) net.Listener {
	if len(listeners) != len(stats) {
		panic("the number of stats trackers should match listeners")
	}
	mpl := &mpListener{
		listeners:      listeners,
		listenerStats:  stats,
		mpConns:        make(map[connectionID]*mpConn),
		chNextAccepted: make(chan net.Conn),
		chClose:        make(chan struct{}),
	}
	return mpl
}

func (mpl *mpListener) Accept() (net.Conn, error) {
	mpl.startOnce.Do(mpl.start)
	select {
	case <-mpl.chClose:
		return nil, ErrClosed
	case conn := <-mpl.chNextAccepted:
		return conn, nil
	}
}

func (mpl *mpListener) Close() error {
	mpl.closeOnce.Do(func() { close(mpl.chClose) })
	return nil
}

// Addr satisfies the net.Listener interface. It returns a fake addr.
func (mpl *mpListener) Addr() net.Addr {
	return fakeAddr{}
}

func (mpl *mpListener) start() {
	for i, l := range mpl.listeners {
		go func(l net.Listener, st StatsTracker) {
			for {
				if err := mpl.acceptFrom(l, st); err != nil {
					select {
					case <-mpl.chClose:
						return
					default:
						log.Debugf("failed to accept on %s: %v", l.Addr(), err)
					}
				}
			}
		}(l, mpl.listenerStats[i])
	}
}

func (mpl *mpListener) acceptFrom(l net.Listener, st StatsTracker) error {
	conn, err := l.Accept()
	if err != nil {
		return err
	}

	var leadBytes [leadBytesLength]byte
	_, err = io.ReadFull(conn, leadBytes[:])
	if err != nil {
		return err
	}

	if uint8(leadBytes[0]) != 0 {
		return ErrUnexpectedVersion
	}
	var cid connectionID
	copy(cid[:], leadBytes[1:])
	newConn := false
	if cid == zeroCID {
		newConn = true
		cid = connectionID(uuid.New())
		copy(leadBytes[1:], cid[:])
	}
	probeStart := time.Now()
	// echo lead bytes back to the client
	if _, err := conn.Write(leadBytes[:]); err != nil {
		return err
	}
	mpl.muMPConns.Lock()
	bc, exists := mpl.mpConns[cid]
	if !exists {
		if newConn {
			bc = newMPConn(cid, conn.RemoteAddr())
			// Enable interface monitoring for server connections to support dynamic subflows
			bc.interfaceEnabled = true
			// Don't start interface monitoring yet - wait until first subflow is added
			mpl.mpConns[cid] = bc
		} else {
			mpl.muMPConns.Unlock()
			return fmt.Errorf("unexpected subflow of CID %v from %v", cid, conn.RemoteAddr())
		}
	}
	mpl.muMPConns.Unlock()
	subflowName := fmt.Sprintf("%x(%s)", cid, conn.LocalAddr().String())

	// Add timeout to detect if bc.add() is hanging
	done := make(chan bool, 1)
	go func() {
		bc.add(subflowName, conn, false, probeStart, st)
		done <- true
	}()

	select {
	case <-done:
		// Subflow added successfully
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout adding subflow %s", subflowName)
	}
	if newConn {
		select {
		case mpl.chNextAccepted <- bc:
			// Connection sent to accept channel
		case <-time.After(5 * time.Second):
			// Timeout sending connection to accept channel
		}
	}
	return nil
}

func (mpl *mpListener) remove(cid connectionID) {
	mpl.muMPConns.Lock()
	delete(mpl.mpConns, cid)
	mpl.muMPConns.Unlock()
}
