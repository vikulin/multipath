package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vikulin/multipath"
)

func main() {
	// Create multiple TCP listeners on different ports
	listeners := []net.Listener{
		createListener(":8080"),
		createListener(":8081"),
		createListener(":8082"),
	}

	// Create stats trackers (one per listener)
	stats := []multipath.StatsTracker{
		&multipath.NullTracker{}, // No stats tracking
		&multipath.NullTracker{},
		&multipath.NullTracker{},
	}

	// Create multipath listener
	mpListener := multipath.NewListener(listeners, stats)
	defer mpListener.Close()

	fmt.Println("Multipath server listening on ports 8080, 8081, 8082")
	fmt.Println("Press Ctrl+C to stop...")

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down server...")
		mpListener.Close()
		os.Exit(0)
	}()

	for {
		conn, err := mpListener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}

		// Handle connection in goroutine
		go handleConnection(conn)
	}
}

func createListener(addr string) net.Listener {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", addr, err)
	}
	return listener
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Printf("New connection from %s\n", conn.RemoteAddr())

	// Phase 1: Consume data from client (like /dev/null)
	fmt.Println("Phase 1: Consuming data from client...")
	buffer := make([]byte, 256*1024) // 256KB buffer
	totalBytes := 0

	for {
		// Set read deadline to prevent hanging - very long timeout for large transfers
		conn.SetReadDeadline(time.Now().Add(600 * time.Second)) // 10 minutes
		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client finished sending data")
				break
			}
			// Check if it's a normal connection close
			errMsg := err.Error()
			if errMsg != "use of closed network connection" &&
				errMsg != "closed connection" {
				log.Printf("Read error: %v", err)
			}
			break
		}

		// Just consume the data - don't store it anywhere (like /dev/null)
		// The data is read into buffer but we don't do anything with it
		totalBytes += n

		// Progress indicator for large transfers
		if totalBytes%(100*1024*1024) == 0 { // Every 100MB
			fmt.Printf("Consumed %d MB so far...\n", totalBytes/(1024*1024))
		}

		// Reset read deadline after successful operation - keep connection alive
		conn.SetReadDeadline(time.Now().Add(600 * time.Second))
	}

	fmt.Printf("Connection from %s closed (total: %d bytes)\n",
		conn.RemoteAddr(), totalBytes)
}
