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
	// Create multiple TCP listeners on different ports (listen on specific IP)
	listeners := []net.Listener{
		createListener("192.168.11.11:8080"),
		createListener("192.168.11.11:8081"),
		createListener("192.168.11.11:8082"),
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

	fmt.Println("Multipath performance server listening on ports 8080, 8081, 8082")
	fmt.Println("This server consumes data without echoing (like /dev/null)")
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
		go handlePerformanceConnection(conn)
	}
}

func createListener(addr string) net.Listener {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", addr, err)
	}
	return listener
}

func handlePerformanceConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Printf("New performance connection from %s\n", conn.RemoteAddr())

	// Consume data from client (like /dev/null) - optimized for large transfers
	buffer := make([]byte, 64*1024) // 64KB buffer to match client
	totalBytes := int64(0)
	startTime := time.Now()
	lastReport := startTime

	for {
		// Set read deadline to prevent hanging - very long timeout for large transfers
		conn.SetReadDeadline(time.Now().Add(30 * time.Minute)) // 30 minutes
		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("Client finished sending data (total: %d bytes)\n", totalBytes)
				break
			}
			// Check if it's a normal connection close
			errMsg := err.Error()
			if errMsg != "use of closed network connection" &&
				errMsg != "closed connection" &&
				errMsg != "connection reset by peer" {
				log.Printf("Read error: %v", err)
			}
			break
		}

		// Just consume the data - don't store it anywhere (like /dev/null)
		totalBytes += int64(n)

		// Progress indicator every 100MB or every 5 seconds
		now := time.Now()
		if totalBytes%(100*1024*1024) == 0 || now.Sub(lastReport) >= 5*time.Second {
			elapsed := now.Sub(startTime)
			if elapsed > 0 {
				throughput := float64(totalBytes) / elapsed.Seconds()
				fmt.Printf("Consumed %d MB (%.2f MB/sec) - %s\n",
					totalBytes/(1024*1024),
					throughput/(1024*1024),
					conn.RemoteAddr())
			}
			lastReport = now
		}

		// Reset read deadline after successful operation - keep connection alive
		conn.SetReadDeadline(time.Now().Add(30 * time.Minute))
	}

	// Final statistics
	elapsed := time.Since(startTime)
	if elapsed > 0 {
		throughput := float64(totalBytes) / elapsed.Seconds()
		fmt.Printf("Connection from %s closed - Total: %d bytes in %v (%.2f MB/sec)\n",
			conn.RemoteAddr(), totalBytes, elapsed, throughput/(1024*1024))
	} else {
		fmt.Printf("Connection from %s closed - Total: %d bytes\n",
			conn.RemoteAddr(), totalBytes)
	}
}
