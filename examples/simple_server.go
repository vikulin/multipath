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

	// Echo server - read and echo back data
	buffer := make([]byte, 1024) // 1KB buffer for messages

	for {
		// Set read deadline to prevent hanging
		conn.SetReadDeadline(time.Now().Add(30 * time.Second)) // 30 second timeout
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

		// Echo back the data
		_, writeErr := conn.Write(buffer[:n])
		if writeErr != nil {
			log.Printf("Write error: %v", writeErr)
			break
		}

		fmt.Printf("Echoed %d bytes\n", n)
	}

	fmt.Printf("Connection from %s closed\n", conn.RemoteAddr())
}
