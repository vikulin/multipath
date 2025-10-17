package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/vikulin/multipath"
)

func main() {
	// Create individual dialers for different paths
	dialers := []multipath.Dialer{
		&tcpDialer{addr: "localhost:8080"},
		&tcpDialer{addr: "localhost:8081"},
		&tcpDialer{addr: "localhost:8082"},
	}

	// Create multipath dialer
	mpDialer := multipath.NewDialer("multipath-server", dialers)

	// Connect with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := mpDialer.DialContext(ctx)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	fmt.Printf("Connected to multipath server via %s\n", conn.RemoteAddr())

	// Send multiple messages
	messages := []string{
		"Hello, Multipath!",
		"This is a test message",
		"Multipath provides better performance",
		"Multiple paths for reliability",
		"Goodbye!",
	}

	for i, msg := range messages {
		fmt.Printf("Sending message %d: %s\n", i+1, msg)

		// Send data
		_, err = conn.Write([]byte(msg))
		if err != nil {
			log.Printf("Write error: %v", err)
			break
		}

		// Read response
		response := make([]byte, len(msg))
		_, err = conn.Read(response)
		if err != nil {
			log.Printf("Read error: %v", err)
			break
		}

		fmt.Printf("Received: %s\n", string(response))

		// Small delay between messages
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("All messages sent and received successfully!")
}

// Custom dialer implementation
type tcpDialer struct {
	addr string
}

func (d *tcpDialer) DialContext(ctx context.Context) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", d.addr)
}

func (d *tcpDialer) Label() string {
	return fmt.Sprintf("TCP dialer to %s", d.addr)
}
