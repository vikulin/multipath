# Multipath Library Usage Guide

This guide shows how to use the `github.com/vikulin/multipath` library in your Go projects.

## Installation

Add the library to your project:

```bash
go get github.com/vikulin/multipath
```

## Basic Usage

### 1. Import the Library

```go
import (
    "context"
    "fmt"
    "net"
    "time"
    
    "github.com/vikulin/multipath"
)
```

### 2. Server Side - Creating a Multipath Listener

```go
package main

import (
    "fmt"
    "net"
    "log"
    
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
    
    // Read data from client
    buffer := make([]byte, 1024)
    n, err := conn.Read(buffer)
    if err != nil {
        log.Printf("Read error: %v", err)
        return
    }
    
    // Echo data back
    _, err = conn.Write(buffer[:n])
    if err != nil {
        log.Printf("Write error: %v", err)
        return
    }
    
    fmt.Printf("Echoed %d bytes\n", n)
}
```

### 3. Client Side - Creating a Multipath Dialer

```go
package main

import (
    "context"
    "fmt"
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
    
    // Send data
    data := []byte("Hello, Multipath!")
    _, err = conn.Write(data)
    if err != nil {
        log.Fatalf("Write error: %v", err)
    }
    
    // Read response
    response := make([]byte, len(data))
    _, err = conn.Read(response)
    if err != nil {
        log.Fatalf("Read error: %v", err)
    }
    
    fmt.Printf("Received: %s\n", string(response))
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
```

## Advanced Usage

### Custom Stats Tracking

```go
type customStatsTracker struct {
    bytesReceived uint64
    bytesSent     uint64
    retransmits   uint64
    rtt           time.Duration
}

func (c *customStatsTracker) OnRecv(bytes uint64) {
    atomic.AddUint64(&c.bytesReceived, bytes)
}

func (c *customStatsTracker) OnSent(bytes uint64) {
    atomic.AddUint64(&c.bytesSent, bytes)
}

func (c *customStatsTracker) OnRetransmit(bytes uint64) {
    atomic.AddUint64(&c.retransmits, bytes)
}

func (c *customStatsTracker) UpdateRTT(rtt time.Duration) {
    c.rtt = rtt
}

func (c *customStatsTracker) GetStats() string {
    return fmt.Sprintf("Received: %d bytes, Sent: %d bytes, Retransmits: %d bytes, RTT: %v",
        atomic.LoadUint64(&c.bytesReceived),
        atomic.LoadUint64(&c.bytesSent),
        atomic.LoadUint64(&c.retransmits),
        c.rtt)
}
```

### Large Data Transfer Example

```go
func transferLargeFile(conn net.Conn, filename string) error {
    file, err := os.Open(filename)
    if err != nil {
        return err
    }
    defer file.Close()
    
    // Set timeouts
    conn.SetWriteDeadline(time.Now().Add(5 * time.Minute))
    
    // Copy file data through multipath connection
    _, err = io.Copy(conn, file)
    if err != nil {
        return fmt.Errorf("transfer failed: %v", err)
    }
    
    return nil
}
```

### Error Handling

```go
func robustConnect(mpDialer multipath.Dialer) (net.Conn, error) {
    maxRetries := 3
    baseDelay := time.Second
    
    for i := 0; i < maxRetries; i++ {
        ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
        
        conn, err := mpDialer.DialContext(ctx)
        if err == nil {
            cancel()
            return conn, nil
        }
        
        cancel()
        
        if i < maxRetries-1 {
            delay := baseDelay * time.Duration(1<<i) // Exponential backoff
            log.Printf("Connection attempt %d failed: %v, retrying in %v", i+1, err, delay)
            time.Sleep(delay)
        }
    }
    
    return nil, fmt.Errorf("failed to connect after %d attempts", maxRetries)
}
```

## Key Benefits

1. **Increased Throughput**: Aggregates bandwidth from multiple network paths
2. **Fault Tolerance**: Continues working even if some paths fail
3. **Load Balancing**: Distributes traffic across available paths
4. **Transparent**: Works as a standard `net.Conn` interface
5. **Automatic Retransmission**: Retries failed transmissions on other paths

## Best Practices

1. **Use Multiple Paths**: Always configure multiple dialers/listeners for best performance
2. **Handle Errors Gracefully**: Network paths can fail, implement proper error handling
3. **Set Timeouts**: Use context timeouts to prevent hanging connections
4. **Monitor Performance**: Use custom stats trackers to monitor path performance
5. **Test with Real Networks**: Test with actual network conditions, not just localhost

## Performance Expectations

Based on testing, the multipath library typically provides:
- **30-90% performance improvement** over single TCP connections
- **Better reliability** with path redundancy
- **Automatic failover** when paths become unavailable
- **Load balancing** across available paths

The exact performance gain depends on your network conditions and the number of paths configured.
