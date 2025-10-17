# Multipath Library API Reference

This document describes the API for the `github.com/vikulin/multipath` library.

## Core Types

### `Dialer` Interface
```go
type Dialer interface {
    DialContext(ctx context.Context) (net.Conn, error)
    Label() string
}
```
Represents a network dialer that can establish connections. Each dialer corresponds to one network path.

### `StatsTracker` Interface
```go
type StatsTracker interface {
    OnRecv(uint64)           // Called when data is received
    OnSent(uint64)           // Called when data is sent
    OnRetransmit(uint64)     // Called when data is retransmitted
    UpdateRTT(time.Duration) // Called when RTT is updated
}
```
Interface for tracking multipath connection statistics.

### `Stats` Interface
```go
type Stats interface {
    FormatStats() (stats []string)
}
```
Interface for formatting connection statistics.

## Main Functions

### `NewListener(listeners []net.Listener, stats []StatsTracker) net.Listener`
Creates a new multipath listener that aggregates multiple network listeners.

**Parameters:**
- `listeners`: Slice of `net.Listener` instances (one per network path)
- `stats`: Slice of `StatsTracker` instances (one per listener)

**Returns:**
- `net.Listener`: A multipath listener that implements the standard `net.Listener` interface

**Example:**
```go
listeners := []net.Listener{
    createListener(":8080"),
    createListener(":8081"),
    createListener(":8082"),
}

stats := []multipath.StatsTracker{
    &multipath.NullTracker{},
    &multipath.NullTracker{},
    &multipath.NullTracker{},
}

mpListener := multipath.NewListener(listeners, stats)
```

### `NewDialer(dest string, dialers []Dialer) Dialer`
Creates a new multipath dialer that aggregates multiple network dialers.

**Parameters:**
- `dest`: Destination identifier (used for logging/debugging)
- `dialers`: Slice of `Dialer` instances (one per network path)

**Returns:**
- `Dialer`: A multipath dialer that implements the `Dialer` interface

**Example:**
```go
dialers := []multipath.Dialer{
    &tcpDialer{addr: "server:8080"},
    &tcpDialer{addr: "server:8081"},
    &tcpDialer{addr: "server:8082"},
}

mpDialer := multipath.NewDialer("my-server", dialers)
```

## Built-in Types

### `NullTracker`
A no-op implementation of `StatsTracker` that doesn't track any statistics.

```go
type NullTracker struct{}

func (st NullTracker) OnRecv(uint64)           {}
func (st NullTracker) OnSent(uint64)           {}
func (st NullTracker) OnRetransmit(uint64)     {}
func (st NullTracker) UpdateRTT(time.Duration) {}
```

## Connection Behavior

### Multipath Connection (`mpConn`)
The multipath connection implements the standard `net.Conn` interface with the following characteristics:

1. **Transparent Interface**: Works exactly like a regular `net.Conn`
2. **Automatic Load Balancing**: Distributes data across available paths
3. **Fault Tolerance**: Continues working when some paths fail
4. **Automatic Retransmission**: Retries failed transmissions on other paths
5. **Ordered Delivery**: Ensures data is delivered in the correct order

### Frame Fragmentation
Large data is automatically fragmented into frames for transmission:
- **Frame Size**: Configurable (default: 200KB)
- **Fragmentation**: Large payloads are split into multiple frames
- **Reassembly**: Frames are reassembled in the correct order
- **Acknowledgments**: Each frame is acknowledged individually

### Path Management
- **Path Addition**: New paths can be added dynamically
- **Path Removal**: Failed paths are automatically removed
- **Path Recovery**: Recovered paths are automatically re-added
- **Load Balancing**: Traffic is distributed based on path performance

## Error Handling

### Common Errors
- **Connection Refused**: When all paths fail to connect
- **Timeout**: When operations exceed their deadline
- **Network Unreachable**: When network paths are unavailable
- **Broken Pipe**: When connections are closed unexpectedly

### Error Recovery
The multipath library automatically handles:
- **Path Failures**: Switches to available paths
- **Retransmissions**: Retries failed frames on other paths
- **Connection Recovery**: Re-establishes failed connections
- **Load Rebalancing**: Adjusts traffic distribution

## Performance Characteristics

### Throughput
- **Aggregated Bandwidth**: Combines bandwidth from all available paths
- **Load Balancing**: Distributes traffic efficiently across paths
- **Retransmission Overhead**: Minimal overhead for retransmissions

### Latency
- **Path Selection**: Uses the fastest available path
- **RTT Monitoring**: Continuously monitors path performance
- **Adaptive Routing**: Adjusts routing based on path conditions

### Reliability
- **Fault Tolerance**: Continues working with partial path failures
- **Data Integrity**: Ensures all data is delivered correctly
- **Ordered Delivery**: Maintains data ordering across all paths

## Best Practices

### Configuration
1. **Multiple Paths**: Always configure multiple dialers/listeners
2. **Path Diversity**: Use different network interfaces when possible
3. **Timeout Settings**: Set appropriate timeouts for your use case
4. **Stats Tracking**: Use custom stats trackers for monitoring

### Error Handling
1. **Graceful Degradation**: Handle partial path failures
2. **Retry Logic**: Implement retry logic for critical operations
3. **Monitoring**: Monitor path performance and health
4. **Logging**: Log important events and errors

### Performance Optimization
1. **Frame Size**: Tune frame size for your network conditions
2. **Buffer Sizes**: Use appropriate buffer sizes for your data
3. **Concurrency**: Use multiple goroutines for concurrent operations
4. **Resource Management**: Properly close connections and free resources

## Example Usage Patterns

### Basic Server
```go
listeners := []net.Listener{
    createListener(":8080"),
    createListener(":8081"),
}

stats := []multipath.StatsTracker{
    &multipath.NullTracker{},
    &multipath.NullTracker{},
}

mpListener := multipath.NewListener(listeners, stats)
defer mpListener.Close()

for {
    conn, err := mpListener.Accept()
    if err != nil {
        continue
    }
    go handleConnection(conn)
}
```

### Basic Client
```go
dialers := []multipath.Dialer{
    &tcpDialer{addr: "server:8080"},
    &tcpDialer{addr: "server:8081"},
}

mpDialer := multipath.NewDialer("server", dialers)

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

conn, err := mpDialer.DialContext(ctx)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

// Use conn like any net.Conn
```

### Custom Stats Tracking
```go
type customTracker struct {
    bytesReceived uint64
    bytesSent     uint64
}

func (c *customTracker) OnRecv(bytes uint64) {
    atomic.AddUint64(&c.bytesReceived, bytes)
}

func (c *customTracker) OnSent(bytes uint64) {
    atomic.AddUint64(&c.bytesSent, bytes)
}

func (c *customTracker) OnRetransmit(bytes uint64) {}
func (c *customTracker) UpdateRTT(rtt time.Duration) {}
```
