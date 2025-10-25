# Multipath Library Examples

This directory contains example programs demonstrating how to use the `github.com/vikulin/multipath` library.

## Examples

### 1. Simple Server (`simple_server.go`)
A basic multipath server that listens on multiple ports and echoes back received data.

**Features:**
- Listens on ports 8080, 8081, and 8082
- Handles multiple concurrent connections
- Graceful shutdown with Ctrl+C
- Echo server functionality

**Run:**
```bash
go run simple_server.go
```

### 2. Simple Client (`simple_client.go`)
A basic multipath client that connects to the server and sends multiple messages.

**Features:**
- Connects to multiple server ports simultaneously
- Sends multiple test messages
- Demonstrates basic multipath usage

**Run:**
```bash
go run simple_client.go
```

### 3. Performance Server (`performance_server.go`)
A high-performance server optimized for large data transfers that consumes data without echoing.

**Features:**
- Listens on ports 8080, 8081, and 8082
- Consumes data like /dev/null (no echoing)
- Optimized for large transfers (10GB+)
- Real-time throughput monitoring
- Progress reporting every 100MB or 5 seconds

**Run:**
```bash
go run performance_server.go
```

### 4. Performance Client (`performance_client.go`)
An advanced client with performance monitoring and large data transfer testing.

**Features:**
- Custom performance tracking
- Large data transfer (10GB test)
- Real-time statistics monitoring
- Throughput measurement

**Run:**
```bash
go run performance_client.go
```

## Running the Examples

### Simple Echo Test:
1. **Start the echo server:**
   ```bash
   cd examples
   go run simple_server.go
   ```

2. **In another terminal, run the client:**
   ```bash
   cd examples
   go run simple_client.go
   ```

### Performance Testing:
1. **Start the performance server:**
   ```bash
   cd examples
   go run performance_server.go
   ```

2. **In another terminal, run the performance client:**
   ```bash
   cd examples
   go run performance_client.go
   ```

## Expected Output

### Server Output:
```
Multipath server listening on ports 8080, 8081, 8082
Press Ctrl+C to stop...
New connection from 127.0.0.1:xxxxx
Echoed 18 bytes
Echoed 22 bytes
Echoed 35 bytes
Echoed 30 bytes
Echoed 9 bytes
Connection from 127.0.0.1:xxxxx closed
```

### Client Output:
```
Connected to multipath server via 127.0.0.1:8080
Sending message 1: Hello, Multipath!
Received: Hello, Multipath!
Sending message 2: This is a test message
Received: This is a test message
Sending message 3: Multipath provides better performance
Received: Multipath provides better performance
Sending message 4: Multiple paths for reliability
Received: Multiple paths for reliability
Sending message 5: Goodbye!
Received: Goodbye!
All messages sent and received successfully!
```

## Customization

You can modify these examples to:

1. **Add more paths:** Add additional dialers/listeners for more network paths
2. **Change ports:** Modify the port numbers in the dialer/listener configurations
3. **Add authentication:** Implement custom authentication logic
4. **Monitor performance:** Use custom stats trackers to monitor path performance
5. **Handle errors:** Add robust error handling and retry logic

## Dependencies

Make sure you have the multipath library installed:

```bash
go get github.com/vikulin/multipath
```

## Notes

- The examples use localhost for simplicity, but in production you would use different network interfaces or remote addresses
- The multipath library automatically handles load balancing and failover
- Performance benefits are most noticeable with multiple network paths and larger data transfers
- The library is designed to be transparent - it implements the standard `net.Conn` interface
