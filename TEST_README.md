# Multipath Protocol Test Suite

This directory contains comprehensive tests for the multipath protocol implementation, covering single connections, multipath scenarios, congestion control, and receive queue functionality.

## Test Files

### 1. `single_connection_test.go`
Tests basic single connection functionality with random data transmission.

**Test Cases:**
- `TestSingleConnectionRandomData`: Tests data transmission with various sizes (1B to 100KB)
- `TestSingleConnectionLargeData`: Tests with large data (1MB) to verify memory management
- `TestSingleConnectionConcurrentWrites`: Tests concurrent writes on single connection
- `TestSingleConnectionStress`: Stress tests with rapid writes and reads
- `TestSingleConnectionErrorHandling`: Tests error conditions and invalid addresses
- `TestSingleConnectionTimeout`: Tests timeout handling
- `TestSingleConnectionFrameOrdering`: Tests frame ordering and sequencing
- `TestSingleConnectionClose`: Tests connection closing behavior

### 2. `multipath_connection_test.go`
Tests multiple path scenarios and load balancing.

**Test Cases:**
- `TestMultiPathConnection`: Tests multiple path data transmission
- `TestMultiPathPathFailure`: Tests path failure and recovery
- `TestMultiPathLoadBalancing`: Tests load balancing across multiple paths
- `TestMultiPathConcurrentConnections`: Tests multiple concurrent connections
- `TestMultiPathRetransmission`: Tests retransmission across multiple paths

### 3. `congestion_control_test.go`
Tests the new congestion control and flow control features.

**Test Cases:**
- `TestCongestionControl`: Tests basic congestion control functionality
- `TestCongestionControlPacketLoss`: Tests packet loss handling
- `TestCongestionControlRTTUpdate`: Tests RTT-based congestion detection
- `TestCongestionControlAckHandling`: Tests ACK handling
- `TestFlowControl`: Tests flow control implementation
- `TestFlowControlEdgeCases`: Tests edge cases in flow control
- `TestEnhancedStatsTracker`: Tests enhanced statistics tracking
- `TestEnhancedStatsTrackerPathStats`: Tests path statistics tracking
- `TestEnhancedStatsTrackerFormatting`: Tests statistics formatting
- `TestCongestionControlStateTransitions`: Tests state transitions
- `TestCongestionControlWindowBounds`: Tests window size bounds
- `TestFlowControlConcurrentAccess`: Tests concurrent access to flow control
- `TestEnhancedStatsTrackerConcurrentAccess`: Tests concurrent access to stats tracker

### 4. `receive_queue_test.go` (Enhanced)
Tests the receive queue improvements and frame ordering. This file was enhanced with additional test cases.

**Original Test Cases:**
- `TestRead`: Tests basic read operations, partial reads, out-of-order frames, duplicate frames, and timing
- `TestReadRXQEarlyClose`: Tests early close behavior and draining remaining data

**Added Test Cases:**
- `TestReceiveQueueConcurrentAccess`: Tests concurrent access to receive queue
- `TestReceiveQueueDeadline`: Tests read deadline functionality
- `TestReceiveQueueDeadlineWithData`: Tests deadline with data available
- `TestReceiveQueueFullBuffer`: Tests full buffer handling
- `TestReceiveQueueBufferCorruption`: Tests buffer corruption detection
- `TestReceiveQueueEmptyRead`: Tests reading from empty queue

### Basic Go Test Commands
```bash
# Run all tests
go test -v ./...

# Run specific test file
go test -v single_connection_test.go
go test -v multipath_connection_test.go
go test -v congestion_control_test.go
go test -v receive_queue_test.go

# Run specific test function
go test -v -run TestSingleConnectionRandomData
go test -v -run TestMultiPathConnection
go test -v -run TestCongestionControl

# Run with coverage
go test -v -cover ./...
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Run with race detection
go test -v -race ./...

# Run with benchmarks
go test -v -bench=. ./...
```
