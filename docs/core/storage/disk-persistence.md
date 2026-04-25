# Disk Persistence System

## Purpose and Scope

This document provides a comprehensive overview of cursus's disk persistence system, which is responsible for durably storing messages to disk and retrieving them on demand. The disk persistence layer ensures data durability through batched asynchronous writes while providing efficient read access via memory-mapped I/O.

This page covers the architecture, core components, write and read paths, and the concurrency model of the disk persistence system. For detailed information about:

Segment file management and rotation mechanics, see [Segment Management](./segment-management.md)
Linux and Windows-specific I/O optimizations, see [Platform-Specific Optimizations](./platform-optimizations.md)
For information about how disk persistence integrates with topic management, see [Topic Management System](../topic/topic-management.md).

## Core Components

### DiskManager

The DiskManager serves as a registry and factory for DiskHandler instances. It maintains a thread-safe map of handlers indexed by a composite key of topic name and partition ID.

| Component | Type                  | Purpose                                    |
|-----------|----------------------|--------------------------------------------|
| handlers  | map[string]*DiskHandler | Registry of active disk handlers          |
| mu        | sync.Mutex            | Protects concurrent access to the handlers map |
| cfg       | *config.Config        | Configuration for creating new handlers   |

Key Methods:

- `GetHandler(topic string, partitionID int)`: Returns existing handler or creates a new one for the topic-partition pair
- `CloseAllHandlers()`: Gracefully shuts down all active handlers

### DiskHandler

Each DiskHandler manages disk I/O for a single topic-partition pair. It maintains separate write and read paths with distinct concurrency characteristics.

| Field          | Type            | Purpose                                         |
|----------------|----------------|-------------------------------------------------|
| BaseName       | string          | Base file path: `{LogDir}/{topic}/partition_{id}` |
| SegmentSize    | uint64          | Maximum segment size (default: 1GB)            |
| CurrentOffset  | uint64          | Current write position within segment          |
| CurrentSegment | uint64          | Current segment number                          |
| writeCh        | chan DiskMessage | Buffered channel for asynchronous writes       |
| done           | chan struct{}   | Shutdown signal channel                         |
| batchSize      | int             | Max messages per batch (from DiskFlushBatchSize) |
| linger         | time.Duration   | Max wait time before flushing partial batch    |
| mu             | sync.Mutex      | Protects metadata (offset, segment number)     |
| ioMu           | sync.Mutex      | Serializes I/O operations (writes, flushes)   |
| file           | *os.File        | Current segment file handle                     |
| writer         | *bufio.Writer   | Buffered writer for efficient I/O              |


## Write Path: Asynchronous Message Persistence

The write path is fully asynchronous to prevent blocking publishers. Messages flow through a channel to a dedicated flush goroutine that performs batching and periodic flushing.

```mermaid
sequenceDiagram
    participant P as Publisher
    participant AH as AppendMessage
    participant CH as writeCh
    participant FL as flushLoop
    participant WB as WriteBatch
    participant SL as syncLoop
    participant D as Disk

    P->>AH: AppendMessage(topic, partition, msg)
    AH->>AH: Assign offset (atomic)
    AH->>CH: send DiskMessage

    loop Batch accumulation
        CH->>FL: receive msg
        FL->>FL: append to batch
    end

    alt Batch full OR linger timeout
        FL->>WB: WriteBatch(batch)
        Note over WB: Serialize outside lock
        WB->>WB: mu.Lock + ioMu.Lock
        WB->>WB: Check segment rotation
        WB->>D: bufio.Write (length + data)
        WB->>D: bufio.Flush
        WB->>WB: mu.Unlock + ioMu.Unlock
    end

    loop Every sync interval
        SL->>D: file.Sync() (fsync)
        SL->>SL: Notify OnSync callback
    end
```

Key Write Functions:

| Function                | File              | Purpose                                       |
|-------------------------|-------------------|-----------------------------------------------|
| `AppendMessage(topic, partition, msg)` | handler.go        | Non-blocking enqueue to write channel        |
| `AppendMessageSync(topic, partition, msg)` | handler.go | Synchronous write (bypasses channel)          |
| `flushLoop()`             | flush_common.go   | Main batching loop in dedicated goroutine     |
| `syncLoop()`              | flush_common.go   | Periodic fsync for durability                 |
| `WriteBatch(batch []DiskMessage)` | flush_common.go | Write a batch of messages to disk             |
| `WriteDirect(topic, partition, msg)` | flush_common.go | Direct synchronous write                      |
| `rotateSegment(nextBaseOffset)` | flush_common.go   | Close current segment and open next           |

## Read Path: Memory-Mapped I/O

The read path is synchronous and uses memory-mapped file access for efficient sequential reads. Unlike the write path, reads do not use channels or background goroutines.

Read Path Characteristics:

| Aspect            | Implementation                                   |
|-------------------|---------------------------------------------------|
| Concurrency       | Synchronous, no background goroutines             |
| Lock Strategy     | Acquires mu only to read CurrentSegment, releases immediately |
| I/O Mechanism     | Memory-mapped file via golang.org/x/exp/mmap      |
| Offset Handling   | Skip offset messages, then read max messages      |
| Error Handling    | Returns partial results on EOF or read errors     |

## Segment-Based Storage Model

Messages are stored in segment files that rotate at a fixed size boundary (default: 1MB). Each topic-partition has its own sequence of segments numbered sequentially.

### File Naming Convention

```
{LogDir}/{TopicName}/partition_{PartitionID}_segment_{SegmentNumber}.log
```

Example:

```
./logs/orders/partition_0_segment_0.log
./logs/orders/partition_0_segment_1.log
./logs/payments/partition_2_segment_0.log
```

### Segment Lifecycle

Rotation Trigger:

When CurrentOffset + messageSize > SegmentSize (1GB by default)

Occurs within `writeBatch()` before writing a message that would exceed the limit

### Message Format

Messages are stored with a length-prefixed binary format enabling random access and streaming reads.

### On-Disk Format

```
[4-byte length (big-endian)][message payload bytes][4-byte length][message payload bytes]...
```

Encoding Details:

| Field          | Size      | Encoding                   | Purpose                     |
|----------------|-----------|-----------------------------|-----------------------------|
| Length Prefix  | 4 bytes   | binary.BigEndian.Uint32    | Size of following payload   |
| Message Payload| Variable  | UTF-8 string bytes          | Actual message content      |

Example:

Message: "hello"
Bytes: [0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f]
        |---- length = 5 ---|  h     e     l     l     o

Advantages:

- **Random Access**: Can skip messages by reading length and seeking
- **Streaming**: Can read messages sequentially without parsing entire file
- **Corruption Detection**: Invalid length values indicate corruption
- **No Delimiters**: No need to escape message content

## Concurrency Model

The DiskHandler uses a dual-mutex strategy to allow concurrent reads while serializing writes.

```mermaid
graph TD
    subgraph "Write Path"
        WB[WriteBatch] -->|1. acquire| MU[mu - metadata]
        WB -->|2. acquire| IOMU[ioMu - I/O]
        WB -->|3. write| DISK[(Segment File)]
        WD[WriteDirect] -->|acquire both| MU
        WD -->|acquire both| IOMU
    end

    subgraph "Read Path (lock-free I/O)"
        RM[ReadMessages] -->|acquire mu briefly| MU2[mu - read segment list]
        RM -->|mmap read| DISK2[(Segment File)]
    end

    subgraph "Sync Path"
        SL[syncLoop] -->|acquire| IOMU2[ioMu]
        SL -->|fsync| DISK3[(Segment File)]
    end
```

### Lock Responsibilities

mu (Metadata Lock):

Protects: CurrentOffset, CurrentSegment
Acquired by: `writeBatch()`, `ReadMessages()`, `WriteDirect()`, `rotateSegment()`
Duration: Short (read/update metadata only)
ioMu (I/O Lock):

Protects: file, writer, actual I/O operations
Acquired by: `writeBatch()`, `WriteDirect()`, `Flush()`, `rotateSegment()`
Duration: Longer (includes disk I/O)
Never held during reads (mmap access is lock-free)

## Shutdown Coordination

### Shutdown Components:

| Component     | Purpose                                         |
|---------------|--------------------------------------------------|
| closeOnce     | Ensures `Close()` runs only once                   |
| done channel  | Signals shutdown to flushLoop and AppendMessage  |
| shutdown WaitGroup | Blocks `Close()` until flushLoop exits        |
| Drain logic   | Processes remaining messages                     |

## Configuration Parameters

The disk persistence system is configured through the config.Config structure:

| Parameter       | Field                | Default  | Purpose                                      |
|-----------------|--------------------|---------|----------------------------------------------|
| Log Directory   | LogDir              | `broker-logs` | Base directory for segment files             |
| Batch Size      | DiskFlushBatchSize  | 50      | Max messages per flush batch                 |
| Linger Time     | LingerMS            | 50      | Max milliseconds before flushing partial batch |
| Write Timeout   | DiskWriteTimeoutMS  | 10      | Timeout for enqueuing to write channel (ms) |
| Channel Buffer  | ChannelBufferSize   | 1024    | Size of DiskHandler write channel            |
| Segment Size    | SegmentSize         | 1GB     | Max segment file size (configurable via `log_segment_bytes`) |
| Index Size      | IndexSize           | 10MB    | Max index file size (configurable via `log_index_size_bytes`) |
| Flush Interval  | DiskFlushIntervalMS | 500     | Periodic fsync interval (ms)                 |

# Summary

The disk persistence system provides durable message storage through:

- **Registry Pattern**: DiskManager maintains handlers for each topic-partition
- **Asynchronous Writes**: Channel-based write path with batching (50 msgs or 50ms default)
- **Synchronous Reads**: Memory-mapped I/O for efficient sequential access
- **Segment-Based Storage**: Configurable segments (1GB default) with index files for fast offset lookup
- **Dual-Mutex Concurrency**: Separate locks for metadata and I/O operations
- **Length-Prefixed Format**: Enables random access and streaming without parsing
- **Graceful Shutdown**: Ensures all buffered messages are flushed before exit

For implementation details, see:

- Write path mechanics: [DiskHandler and Write Path](./disk-persistence.md)
- Segment file operations: [Segment Management](./segment-management.md)
- OS-specific optimizations: [Platform-Specific Optimizations](./platform-optimizations.md)

---

# DiskHandler and Write Path

## Purpose and Scope

This document provides a detailed explanation of the DiskHandler struct and its asynchronous write path, which forms the foundation of cursus's disk persistence layer. It covers the batching mechanism, the flushLoop goroutine, and the concurrency control strategies used to achieve high-throughput, durable message storage.

For information about segment rotation and the DiskManager registry, see [Segment Management](./segment-management.md). For Linux-specific optimizations like sendfile and fadvise, see [Platform-Specific Optimizations](./platform-optimizations.md). For the complete disk persistence architecture, see [Disk Persistence System](./disk-persistence.md).

## DiskHandler Structure

The DiskHandler is the core component responsible for persisting messages to disk for a single topic-partition pair. Each partition in the system has its own dedicated DiskHandler instance, enabling parallel I/O operations across partitions.

### Key Fields

| Field          | Type             | Purpose                                                      |
|----------------|-----------------|--------------------------------------------------------------|
| BaseName       | string           | Base path for segment files, e.g., `./logs/orders/partition_0` |
| SegmentSize    | uint64           | Maximum size per segment file (default: 1GB)                |
| CurrentOffset  | uint64           | Current write position within the active segment            |
| CurrentSegment | uint64           | Active segment number (base offset of current segment)      |
| writeCh        | chan DiskMessage  | Buffered channel for asynchronous message enqueue           |
| done           | chan struct{}    | Shutdown signal channel                                      |
| batchSize      | int              | Maximum messages per batch (default: 500)                   |
| linger         | time.Duration    | Maximum wait time before flushing partial batch (default: 100ms) |
| writeTimeout   | time.Duration    | Timeout for channel enqueue operations                      |
| mu             | sync.Mutex       | Protects metadata: CurrentOffset, CurrentSegment, file      |
| ioMu           | sync.Mutex       | Serializes I/O operations: `writer.Write()`, `writer.Flush()`   |
| file           | *os.File         | Active segment file handle                                   |
| writer         | *bufio.Writer    | Buffered writer for efficient disk writes                   |


## Configuration Parameters

The following `config.Config` fields control DiskHandler behavior:

| Config Field           | DiskHandler Field | Default      | Purpose                                      |
|------------------------|-----------------|-------------|----------------------------------------------|
| LogDir                 | BaseName (derived)| `broker-logs` | Root directory for segment files             |
| ChannelBufferSize      | writeCh capacity | 1024        | Size of write channel buffer                 |
| DiskFlushBatchSize     | batchSize        | 50          | Messages per batch                            |
| LingerMS               | linger           | 50ms        | Max wait before flushing partial batch       |
| DiskWriteTimeoutMS     | writeTimeout     | 10ms        | Timeout for channel enqueue                   |
| DiskFlushIntervalMS    | syncIntervalMS   | 500ms       | Periodic fsync interval                       |

## Asynchronous Write Path

The write path is designed to be non-blocking for publishers while maximizing disk throughput through batching and buffering.

### Step-by-Step Flow

- **AppendMessage**: Publisher calls `AppendMessage(msg)` on partition's DiskHandler
- **Channel Enqueue**: Message is sent to unbuffered writeCh channel
- **flushLoop Dequeue**: Background goroutine receives message from writeCh 
- **Batching**: Messages accumulate in memory slice until batch size or linger timeout
- **writeBatch**: Batch is written to bufio.Writer with length prefixes
- **Flush**: `writer.Flush()` writes buffered data to file
- **Sync**: `file.Sync()` ensures data reaches physical disk (fsync)

## Timeout Handling

When `writeTimeout` is configured and the channel is full, `AppendMessage` enters a retry loop with exponential backoff:

1. **First attempt**: Non-blocking select with default case
2. **Second attempt**: Timed select with time.NewTimer(writeTimeout)
3. **On timeout**: Log warning and retry from step 1

This ensures publishers don't block indefinitely while maintaining message durability guarantees.

## flushLoop: Batching Goroutine

The flushLoop goroutine is the heart of the asynchronous write path. It continuously dequeues messages from writeCh, accumulates them into batches, and flushes to disk based on configurable thresholds.

## Batching Parameters

| Condition         | Threshold                          | Behavior                          |
|------------------|-----------------------------------|----------------------------------|
| Batch Full        | len(batch) >= batchSize (50)       | Immediate flush                  |
| Linger Timeout    | ticker.C fires (50ms)              | Flush if len(batch) > 0          |
| Shutdown          | done channel closed                 | Drain and flush remaining messages|


## writeBatch: Disk Write Implementation

The `writeBatch` function performs the actual disk I/O, writing a batch of messages with length prefixes to the active segment file.

### Message Format on Disk

Each message is written with a 4-byte big-endian length prefix:

| Offset | Length (bytes) | Data (hex)                                   | Message (UTF-8)   |
|--------|----------------|----------------------------------------------|------------------|
| 0      | 11             | 68 65 6C 6C 6F 20 77 6F 72 6C 64           | "hello world"    |
| 15     | 12             | 61 6E 6F 74 68 65 72 20 6D 73 67           | "another msg"    |


### Concurrency Control

The `writeBatch` function uses a dual-mutex strategy for fine-grained concurrency control:

| Mutex | Protects                          | Locked By                                         |
|-------|----------------------------------|--------------------------------------------------|
| mu    | CurrentOffset, CurrentSegment, file | `writeBatch()`, `ReadMessages()`, `rotateSegment()`   |
| ioMu  | writer, `writer.Write()`, `writer.Flush()` | `writeBatch()`, `Flush()`, `WriteDirect()`          |

This separation allows concurrent reads while writes are in progress, as readers use memory-mapped I/
O and don't conflict with the write path.

## Segment Rotation

When CurrentOffset + totalLen > SegmentSize, writeBatch calls `rotateSegment()` to create a new segment file.

## Shutdown and Resource Cleanup

The `Close()` method provides graceful shutdown with guaranteed message durability.

### Drain Loop

The shutdown drain loop ensures all pending messages are flushed to disk:

```
case <-d.done:
    draining := true
    for draining {
        if len(batch) >= d.batchSize {
            d.writeBatch(batch)
            batch = batch[:0]
            continue
        }
        select {
        case msg, ok := <-d.writeCh:
            if !ok {
                draining = false
                continue
            }
            batch = append(batch, msg)
        default:
            draining = false
        }
    }
    if len(batch) > 0 {
        d.writeBatch(batch)
    }
```

## Alternative Write Paths

While `AppendMessage` is the primary write path, DiskHandler also provides alternative methods for specific use cases.

### WriteDirect: Synchronous Write

The `WriteDirect` method bypasses the channel and batching logic for immediate, synchronous disk writes.

| Method        | Write Path                     | Durability                     | Use Case                          |
|---------------|--------------------------------|--------------------------------|----------------------------------|
| `AppendMessage()` | Asynchronous (channel)         | Guaranteed after flush          | High-throughput publishing       |
| `WriteDirect()`   | Synchronous (direct)           | Immediate                       | Critical messages, low volume    |

## Performance Characteristics

### Batching Benefits

The batching mechanism provides several performance advantages:

- **Reduced System Calls**: 500 messages → 1 `write()` syscall
- **Amortized Fsync Cost**: Single `file.Sync()` per batch
- **Better Disk Utilization**: Sequential writes with larger I/O size
- **Lower CPU Overhead**: Fewer context switches between goroutines

## Latency Profile

| Scenario    | Latency      | Notes                                 |
|------------|-------------|---------------------------------------|
| Best Case  | ~10ms       | Batch full immediately, fast disk     |
| Typical    | ~50-150ms   | Linger timeout (100ms) + fsync        |
| Worst Case | >100ms      | Linger timeout + slow disk + rotation |

## Testing

The `handler_test.go` file provides comprehensive test coverage for the write path.

### Test Cases

| Test                        | Purpose                   | Key Assertions                        |
|------------------------------|---------------------------|--------------------------------------|
| TestDiskHandlerBasic         | Basic append and flush    | All messages present in segment files|
| TestDiskHandlerChannelOverflow| Channel backpressure     | Synchronous fallback works correctly |
| TestDiskHandlerRotation      | Segment rotation          | Multiple segment files created       |

## Configuration Examples

### High-Throughput Configuration

```
disk_flush_batch_size: 1000    # Larger batches
linger_ms: 200                  # Wait longer for full batches
channel_buffer_size: 10000      # Deep queue
disk_write_timeout_ms: 5000     # Longer timeout
```

### Low-Latency Configuration

```
disk_flush_batch_size: 100      # Smaller batches
linger_ms: 10                   # Quick flushes
channel_buffer_size: 1000       # Shallow queue
disk_write_timeout_ms: 1000     # Quick timeout
```

### Memory-Constrained Configuration

```
disk_flush_batch_size: 50       # Small batches
linger_ms: 50                   # Moderate linger
channel_buffer_size: 100        # Minimal queue
disk_write_timeout_ms: 500      # Fast timeout
```

Note: For complete configuration reference, see [Configuration Reference](../../user-guide/configuration.md).

# Summary

The DiskHandler write path provides:

- **Asynchronous writes**: Non-blocking AppendMessage for publishers
- **Batching**: Up to 50 messages per batch or 50ms linger timeout (configurable)
- **Durability**: Guaranteed fsync after each batch
- **Concurrency control**: Dual-mutex strategy for parallel reads/writes
- **Graceful shutdown**: Drain loop ensures all messages are persisted
- **Performance tuning**: Configurable batch size, linger time, and channel depth

Key Takeaways:

- Each topic-partition has a dedicated DiskHandler instance
- The `flushLoop` goroutine continuously batches and flushes messages
- Length-prefixed message format enables efficient streaming reads
- Dual mutexes (mu and ioMu) enable concurrent read/write operations
- Configurable parameters allow tuning for throughput vs. latency

