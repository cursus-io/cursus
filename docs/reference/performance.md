# Performance

This document explains the configurable parameters that control cursus's performance characteristics. It covers buffer sizes, batching behavior, linger times, and how to tune these settings for different workload patterns. For information about running performance benchmarks, see [Running Benchmarks](./benchmark.md).

## Overview

The cursus system exposes several tunable parameters that control the trade-off between throughput, latency, and resource utilization. These parameters primarily affect:

- **Channel buffer sizes** - Control memory usage and backpressure behavior
- **Disk batching parameters** - Control disk write efficiency and latency
- **Linger times** - Control maximum latency before forced flush
- **Write timeouts** - Control blocking behavior when channels are full

All parameters can be configured via YAML/JSON configuration files, environment variables, or command-line flags.

## Configuration Parameters

The following table lists all performance-related configuration parameters with their default values and impacts:

| Parameter                  | Config Key                     | Default | CLI Flag                   | Impact                                           |
|----------------------------|-------------------------------|---------|----------------------------|------------------------------------------------|
| Partition Channel Buffer    | channel_buffer_size            | 1024    | --channel-buffer           | Controls DiskHandler write channel depth      |
| Disk Flush Batch Size       | disk_flush_batch_size          | 50      | --disk-flush-batch         | Messages per disk flush operation             |
| Linger Time                 | linger_ms                      | 50      | --linger-ms                | Maximum wait time before forced flush         |
| Partition Channel Buffer    | partition_channel_buffer_size  | 10000   | --partition-ch-buffer      | Partition message queue size                  |
| Consumer Channel Buffer     | consumer_channel_buffer_size   | 1000    | --consumer-ch-buffer       | Per-consumer message queue size               |
| Disk Write Timeout          | disk_write_timeout_ms          | 10      | --disk-write-timeout       | Timeout for synchronous writes when channel full |


## Configuration Hierarchy

Configuration Precedence (highest to lowest):

- Command-line flags (highest priority)
- YAML/JSON configuration file
- Environment variables
- Default values (lowest priority)

## Channel Buffer Configuration

The broker system uses three types of buffered channels, each serving different purposes in the message flow pipeline.

### Partition Channel Buffer

The `partition_channel_buffer_size` parameter controls the input queue depth for each partition. This is the primary buffer for handling bursts of incoming messages.

### Configuration:

Default: 1024 messages

Config key: channel_buffer_size

CLI flag: --partition-ch-buffer

Impact:
- Higher values: Better burst handling, more memory usage, higher maximum latency during backpressure
- Lower values: Less memory usage, faster backpressure propagation, potential message loss if publishers don't handle blocking

Tuning guidelines:
- For high-throughput scenarios with bursty traffic: 10000-50000
- For low-latency scenarios with steady traffic: 1000-5000
- For memory-constrained environments: 500-2000

## Consumer Channel Buffer

The `consumer_channel_buffer_size` parameter controls the message queue size for each individual consumer within a consumer group.

### Configuration:

Default: 1000 messages

Config key: consumer_channel_buffer_size

CLI flag: --consumer-ch-buffer

Impact:
- Higher values: Allows slower consumers to lag without blocking partition distribution
- Lower values: Faster detection of slow consumers, less memory per consumer

### Tuning guidelines:

- For slow/variable consumer processing: 2000-5000
- For fast, consistent consumers: 500-1000
- When running many consumers: 100-500 (to reduce total memory)

## Disk Write Channel

The `DiskHandler.writeCh` is intentionally unbuffered to provide immediate backpressure to the partition layer. This ensures that disk write performance directly affects the publish rate.

- **Key characteristic**: When writeCh is full (or cannot accept immediately), the partition layer will block, creating natural flow control.

## Disk Write Batching

The disk persistence layer uses a batching mechanism to amortize the cost of `fsync()` operations across multiple messages. This is the most significant performance tuning parameter for write-heavy workloads.

## Batch Size Configuration

The `disk_flush_batch_size` parameter controls how many messages are accumulated before performing a disk flush operation.

### Configuration:

Default: 500 messages

Config key: disk_flush_batch_size

CLI flag: --disk-flush-batch

Impact:
- **Higher values**: Better throughput (fewer fsync() calls), higher latency, more messages at risk during crashes
- **Lower values**: Lower latency, more frequent disk I/O, reduced throughput

### Tuning guidelines:
- For maximum throughput: 1000-5000
- For balanced throughput/latency: 200-500
- For minimum latency: 50-100
- For durability-critical systems: 1-50 (with longer linger time)

## Linger Time Configuration

The `linger_ms` parameter sets the maximum time the system will wait before flushing a partial batch to disk. This prevents messages from sitting in memory indefinitely when traffic is low.

### Linger Time Mechanism

Configuration:

Default: 100 milliseconds

Config key: linger_ms

CLI flag: --linger-ms

Impact:
- **Higher values**: Better throughput under low traffic (more time to accumulate batches), higher worst-case latency
- **Lower values**: Lower latency guarantee, more frequent small flushes, reduced efficiency

### Tuning guidelines:

- For latency-sensitive applications: 10-50ms
- For balanced performance: 50-100ms
- For throughput-optimized: 100-500ms
- For batch-processing workloads: 500-2000ms

## Tuning Strategies

### High-Throughput Configuration

Optimize for maximum message throughput at the cost of higher latency:

```
broker:
  channel_buffer_size: 50000          # Large burst absorption
  disk_flush_batch_size: 2000         # Large batches
  linger_ms: 500                      # Long linger for batch accumulation
  partition_channel_buffer_size: 50000
  consumer_channel_buffer_size: 5000
```

Expected behavior:
- Messages batched into groups of 2000
- Maximum latency: 500ms (linger timeout)
- High throughput due to amortized fsync cost
- Large memory footprint

Use cases: 
- Log aggregation, analytics pipelines, batch ETL

### Low-Latency Configuration

Optimize for minimum message latency at the cost of reduced throughput:

```
broker:
  channel_buffer_size: 1000           # Small buffers for fast flow
  disk_flush_batch_size: 50           # Small batches
  linger_ms: 10                       # Very short linger
  partition_channel_buffer_size: 2000
  consumer_channel_buffer_size: 500
```

Expected behavior:

- Messages flushed quickly in small batches
- Maximum latency: 10ms (linger timeout)
- Lower throughput due to frequent fsync
- Small memory footprint

Use cases: 
- Real-time messaging, trading systems, alerting

### Balanced Configuration (Default)

The default configuration provides a reasonable balance:

```
broker:
  channel_buffer_size: 10000          # Moderate burst handling
  disk_flush_batch_size: 500          # Moderate batching
  linger_ms: 100                      # 100ms max latency
  partition_channel_buffer_size: 10000
  consumer_channel_buffer_size: 1000
```

Expected behavior:
- Good throughput with acceptable latency
- Maximum latency: 100ms
- Reasonable memory usage

Use cases: 
- General-purpose message broker, microservices communication

## Performance Trade-offs

### Batching vs. Latency

The fundamental trade-off in disk persistence:

| Batch Size | Throughput   | Avg Latency | P99 Latency | Fsync/sec       | Memory  |
|------------|-------------|------------|------------|----------------|---------|
| 10         | Low         | 5ms        | 10ms       | High (~1000)   | Low     |
| 100        | Medium      | 25ms       | 50ms       | Medium (~100)  | Low     |
| 500        | High        | 50ms       | 100ms      | Low (~20)      | Medium  |
| 2000       | Very High   | 150ms      | 500ms      | Very Low (~5)  | High    |

**Note:** Actual values depend on disk I/O characteristics and message size.

### Buffer Size vs. Memory

Memory consumption calculation:

```
Total Memory = (num_partitions × partition_buffer_size × avg_msg_size) + (num_consumers × consumer_buffer_size avg_msg_size)
```

Example:

```
100 topics × 4 partitions = 400 partitions
10000 messages/partition buffer
1KB average message size
```
Result: ~3.8GB for partition buffers alone


## Monitoring Performance Settings

### Key metrics to monitor when tuning performance:

Channel depth metrics - How full are the buffers?
- If consistently full: Increase buffer size or improve downstream performance
- If consistently empty: Decrease buffer size to save memory

Flush frequency - How often does `writeBatch()` execute?
- Due to batch size: Good (efficient batching)
- Due to linger timeout: Consider increasing linger or batch size

Disk I/O latency - How long does `file.Sync()` take?
- High latency: Consider faster disk or larger batches
- Variable latency: Increase buffers to absorb variance
- Message latency P50/P99 - End-to-end message timing

For detailed metrics collection, see [Monitoring and Observability](./observability.md).

# Platform-Specific Optimizations

The disk layer includes platform-specific optimizations that affect performance:

## Linux Optimizations

On Linux systems, the codebase uses:

- **fadvise(SEQUENTIAL)** - Hints kernel to perform read-ahead
- **sendfile(2)** - Zero-copy data transfer for reads

These optimizations are automatically enabled on Linux builds and can significantly improve read performance.

## Windows Implementation

Windows builds use alternative implementations without zero-copy transfers, which may result in lower performance for read-heavy workloads.

For details on platform-specific implementations, see [Platform-Specific Optimizations](../core/storage/platform-optimizations.md).

## Configuration Examples by Workload

### Event Streaming (High Volume, Moderate Latency)

```
broker:
  channel_buffer_size: 25000
  disk_flush_batch_size: 1000
  linger_ms: 200
  partition_channel_buffer_size: 25000
  consumer_channel_buffer_size: 2000
```

### Request/Response Pattern (Low Latency)

```
broker:
  channel_buffer_size: 2000
  disk_flush_batch_size: 100
  linger_ms: 20
  partition_channel_buffer_size: 2000
  consumer_channel_buffer_size: 500
```

### Log Aggregation (Maximum Throughput)

```
broker:
  channel_buffer_size: 100000
  disk_flush_batch_size: 5000
  linger_ms: 1000
  partition_channel_buffer_size: 100000
  consumer_channel_buffer_size: 10000
```

# Summary

Performance tuning in cursus involves balancing three primary parameters:

- `disk_flush_batch_size` - Controls throughput vs latency trade-off (default: 50)
- `linger_ms` - Sets maximum message latency guarantee (default: 50ms)
- `channel_buffer_size` / `partition_channel_buffer_size` / `consumer_channel_buffer_size` - memory usage and burst handling (defaults: 1024/10000/1000)

## Key principles:

- Increase batch size and linger for throughput-oriented workloads
- Decrease batch size and linger for latency-sensitive workloads
- Increase buffer sizes for bursty traffic patterns
- Monitor channel depths and flush patterns to validate tuning

For benchmarking your specific configuration, see [Running Benchmarks](./benchmark.md).
