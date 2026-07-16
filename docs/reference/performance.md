# Performance And Tuning

Cursus throughput comes from partition parallelism, buffered batch writes, sparse indexes, mmap reads, and optional platform-specific I/O paths. Tune only after measuring the real durability, retention, transaction, and replication settings used in production.

## Default Profile

| Setting | Default | Effect |
|---|---:|---|
| `channel_buffer_size` | 1024 | Pending records per partition storage writer. |
| `partition_channel_buffer_size` | 10000 | In-process partition dispatch capacity. |
| `consumer_channel_buffer_size` | 1000 | In-process consumer receive capacity. |
| `broadcast_channel_buffer_size` | 10000 | Broadcast/fan-out capacity. |
| `disk_flush_batch_size` | 50 | Records accumulated before a batch write. |
| `linger_ms` | 50 | Maximum wait for a partial async batch. |
| `disk_flush_interval_ms` | 500 | Data/index `Sync` interval. |
| `disk_write_timeout_ms` | 10 | Buffered enqueue timeout. |
| `log_segment_bytes` | 1 GiB | Data segment roll limit. |
| `log_index_size_bytes` | 10 MiB | Sparse index roll limit. |
| `log_index_interval_bytes` | 4096 | Approximate bytes between sparse entries. |

`writeCh` is buffered, not unbuffered. A larger buffer absorbs short bursts but uses more memory and can widen the amount of work queued inside the process.

## Write Path Trade-offs

### Batch And Linger

A larger batch or linger can improve sequential write efficiency and throughput. It also increases queueing latency and the number of records waiting for the next flush. A smaller value reduces latency at the cost of more writer/syscall overhead.

### Sync Interval

The batch writer flushes Go buffers to the file descriptor; the sync loop controls periodic file `Sync`. Shortening the interval reduces the unsynced power-loss window and increases storage pressure. Do not describe a successful async enqueue as durable.

Publish acknowledgements also depend on the selected `acks` mode, replication quorum, and HWM path. Test broker kill, host restart, and leader failover when changing durability-related settings.

### Segment And Index

Larger segments reduce file turnover but lengthen worst-case active-tail scans and increase retention granularity. A smaller sparse-index interval speeds seeks while increasing index writes and file size. Segment rolls also occur on index capacity or time.

## Read Path Trade-offs

Raw storage reads use the sparse index followed by mmap scanning. `read_committed` adds HWM/LSO checks and transaction visibility filtering. Its in-memory transaction index avoids repeatedly scanning every later marker, but unresolved transactions can hold the stable boundary and reduce visible progress.

Large poll batches reduce command overhead but increase handler latency and retry work. Stream batches reduce reconnect overhead but still require heartbeats, generation fencing, and offset commit discipline.

## Partitioning

Partition count is the main unit of producer, consumer, and disk parallelism. Too few partitions cap concurrency; too many create files, goroutines, coordinator assignments, metrics, and replication work. Keyed records preserve ordering only inside the selected partition. Increasing partition count can remap future records for the same key.

## Transaction Cost

A broker transaction adds:

- durable coordinator state transitions,
- idempotent output validation/appends,
- one control marker per touched output partition,
- one fenced bulk consumer offset update,
- final decision replication and `read_committed` visibility checks.

Batch related records and offsets into a meaningful transaction, but avoid unbounded transactions: an open transaction holds the stable visibility boundary on touched partitions. All staged source offsets must belong to one consumer scope.

Coordinator synchronization currently persists the complete staged transaction snapshot after each mutation. Very large transactions therefore amplify standalone journal and distributed metadata traffic; keep transaction batches bounded and measure coordinator latency as well as partition throughput. A standalone encoded snapshot record is limited to 32 MiB. The standalone journal is append-only and does not yet compact superseded snapshots automatically.

## Example Profiles

### Throughput-oriented starting point

```yaml
channel_buffer_size: 10000
disk_flush_batch_size: 500
linger_ms: 10
disk_flush_interval_ms: 250
log_segment_bytes: 1073741824
log_index_interval_bytes: 8192
```

### Latency-oriented starting point

```yaml
channel_buffer_size: 1024
disk_flush_batch_size: 10
linger_ms: 2
disk_flush_interval_ms: 100
log_index_interval_bytes: 4096
```

These are starting points, not safe production defaults. Validate storage latency, recovery time, quorum behavior, and power-loss tolerance.

## Metrics To Watch

- publish/consume rate and latency,
- storage pending writes, segment bytes/count, active readers, and stat failures,
- partition earliest/latest/HWM/LSO,
- group lag, rebalance, heartbeat, and offset-gap counters,
- transaction command errors and known transaction state via `TXN_STATUS` (state-count and committing-age metrics are not exported yet),
- replication lag, ISR size, quorum failures, and leader changes,
- process memory, goroutines, file descriptors, disk latency, and disk free space.

## Benchmark Discipline

Run benchmarks with a cold and warm storage cache, retained history, the intended isolation level, and both standalone and cluster modes. A run is valid only when publish count, unique consumed count, missing count, duplicate count, commit errors, and process exit status agree. Container build/compose noise should be separated from the benchmark summary, but broker/consumer error lines must remain available for diagnosis.

See [Benchmarks](benchmark.md) and [Benchmark Verification](../benchmark-verification.md).
