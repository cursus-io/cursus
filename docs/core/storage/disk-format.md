# Disk Format

This document describes the Cursus partition-log files. The format is Cursus-owned and may evolve only with compatible recovery and upgrade handling.

## Layout

Each topic partition has independent segment and sparse offset-index files:

```text
{log_dir}/{topic}/
  partition_{partition}_segment_{baseOffset:020d}.log
  partition_{partition}_segment_{baseOffset:020d}.index
```

The segment number is the first logical record offset in that segment, not an ordinal file number. The default `log_segment_bytes` is 1 GiB and is configurable. A segment also rolls when its index would exceed `log_index_size_bytes` or when the configured time-based roll interval expires.

## Segment Record

A log segment is a sequence of length-prefixed records:

```text
uint32_be serializedLength
byte[serializedLength] serializedDiskMessage
```

The maximum accepted serialized record size is 16 MiB. The serialized message uses big-endian fixed-width integers and length-prefixed strings/bytes in this order:

| Field | Encoding |
|---|---|
| topic | `uint16` length + UTF-8 bytes |
| partition | `uint32` |
| logical offset | `uint64` |
| producer id | `uint16` length + bytes |
| producer sequence | `uint64` |
| producer epoch | `uint64` representation of non-negative `int64` |
| payload | `uint32` length + bytes |
| event type | `uint16` length + bytes |
| schema version | `uint32` |
| aggregate version | `uint64` |
| metadata | `uint16` length + bytes |
| key | `uint16` length + bytes |
| transactional id/state/marker | three `uint16` length-prefixed values |
| control batch type | `uint16` length + bytes |
| control batch version | `int16` |
| control coordinator epoch | `int64` |
| control key/value | two `uint16` length-prefixed byte arrays |

The transaction/control fields are empty for ordinary records. Control records remain in the raw log and are filtered by `read_committed`.

## Sparse Offset Index

Each `.index` file contains fixed 16-byte entries:

```text
uint64_be logicalOffset
uint64_be bytePosition
```

An entry is written after the configured byte interval (default 4096 bytes). Reads locate the segment and nearest indexed byte position, then scan and validate contiguous logical offsets. Segment data is opened through mmap for reads.

## Write And Sync Contract

Asynchronous writes enter a buffered `writeCh` (default capacity 1024). `flushLoop` serializes and writes a batch when any of these occurs:

- `disk_flush_batch_size` records are ready (default 50),
- `linger_ms` expires (default 50 ms),
- an explicit flush or shutdown drains pending records,
- a segment roll is required.

`WriteBatch` flushes the Go buffered data and index writers to the operating-system file descriptors. `syncLoop` calls `Sync` for data and index files at `disk_flush_interval_ms` (default 500 ms) and advances the partition durability callback after a successful sync. Explicit `Flush`, segment rotation, and shutdown also sync data.

A write being visible in the process page cache is different from surviving power loss. Client acknowledgements and replicated HWM advancement must be interpreted according to the selected publish/replication path and the synced committed-tail contract, not merely successful channel enqueue.

## Recovery

Startup recovery scans the active segment, validates every length and decoded record, checks contiguous offsets, and truncates an invalid or partial tail. It rebuilds or opens the sparse index and clamps recovered committed high watermarks to the durable tail.

Partition-owned side checkpoints include:

- synced high-watermark state,
- producer epoch/sequence state (also rebuildable from records),
- event-stream indexes and snapshots where enabled.

Transaction visibility indexes are rebuilt from durable transaction records and markers. The durable transaction coordinator decision remains a separate metadata authority.

## Standalone Transaction Journal

A standalone broker stores coordinator snapshots in `{log_dir}/__transaction_state.journal`. This file is separate from partition segments. Each record uses the following versioned frame:

    uint32_be payloadLength
    byte[payloadLength] JSON {"version":1,"transaction":{...}}
    uint32_be crc32(payload)

The encoded payload is limited to 32 MiB. Every accepted transition is appended and fsynced. Before appending, the broker truncates bytes beyond the last validated record so a failed partial write cannot hide later acknowledged state. Startup repairs only a torn or checksum-corrupt final frame and rejects corruption before the tail. The decoder accepts the earlier bare-snapshot JSON payload for recovery compatibility, but all new writes use version 1.

The journal is append-only and currently has no automatic compaction. Backups must keep it consistent with partition logs and the standalone consumer offset store.

## Retention And Deletion

Time/size retention removes eligible closed segments through the retention loop. The active segment is preserved. `log_cleanup_policy=delete` is the only implemented cleanup policy; compaction is not implemented and unsupported values normalize to `delete`.

Readers can hold references while retention runs. Deletion uses the storage lifecycle to avoid removing an actively read segment and returns `OFFSET_OUT_OF_RANGE` when a requested logical offset predates the earliest retained segment.

## Concurrency

`DiskHandler` separates metadata/file ownership (`mu`), serialized writer operations (`ioMu`), and index reader/writer state (`indexMu`). Write paths acquire metadata before I/O locks. Readers copy the segment list under a short lock and release it before mmap scanning.

## Platform Notes

Linux builds can apply sequential-access advice and zero-copy transfer where the selected path supports it. Windows uses portable file operations. The broker does not rely on `O_DIRECT` or per-batch `O_SYNC` as a universal format guarantee.

## Defaults

| Setting | Default |
|---|---:|
| Segment size | 1 GiB |
| Index size | 10 MiB |
| Sparse index interval | 4096 bytes |
| Async channel capacity | 1024 records |
| Flush batch | 50 records |
| Linger | 50 ms |
| File sync interval | 500 ms |
| Cleanup policy | `delete` |
