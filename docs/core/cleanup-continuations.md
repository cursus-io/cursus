# Bounded consumer cleanup continuations

## Incident and scope

On `aff3c938cb26ba374aba97d162cfa0c15a686c65`, a successful bounded scan
returned the same pending error shape as a failed read. The materialization
worker retried all topics only on its five-second ticker. The limit was 128
records or 1 MiB, so 54,164 historical offsets required at least 424 batches;
that is roughly 35 minutes of polling alone, before byte-limited batches.
Offsets describe historical range, not the number of current groups.

This change does not delete log data, reset offsets, bypass validation, change
startup probes, or deploy anything. It changes how local work is scheduled and
how validated cleanup progress survives restart.

## Kafka comparison and design checks

Primary sources examined:

* [Kafka 4.1.0 OffsetMetadataManager](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java):
  current offsets live in timeline maps indexed by group/topic/partition;
  expiration visits current state and produces tombstones.
* [Kafka 4.1.0 LogCleaner](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaner.java)
  and [official compaction design](https://kafka.apache.org/41/design/design/):
  background cleaners build key-to-latest-offset maps and rewrite segments,
  with I/O throttling and tombstone retention.
* [Accepted KIP-848](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848+The+Next+Generation+of+the+Consumer+Rebalance+Protocol):
  describes coordinator event loops, replicated state, epochs and incremental
  reconciliation. Proposal text is not evidence that every optional KIP feature
  is implemented; the versioned source above establishes the concrete map and
  tombstone behavior used in this comparison.

Adopt bounded background work, explicit continuation and independently delayed
failures. Cursus must retain its committed-history proof scan: a local group
cache does not prove a newer whole-group deletion. It must check all partitions,
registration epochs, later live records, transaction-control envelopes, scope,
identity/generation and the final committed cut. Distributed internal metadata
compaction remains disabled. Kafka offset expiration and segment compaction are
different operations and are not throughput baselines for this proof scan.

Regression/fault checks cover record/byte/time exhaustion, malformed pages and
metadata, valid transaction controls, new commits at the final cut, replacement,
rollback, changed targets, checkpoint corruption, failed generation publication,
cancellation, independent errors/waits, short-topic fairness and replica checks.
Recovery accounting includes actual decoded records and storage-frame bytes.

## Scheduling and bounds

The existing single manager worker selects one due topic in rotating name order.
An advancing bounded scan returns `continue`; it yields for 1 ms and resumes.
External waits use the normal interval. Actual failures use independent capped
exponential backoff (5, 10, 20, 40, then 60 seconds by default). Replica inventory
checks retain their own five-second deadline. Fast work does not accelerate
failing topics or replica inventory scans. No per-topic goroutines are created.

Each scan retains the 128-record, 1-MiB and 128-page limits, plus a cooperative
10-ms deadline checked between pages. Cancellation is checked between pages and
disk records; view acquisition uses cancellable partition/storage locks and
nonblocking coordinator lifecycle admission. System calls, JSON decoding of one
bounded page, existing ownership callbacks and durable writes are not forcibly
preempted. The deadline is not a hard real-time bound on one syscall.

A typed byte-budget result distinguishes insufficient remaining batch space
from a record exceeding the full 1-MiB allowance. The former continues from the
same cursor with a fresh budget; the latter remains a clear, backed-off error.
No cursor moves past an unvalidated record. Empty compacted segments must move
the physical cursor, otherwise the result is an error, not a continuation.

## Persistent progress and durability

Each unfinished topic can save a versioned, checksummed checkpoint under
`<log-dir>/.consumer-cleanup/<sha256-topic>.json`. It contains selected target
epochs/generations, accumulated lifecycle evidence, all partition bounds and
physical cursors. It contains no event payloads. Saves occur after the first
batch, then after 1,024 records or 8 MiB, and at the completed cut. The actual
uncheckpointed work can include one bounded batch beyond a threshold. A failed
save during an incomplete scan is an error with backoff. A completed scan logs
the cache failure and proceeds through the normal final validation and apply;
a disposable checkpoint cannot block an already complete proof. A failed,
missing or stale load falls back to scanning.

Partitions expose a local durable 32-byte random generation file. Appends keep
it. Every existing prefix-mutation gate overwrites and fsyncs it **before** data
mutation; failure prevents the mutation. Bootstrap/range recovery and torn-tail
repair also invalidate it. This file is never a remote replication proof.
Checkpoint publication syncs the referenced active logs, writes and fsyncs a
temporary checkpoint, then atomically replaces the previous file and syncs the
parent directory on Unix (Windows uses write-through replacement). Losing a new
directory entry can only lose the cache or expose an older validated checkpoint.

Loading requires the same topic instance/lifecycle and durable partition tokens,
retained starts, sufficient committed ends, selected group epochs/generations,
valid bounds and checksum. It rebinds process-local partition/group identities
and still performs final committed-cut and scope validation before deletion.
Changed or missing evidence never authorizes deletion. Restarts can repeat work
since the last checkpoint; generation changes require a full validated scan.

This is a local cache format addition, not a wire/Raft/log-record format change.
Older binaries do not maintain the new cache fence: remove the derived
`.consumer-cleanup` checkpoints before running a downgraded binary against these
volumes. Do not copy checkpoints independently of their partition data and
generation files. Full coordinator startup/admission replay is unchanged; the
checkpoint only saves the additional topic-cleanup proof scan.

## Reproduction and measurement

`TestCleanupHistoryMeasurement` is opt-in with `CURSUS_CLEANUP_MEASURE=baseline`
or `continuation`. The same test compiles on the incident commit. It creates
56,000 records over four real disk partitions, with runs of twenty 60-KiB records
every 512 offsets. It reports active wall time, process CPU time, allocations,
sampled heap, scanned bytes/records and concurrent unrelated-operation latency.
Five-second historical waits are virtual; no measured wall-time claim includes
those unslept waits. `TestMaterializationScheduler54164HistoryDoesNotAccumulatePollingDelay`
independently exercises the actual scheduler with an injected clock.

`TestCleanupCheckpointReopenSkipsCertifiedDiskWork` closes/reopens actual storage
and restores stale group state after normal coordinator recovery. In the measured
8,192-record run, cleanup had read 4,096 records / 1,413,096 bytes before restart;
after restart it read 4,992 records / 1,722,240 bytes. It reused evidence for
3,200 records and repeated 896 records. General startup replay is excluded from
these cleanup-only counters and still reads the metadata log.

Performance results and limitations are recorded in the accompanying measurement
report. These local synthetic workloads do not establish production latency,
physical-device I/O, a physical power-loss guarantee or full Raft/network load
performance. No persistent current-state index or compaction engine is added.
