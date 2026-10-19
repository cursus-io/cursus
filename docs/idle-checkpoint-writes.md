# Unchanged partition checkpoints

## Baseline and scope

On 2026-10-20 (Asia/Seoul), a fresh fetch of `cursus-io/cursus` resolved
`origin/main` to `35cf766a6e97e0549bbb589cc0b4d4dc40f6595b`, the supplied deployed
commit. The tree diff is empty. The one-second catch-up scan still invokes
`FlushDisk` for an already-in-ISR replica at/above committed HWM, and both
checkpoint writers still replace their files unconditionally.

The supplied production observation was approximately 0.71 CPU cores,
602 cgroup writes/s and 0.80 MB/s across three Cursus pods. Of 243 compared
`.hwm`/`.producers` files, 231 changed mtime without changing SHA-256 over two
snapshots separated by 15 seconds. ARC runner pods were absent, but a separate
host runner was active. These observations identify repeated checkpoint work;
they do not establish that the whole node was idle or predict a CPU reduction.
This change does not alter MySQL, deploy an image, or tune flush intervals.

## Kafka comparison and design

The reference is released **Apache Kafka 4.0.0**, not its moving trunk.

| Reference | Adopt | Cursus difference and consequence |
| --- | --- | --- |
| [Accepted KIP-101](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=177052956) | A local HWM alone is not proof of a safe truncation boundary; retain leader-epoch lineage checks. | This patch preserves Cursus's existing leader/lifecycle/prefix verification and Raft-authoritative rollback entry points. Checkpoint equality neither authorizes truncation nor admits a replica to ISR. |
| [ProducerStateManager.takeSnapshot](https://github.com/apache/kafka/blob/4.0.0/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateManager.java) | Publish a new snapshot only when persistence state changes; acknowledge the captured snapshot after success. | Kafka uses map/snapshot offsets. Cursus must compare producer epoch, sequence, record offset, coverage and deletions: rollback/refill can change state at the same offset. TTL removes the memory cache only; retained fencing entries remain until retention removes their records. |
| [LocalLog.flush and markFlushed](https://github.com/apache/kafka/blob/4.0.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java) | Keep data flush progress separate from metadata checkpoint progress. | All existing Cursus `StorageHandler.Flush` calls, synchronous append barriers, periodic disk syncs and replication fences remain. Checkpoint equality cannot prove that queued data is durable. |
| [CheckpointFile.write](https://github.com/apache/kafka/blob/4.0.0/server-common/src/main/java/org/apache/kafka/server/common/CheckpointFile.java) | Serialize publication and use a temporary file, file sync, then replacement. | Cursus also requires successful parent-directory sync on Unix before caching success. Windows retains its existing `MOVEFILE_REPLACE_EXISTING \| MOVEFILE_WRITE_THROUGH`. Errors invalidate the prior saved image, including errors after rename. |
| [ReplicaManager.checkpointHighWatermarks](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/ReplicaManager.scala) | Keep committed visibility distinct from the local log tail and preserve shutdown checkpointing. | Kafka periodically writes grouped HWM checkpoints; it does **not** provide a general unchanged-HWM skip here. Cursus has per-partition files and Raft-driven recovery; temporary snapshot visibility must not overwrite the pre-replay durable HWM. |
| [Official 4.0 topic configuration](https://kafka.apache.org/40/generated/topic_config.html#topicconfigs_flush.messages) | Understand the distinction between replicated durability and physical flush policy. | Kafka recommends replication/OS flushing in its default operating model. This is not permission to remove Cursus fsyncs or weaken its existing synchronous API guarantees. |

Before implementation, the comparison defined the regression checks: zero
checkpoint operations for unchanged state with a retained data barrier; durable
publication of each changed component; retry at every failure stage including
post-rename directory failure; concurrent state changes; monotonic ordinary HWM;
explicit rollback; snapshot replay; producer expiration/retention; process exit
at publication boundaries; and restart reconstruction work/bytes.

## Implementation and invariants

The HWM writer takes its publication mutex before reading partition state and
releases the partition read lock before disk I/O, while retaining the publication
mutex. Therefore a writer cannot capture an old HWM, wait behind a newer
publication, then overwrite it. Partition updates are not blocked by checkpoint
I/O; updates made after capture differ from the saved image and signal another save. Normal
HWM advancement remains monotonic; explicit fenced rollback can publish a lower
value. Snapshot replay suppresses temporary HWM publication until finalization.

The producer writer compares the entire retained-state map and coverage under
`producerStateMu`. An unchanged call allocates no snapshot and does no file I/O.
A changed call copies an immutable image, releases the state lock, and persists
it under the publication mutex. Success records that image, never the live map.
Concurrent updates remain different on the next attempt. No mutation-maintained
dirty bit can accidentally miss a recovery, retention, or same-offset refill.
The trade-off is O(number of retained producers) equality work per idle call and
one additional retained map; changed snapshots still require serialization.

A failed publication invalidates the saved image even when rename succeeded.
Returning to the former value therefore retries rather than trusting an image
that may no longer exist on disk. The HWM maintenance loop retains pending work
after failure. Explicit flush failures signal maintenance for another attempt.
This does not change the public `FlushDisk()` signature or promise callers a
new error-returning durability API.

Valid loaded checkpoints seed comparison state. Filtered producer entries cause
a subsequent rewrite. Truncation clears producer state with coverage reset to
the retained start, then reconstruction and its notification restore the correct
prefix. Reconstruction still runs if publishing the intermediate empty image
fails: truncation has already happened, so leaving the live producer map empty
would lose retained deduplication/fencing state. Retrying at the already-truncated
LEO retries publication of the recovered image. A recovery read failure still
fences the partition. An empty reconstruction also signals maintenance. Producer TTL cache
eviction alone does not discard retained deduplication/fencing state.

Recovery continues to scan the retained log in bounded record pages; this PR
does not introduce a new replay shortcut or claim fewer recovery bytes. Removing
unchanged writes cannot increase the age of the last *changed* checkpoint.
Persistent storage errors still reduce availability: affected saves keep retrying
and existing disk health/fencing behavior applies. There is no checkpoint format,
wire-protocol, leadership or ISR-admission change.

## Validation and measurement

`TestCheckpointUnchangedFlushHasZeroFileOperations` counts open, write, written
bytes, file sync, close, rename and directory sync through real filesystem
operations. It verifies zero additional checkpoint operations for 100 flushes,
while all 101 data flush calls still occur. Tests cover independent HWM/coverage
changes, all publication failures, reverting after a failed publication,
concurrent producer updates, concurrent HWM writers, background retry without
new mutations, and temporary snapshot visibility. An FSM test executes repeated
in-ISR catch-up scans across a leader-epoch change and verifies stable file bytes
and mtimes. Existing rollback, truncation, TTL, retention, epoch fencing and
shutdown/restart tests remain part of the full topic suite.

Crash tests launch a child process and exit without graceful close before file
sync, before rename, after rename and before directory sync, for both checkpoint
types. Restart verifies the exact old/new checkpoint image, retained payloads
and producer duplicate suppression. Each explicit reconstruction reads one page,
two records and 11 payload bytes; record framing/index I/O is not counted as
payload bytes. Normal shutdown/restart also verifies changed producer state and
that valid loaded images avoid rewriting on a subsequent flush and close.
This is a process-crash test, not a power-loss emulation of a storage controller.

The opt-in Linux `TestCheckpointWorkload` runs 3 FSM instances, 8 partitions
each, real DiskHandlers and the actual `BuildReplicaCatchupRequests` path once
per second. The load case appends 2 logical 1-KiB messages/s to all 3 replicas
and applies epoch-fenced partition commits. Default data sync settings are
unchanged. Initial state and one-second settling are excluded. It records
process user/system CPU, `/proc/self/io`, host CPU counters with iowait separate,
and append/local-commit p50/p95/p99. Local commit includes FSM application and
disk barriers; it excludes Raft networking and client RTT. Run the identical
workload source against both revisions. System-call tracing is a separate run
so tracing overhead does not contaminate the CPU/latency comparison.

Measured results and environment evidence are recorded alongside this document
in `idle-checkpoint-measurements.md`. Production rollout and a production-scale
network latency comparison remain separate operational validation.
