# Log Compaction

Cursus compacts keyed application-topic records in standalone and distributed mode. Compaction retains the latest ordinary record for each key while preserving logical offsets and broker recovery metadata.

## Scope

Supported cleanup policies are:

| Policy | Behavior |
|---|---|
| `delete` | Delete complete closed segments by retention time or retained bytes. |
| `compact` | Rewrite eligible closed segments and remove superseded keyed records. |
| `delete,compact` | Run both maintenance policies on their independent intervals. |

Event-sourcing topics reject compaction because aggregate replay requires complete history. Broker-owned distributed metadata is also excluded. A distributed application topic accepts `compact` or `delete,compact` only when every active broker advertises lifecycle protocol version 2.

Accepting the topic policy does not force a cleaner pass. Every distributed
partition evaluates a fail-closed runtime gate and skips compaction unless:

- the authoritative topic definition and partition metadata are available,
- committed HWM provenance is explicit,
- every configured replica is active and the complete replica set is in ISR,
- every replica advertises lifecycle protocol version 2,
- the local broker is an in-sync configured replica,
- local and FSM lifecycle epochs and cleanup policies match, and
- local LEO and HWM both equal the authoritative committed HWM.

These conditions prevent a rewritten segment generation from being installed
while replica membership, topic identity, or committed visibility is
ambiguous.

## Record Selection

Compaction operates independently per partition. A record can be removed only when all of the following are true:

- it has a non-empty message key,
- it has no transaction or control-batch metadata,
- a later ordinary record with the same key exists in the partition,
- it is not the latest durable record for its producer ID.

Unkeyed records, transactional records, transaction markers, and control records are always retained. The latest record for each producer is retained as a producer sequence recovery anchor. An empty payload has no special expiry behavior; when it is the latest keyed value, it remains present.

The active segment is never rewritten or used as a compaction input. Updates in the active segment become eligible only after that segment rolls. This can retain an older value for one extra pass, but it cannot remove the latest closed value prematurely.

## Offset Contract

Compaction never renumbers records and never moves the partition LEO or HWM. Removed records create physical holes in closed segments.

A read from a removed offset returns the first retained record whose offset is greater than or equal to the requested offset. Consumer group commits remain `nextOffset` values, so a resumed consumer does not replay a compacted-away value. Unlike retention deletion, compaction does not move the earliest logical segment base solely because records were removed.

The active segment remains contiguous and uses strict recovery validation. Closed compacted segments require strictly increasing offsets but may contain gaps.

Consumers refresh the authoritative cleanup policy from topic metadata. On a
policy that includes `compact`, a forward offset jump is classified as an
expected compacted hole and increments
`cursus_consumer_compacted_offsets_skipped_total`. The same jump on a
non-compacted topic increments `cursus_consumer_offset_gap_total`. Missing or
invalid cleanup policy metadata never enables the compacted-hole
classification.

## Rewrite And Recovery

The cleaner serializes with delete retention but keeps producer appends running during closed-segment scans and temporary-file construction:

1. snapshot the set of closed segments,
2. scan that snapshot to identify latest key and producer offsets,
3. calculate superseded bytes in the same closed-segment snapshot,
4. rewrite only closed segments when the dirty-byte ratio reaches `log_min_cleanable_dirty_ratio`,
5. rebuild each rewritten sparse index,
6. fsync same-directory temporary files and a versioned `.log.compacted-<size>` sidecar,
7. install the sidecar before atomically replacing the log, then the index, and sync the parent directory where the platform supports it.

The partition metadata lock is held only for the final replacement and directory sync. A segment that rolls while a pass is running is considered on the next pass.

Readers permit logical offset holes only when a valid sidecar exists and its encoded size matches the current log file. A sidecar installed before a failed log replacement does not match the old larger file and is removed at startup. Once the compacted log is visible, its marker is already durable. Older size-bound markers remain until startup cleanup so an interrupted replacement still recognizes whichever log generation survived. Ordinary closed segments without a matching marker stay on strict contiguous-offset validation.

The log is replaced first because it is authoritative. If a broker stops before the index replacement, a stale index entry is validated against the rewritten log and falls back to a scan. Startup removes abandoned `.compacting` files and stale sidecars. Rewritten segments retain their original modification time so time-based retention is not postponed.

Active readers cause the maintenance pass to skip rather than replacing a mapped file.

## Distributed Replica Catch-up

A replica outside ISR catches up only to the Raft-authoritative committed HWM.
The leader returns a fenced logical range containing topic, partition, target
broker, leader epoch, lifecycle epoch, start offset, end offset, committed HWM,
and whether the range is compacted. A compacted range may contain zero or more
physical records while still advancing across a non-empty logical interval.

The follower validates every identity and boundary before mutation. It stages
the compacted log and sparse index, fsyncs them, records their sizes and SHA-256
checksums in a durable pending manifest, installs the marker and empty tail,
and removes the pending manifest last. Restart completes or validates an
interrupted installation from that manifest. HWM advances only after the
entire committed range is present; ISR admission remains a separate,
leader/lifecycle/HWM-fenced catch-up proof.

## Configuration

| Setting | Default | Meaning |
|---|---:|---|
| `log_cleanup_policy` | `delete` | Broker default: `delete`, `compact`, or `delete,compact`. |
| `log_compaction_check_interval_ms` | 300000 | Interval between compaction passes. |
| `log_min_cleanable_dirty_ratio` | 0.5 | Minimum removable closed-segment bytes divided by total closed-segment bytes. |
| `log_retention_check_interval_ms` | 300000 | Interval between delete-retention passes. |

A topic overrides the broker cleanup default with `CREATE ... cleanup_policy=<policy>`. Repeating `CREATE` patches only supplied fields and advances the topic definition revision only when the effective definition changes. In distributed mode do not enable compaction until every broker is on a lifecycle-protocol-v2-capable build.

The Go SDK exposes `Producer.CreateTopicWithOptions` and `TopicOptions.CleanupPolicy`. `CreateTopic(topic, partitions)` is the minimal convenience form and inherits the broker default.

## Operational Guidance

Keyed state topics are the intended use case. Choose a segment roll size/time that produces closed segments frequently enough to clean, then observe disk usage and cleaner duration before lowering the dirty ratio.

Monitor `cursus_broker_log_compaction_runs_total{result,reason}`. A skipped
pass can be normal—for example `no_closed_segments`, `dirty_ratio`, or
`active_readers`. Repeated distributed gate reasons such as
`mixed_broker_protocol`, `replica_not_active`,
`replica_not_caught_up`, `topic_lifecycle_mismatch`, or
`partition_metadata_unavailable` indicate that compaction is intentionally
paused until cluster state converges. An `error` result requires storage and
recovery investigation.

Do not enable compaction for audit/event history that must retain every mutation. Use a separate compacted state topic and an uncompacted event-sourcing topic when both current state and full history are required.

Backups and restores must preserve each compacted `.log` file together with its matching `.log.compacted-<size>` sidecar and `.index` file. A missing or invalid sidecar is not reconstructed from offset holes: startup and reads fail closed instead of treating an unmarked gap as valid compaction. Restore the files from the same backup generation before opening the broker.

Tombstone grace periods and per-topic dirty-ratio overrides are not part of the current contract. An empty payload is retained as the latest value and is not a time-based tombstone.
