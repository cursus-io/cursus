# Log Compaction

Cursus can compact keyed topic records on a standalone broker. Compaction retains the latest ordinary record for each key while preserving logical offsets and broker recovery metadata.

## Scope

Supported cleanup policies are:

| Policy | Behavior |
|---|---|
| `delete` | Delete complete closed segments by retention time or retained bytes. |
| `compact` | Rewrite eligible closed segments and remove superseded keyed records. |
| `delete,compact` | Run both maintenance policies on their independent intervals. |

Compaction is currently standalone-only. `CREATE` rejects a compact policy when distribution is enabled because follower catch-up does not yet install rewritten segment generations. Event-sourcing topics also reject compaction because aggregate replay requires complete history.

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

## Configuration

| Setting | Default | Meaning |
|---|---:|---|
| `log_cleanup_policy` | `delete` | Broker default: `delete`, `compact`, or `delete,compact`. |
| `log_compaction_check_interval_ms` | 300000 | Interval between compaction passes. |
| `log_min_cleanable_dirty_ratio` | 0.5 | Minimum removable closed-segment bytes divided by total closed-segment bytes. |
| `log_retention_check_interval_ms` | 300000 | Interval between delete-retention passes. |

A topic overrides the broker cleanup default with `CREATE ... cleanup_policy=<policy>`. Repeating `CREATE` with the same partition count updates the topic policy. Provisioning code should issue the same idempotent topic declaration whenever it establishes broker resources.

The Go SDK exposes `Producer.CreateTopicWithOptions` and `TopicOptions.CleanupPolicy`. Existing `CreateTopic(topic, partitions)` remains a compatibility wrapper and inherits the broker default.

## Operational Guidance

Keyed state topics are the intended use case. Choose a segment roll size/time that produces closed segments frequently enough to clean, then observe disk usage and cleaner duration before lowering the dirty ratio.

Do not enable compaction for audit/event history that must retain every mutation. Use a separate compacted state topic and an uncompacted event-sourcing topic when both current state and full history are required.

Backups and restores must preserve each compacted `.log` file together with its matching `.log.compacted-<size>` sidecar and `.index` file. A missing or invalid sidecar is not reconstructed from offset holes: startup and reads fail closed instead of treating an unmarked gap as valid compaction. Restore the files from the same backup generation before opening the broker.

Distributed compacted-segment installation, tombstone grace periods, and per-topic dirty-ratio overrides are not part of the current contract.
