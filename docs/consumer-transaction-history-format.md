# Consumer transaction history format and replay index

This foundation introduces a fenced writer format and transaction-aware derived
replay indexes. It does not authorize deleting transaction records. The existing
ordinary history compactor continues to retain every intent and control frame.

## Comparison and regression contract

The comparison uses accepted [KIP-848](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848+The+Next+Generation+of+the+Consumer+Rebalance+Protocol),
[Kafka compaction design](https://kafka.apache.org/41/design/design/), and Kafka
4.1.0 commit `13f70256db3c994c590e5d262a7cc50b9e973204`:
[OffsetMetadataManager](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java)
and [LogCleaner](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaner.java).
Kafka's implementation replays retained metadata, protects producer continuity,
and retains controls until their associated batches can be removed. KIP-848
changes the consumer protocol; it does not provide a persistent Cursus snapshot.

Cursus adopts explicit transaction evidence and version fencing. It retains its
own registration epochs, reservations, materialized offset revisions and physical
source provenance. A mutable transaction-manager snapshot is not committed-log
proof. Tests must cover gate activation/restore, old brokers and formats,
coordinator movement, producer sequence fencing, pending-to-visible decisions
at an unchanged committed cut, incompatible suffixes, source replacement and
full replay fallback. No test may substitute cached decisions for current
transaction visibility or reset unrelated membership clocks.

## Durable activation and compatibility

Protocol 12 adds `CONSUMER_TRANSACTION_HISTORY_ACTIVATE`. It requires the
ordinary history gate and every registered broker (including offline entries)
and voter to support protocol 12, bootstrap protocol 2 and durable incarnations.
Only the leader activates it when history maintenance is configured. Registration
rejects older protocols after activation.

Activation is persisted in Raft snapshot 14. Restore rejects inconsistent flags,
old snapshot versions carrying the flag, or incompatible registered brokers.
Unactivated snapshots retain version 11/12/13. Local Raft recovery marker 15 is
published atomically and synced after replay of supported marker 9 through 14
volumes, before advertising capability. Older binaries fail closed on marker 15.
In-place downgrade is unsupported; restore a complete pre-upgrade backup into
isolated volumes. Disabling maintenance does not undo the durable gate.

The cooperative runtime retains its independent protocol-11 gate and
snapshot-13 format. Protocol 11 alone cannot activate transaction history.
Either activation order preserves both flags; when transaction history is
active, snapshot 14 carries both contracts. Inactive transaction history may
still use snapshot 13 for an activated cooperative runtime.

Before activation, writers emit metadata v3 and existing control identities.
After activation, processing transactions emit v7 intents and controls with
producer `txn-history:<transactional ID>:<decision>`. Their sequence is the
positive coordinator epoch, preserving a stable producer identity across owner
movement. Sparse sequences are permitted only for validated internal consumer
controls; ordinary producer sequencing is unchanged. Producer epoch, current
transaction route, control bytes and decision remain validated. New readers
accept both formats. Legacy history remains authoritative and retained.

## Derived replay index v2

The authoritative state remains committed log frames plus the Raft compatibility
baseline. The index is disposable. It carries selected ordinary records, source
provenance, raw v7 intents and controls, exact partition cuts and durable source
identities. Capture, fsync and atomic publication use the existing
[recovery index protocol](consumer-recovery-index.md).

On reuse, current fixed read views resolve each cached intent's visibility.
The cache never freezes a pending transaction's decision. The production reader
uses the partition's marker/decision view without rereading hidden disk frames;
adapters without that capability use committed reads. Identity validation before
and after replay fences replacement. Registration/tombstone, deleted-group epoch
fences, offsets/revisions, reservations and decisions remain complete snapshots.

Ordinary v1 indexes remain readable. Missing, corrupt, unsupported or oversized
indexes, incompatible legacy transaction ranges, retention, compaction,
bootstrap/restore or topic identity replacement trigger full replay. The index
limit remains 32 MiB. Cache write and certification costs are observable and
must be included in recovery comparisons. A hit can reduce replay work while
authoritative history still grows; this foundation alone does not bound history.

`cursus_consumer_metadata_index_hit` reports the last recovery's actual result.
`cursus_consumer_metadata_index_work` separates index read/write bytes, source
records/payload bytes and transaction checks. Source payload bytes are logical,
not physical disk I/O. Existing recovery readiness and corruption metrics remain
applicable. Warm index reuse is not a cold recovery result.
