# Committed group cleanup on replicas

## Design and checks established before implementation

This extends the single-topic safety contract in `topic-cleanup-safety.md`.
Topic intent and local materialization remain separate: a committed truncate
or delete can be pending while consumer metadata catches up.

Primary references:

- [Accepted KIP-496](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/122915859/KIP-496+Administrative+API+to+delete+consumer+offsets)
  keeps administrative deletion under coordinator authority and protects
  subscriptions.
- [Kafka Admin API](https://kafka.apache.org/38/javadoc/org/apache/kafka/clients/admin/Admin.html)
  distinguishes group deletion from partition-scoped offset deletion.
- [Kafka 3.9.0 GroupMetadataManager](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala)
  tracks owned/loading partitions and partition epochs, replays keyed records
  and tombstones, and sizes loading buffers. Its `minOneMessage` path can exceed
  the configured buffer to make progress on a large record.

Adopt durable deletion, ownership fencing, and replay before accepting a
derived cache as authoritative. Cursus differs: metadata Raft drives topic
materialization on every broker, including brokers with stale group caches.
Its deletion is a versioned JSON lifecycle record with an increasing epoch,
not Kafka's null-valued tombstone. Registration and deletion share a compacted
key. Absence of a retained registration therefore never proves deletion.
The existing multi-topic, pattern and reservation protections remain.

The implementation reads only the committed, flushed metadata prefix
through a bounded local view. Each retry scans at most 128 records, 1 MiB
of serialized record bytes and 128 physical segment pages. Length headers and
file metadata are additional bounded work. The sequential reader carries the
segment and byte position between pages; it does not consult sparse indexes or
fall back to rescanning the log prefix. Counts include every decoded record,
even records skipped before the requested offset or beyond a compacted gap.
Progress survives ordinary append and transient
read failures; partition replacement, rollback, snapshot recovery, retained
start changes and local group replacement invalidate it. Only tracked groups'
latest lifecycle evidence is retained. A fixed maximum number of targets and
pending topics bounds cache memory (64 groups per scan, 64 cached topics).
Unfinished scans are not evicted to admit more topics; completed scans can be
evicted and rebuilt if capacity is needed. Progress is derived and is rebuilt after
restart; durable epoch fences remain in normal coordinator snapshots.

A follower applies an existing valid deletion only when its epoch exceeds the
local registration and every observed live record. It neither expires members
nor emits a deletion to bypass stale membership. Only the recovered owning
coordinator may create a new tombstone, with ownership rechecked at append.
Applying a deletion removes just that group and its ownership clock. Other
groups, offsets, reservation decisions, event streams and dedupe indexes are
not reloaded or replaced.

Safety takes precedence over availability when proof is absent, malformed,
conflicting, oversized or not yet replicated. Such work stays pending with
the topic, group/epoch, cursor, committed boundary, scanned records/bytes and
retry reason visible. Incremental retries cost only new pages; invalidation
costs another bounded replay of the retained prefix. These are local reads,
not additional replication transfers.

Unlike Kafka's oversized-first-record loading path, this narrowly scoped
cleanup reader refuses a record larger than its byte budget and reports
pending work. It never silently skips such a record. Normal startup/ownership
recovery remains the existing full coordinator recovery path; this change
does not claim to bound it or the distributed administrative admission reload.
Multi-topic, pattern and unresolved reservation cleanup remains intentionally
blocked by the existing safety contract.

Regression/fault checks defined before implementation:

1. Three brokers recover a live group, its owner commits leave/deletion, and
   every replica completes truncate/delete despite stale local membership.
2. No record, uncommitted deletion, wrong key/topic/type, malformed record,
   equal/older epoch, conflicting record or newer registration permits deletion.
3. Re-registration and snapshot replacement during replay invalidate old proof.
4. Read failures and lost acknowledgement converge without new follower writes.
5. Appended pages resume at the saved cursor; bounded tests measure records and
   bytes, including large records and unrelated metadata.
6. Rollback, partition replacement, compacted gaps and snapshot recovery cannot
   reuse invalid proof. Export/import retains the applied deletion epoch.
7. Unrelated heartbeat times, membership, offsets and reservations are unchanged.
8. Active owners still refuse deletion; stale ownership cannot forward a newly
   invented tombstone to a different coordinator.

## Implementation and validation

The FSM schedules scoped reconciliation after committed topic intent.
Raft Apply and snapshot/replay completion only perform a nonblocking group
check; they never wait for a metadata writer or originate a new tombstone.
The background retry originates writes without holding the materialization
mutex, so the same Raft loop can process the required partition commit.
It checks that the topic intent is still current after capturing group targets
under the lifecycle lock; a snapshot replacement also invalidates the target
pointer used by the owning writer.

The first
read view builds proof without holding the global coordinator lock. Application
takes that lock before pinning a second view, following the existing
coordinator-to-topic-manager lock order, and verifies that the captured log
boundaries have not changed. A newer commit is read on the next retry.

`CLUSTER_STATUS` pending topic entries include `consumer_cleanup` with group
epoch, cursor, end, records, bytes, reads and reason. Truncation entries also
include topic revision and lifecycle epoch. Existing materialization readiness
and pending/attempt metrics continue to apply. Aggregate counters are
`consumer_cleanup_replay_{records,bytes,reads,invalidations}_total`; they do not
add per-group labels or expose payloads.

The same three-broker restart/leave/truncate test failed against pre-change main
`792f5b7`: follower logs kept reporting one active member after the owner's
leave, leaving topic and replica materialization pending. The implementation
completed that sequence, re-created the group, committed a new offset, and
deleted the topic while preserving an unrelated active group's heartbeat and
offset. The test is included as a dedicated ordinary E2E workflow job.

Unit and disk-backed FSM checks cover invalid and superseded evidence,
read/acknowledgement failure, snapshot replacement, rollback invalidation,
compacted gaps, truncate/delete, and a missing sparse index. The physical replay
test verifies that 200 records are decoded exactly once across small pages and
that the byte total matches the log size excluding four-byte record headers.
