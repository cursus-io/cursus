# Truncate contract and recovery verification

## Baseline and scope

The audit starts at main `6b92842311150c5a8e5a991c3fc8beed5fa19d82`.
Shared-group protection (#275) and committed follower cleanup (#277) are
merged. Issue #272 remains open: its three-broker lost-reply, transient-read
and delayed-registration fault cases are not all covered. Neither those PRs
nor this audit alone establish completion of #164.

## Primary-source comparison (before implementation)

- [Kafka Admin DeleteRecords](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/Admin.html)
  deletes records below a partition offset.
- [Accepted KIP-107](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/67636826/KIP-107+Add+deleteRecordsBefore+API+in+AdminClient)
  defines leader routing, log-start checkpoints and replica low watermarks.
- [Adopted KIP-98](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
  separates producer identity/sequence fencing from transaction decisions.
- [Adopted KIP-496](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/122915859/KIP-496+Administrative+API+to+delete+consumer+offsets)
  defines scoped offset deletion with subscription protection.
- Version-pinned [Kafka 3.9.0 Partition.scala](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/cluster/Partition.scala),
  `deleteRecordsOnLeader`, advances the start offset and returns the low watermark.
- Version-pinned [Kafka 3.9.0 ProducerStateManager.java](https://github.com/apache/kafka/blob/3.9.0/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateManager.java)
  has distinct `onLogStartOffsetIncremented` and `truncateFullyAndStartAt`
  paths. Prefix deletion is not a full producer/transaction reset.

Adopt durable intent, authoritative routing, generation fences and protection
of subscribed groups. Cursus differs: it preserves the definition, advances
revision/epoch, resets every partition to LEO/HWM zero, prunes target-topic
transaction references, removes eligible group offsets and producer sequences,
and resets event indexes/snapshots. Kafka DeleteRecords does not provide this
combined reset contract. Cursus whole-group tombstones still require rejection
of multi-topic/pattern groups and pending reservations; unrelated state survives.

Safety requires the local epoch publication marker to follow **all** successful
storage and dependency cleanup. An error leaves the topic fenced and readiness
unavailable; it must not merely label an already-published topic pending.
This sacrifices availability until durable cleanup can succeed. Retrying a
pending reset can repeat storage work and dependency replay. Count bytes and
records/pages processed, not just requests; snapshot file size is storage size,
not measured physical restore I/O. Never subtract process counters across restart.

## Checks defined before implementation

1. Fail group tombstone and transaction-journal rewrite after the manifest
   transition. Require pending readiness, no public topic access, and no synced
   completion marker. A retry must complete cleanup before publishing.
2. Crash before/after manifest temp sync, rename and directory sync; storage
   removal/recreation and watermark persistence; group tombstones; transaction
   rewrite; marker temp sync, rename and directory sync. Restart must expose
   either the untouched pre-commit generation or a fenced/new generation, never
   old records, offsets, sequences, transaction references, indexes or snapshots
   after a completed reset.
3. A committed result receipt must survive standalone restart, Raft replay and
   snapshot restore. An exact retry must preserve fresh post-reset writes and
   new memberships; unrelated updates or an unproven old revision must not be
   mistaken for that receipt. Same-name recreation must not accept an old
   instance's retry. Legacy records without proof must fail closed.
4. Three brokers: follower routing, concurrent traffic, leader loss, lost reply,
   replay and actual snapshot restoration. Verify committed intent separately
   from each broker's materialization and capture recovery records/bytes/pages.
5. Preserve admin-only/internal-topic guards, shared/pattern subscription and
   reservation protection, terminal transaction state for unrelated topics and
   event/producer retry identity. Desired-list omission performs no mutation.

## Baseline observations

The focused topic/controller/FSM/admin SDK tests pass outside the Windows
sandbox. The sandbox run failed on file replacement and loopback socket access;
those failures are not crash-recovery evidence. Existing tests explicitly expect
conflict after a successful old-revision retry; the admin SDK deliberately does
not replay an ambiguous truncate. This differs from #164's idempotent retry
requirement. The approved follow-up design uses durable result evidence and a
topic-instance boundary, with an explicit protocol/storage compatibility gate.
That design is not implemented by the publication-fence fix.

The group-writer regression fails on the baseline: cleanup returns an error
but `IsTruncationPending` is false. The publication fix retains the fence on
storage, group or journal errors, including startup recovery. Group-write and
journal-replacement failure checks now pass, including recovery retry and
reloading the rewritten transaction journal without the target references.
Focused controller/topic/coordinator/FSM/SDK lifecycle tests pass on Windows
outside the sandbox. These are not the complete process-crash or three-broker
fault matrix described above, and introduce no storage or wire format change.
