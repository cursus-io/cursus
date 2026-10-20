# Topic cleanup and consumer-group scope

## Design comparison and regression plan

This change is a safety prerequisite for committed consumer-group lifecycle
reconciliation (issue #272), also relevant to the truncate contract in #164.
It does not implement follower reconciliation or declare either issue complete.

Primary sources checked before implementation:

- [Accepted KIP-496](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/122915859/KIP-496+Administrative+API+to+delete+consumer+offsets)
  defines partition-scoped offset deletion and protects subscribed topics.
- [Kafka 3.8 Admin API](https://kafka.apache.org/38/javadoc/org/apache/kafka/clients/admin/Admin.html)
  documents `deleteConsumerGroupOffsets` as a partition-level operation.
- [Kafka 3.9.0 GroupMetadataManager](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala),
  `cleanupGroupMetadata`, writes tombstones for selected offsets and only marks
  an empty group dead when it has no remaining offsets.

Adopt Kafka's distinction between removing topic-specific offsets and deleting
an entire group. Cursus currently cleans topic dependencies through a durable
**whole-group** lifecycle tombstone, not partition-scoped tombstones. Reusing
that operation for a multi-topic group can erase unrelated offsets. A missing
offset for the target topic is also not proof that a group does not subscribe
to it.

The implemented guard discovers references from subscriptions, retained
offsets and reservations. Before topic intent is submitted, and again before
group cleanup, it rejects active members, pending offset reservations,
and groups whose state is not confined to the target topic. Pattern-backed
groups remain protected even if their current expansion contains only
one topic. Single-topic inactive groups keep their durable deletion path.
Cleanup serializes its preflight and deletion with group lifecycle changes
and revalidates scope under the group lock before writing each tombstone.

Safety improves by rejecting destructive operations that cannot preserve
unrelated state. Availability is deliberately reduced for shared or pattern
subscriptions until topic-scoped durable reconciliation exists. Previously
committed intent may remain `cleanup_pending`; a retry is not permission to
erase the protected group. This introduces no metadata format or protocol
capability change.

The added checks inspect in-memory group state; they do not add log scans,
recovery reads, or metadata writes on rejection. Their work is proportional to
the groups and subscription/offset/reservation entries inspected, rather than
log length. Existing distributed admission still reloads consumer metadata;
this change does not claim to bound that replay's records or bytes. Future
reconciliation must measure scanned records, payload bytes, persisted progress,
retry work and transfer bytes, not merely request counts.

Regression and fault-recovery checks defined before implementation:

1. An active multi-topic subscription with no committed offsets blocks both
   DELETE and TRUNCATE before their topic definition or revision changes.
2. An inactive multi-topic group with offsets for two topics survives cleanup
   attempts with both offsets intact; no tombstone is emitted for any group.
3. Pattern subscriptions, legacy wildcard registrations, and retained offsets
   outside the target remain protected.
4. A transaction reservation still blocks cleanup after its member leaves.
   A later blocking group must not cause an earlier group to be deleted.
5. Single-topic inactive cleanup remains idempotent and durable.
6. Snapshot restore and durable metadata replay retain the protection.
7. Already-committed FSM truncate/delete retries preserve shared group state
   and report pending materialization instead of deleting the group.
8. Controller tests cover standalone and distributed preflight; race tests
   cover coordinator/controller/FSM packages.

The original three-broker stale-membership convergence, committed-record
validation, generation races, compaction, bounded retry progress and recovery
observability remain follow-up work under #272.
