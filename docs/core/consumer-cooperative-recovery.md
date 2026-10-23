# Cooperative ownership recovery contract

This layer carries cooperative ownership through consumer metadata records and
coordinator/Raft snapshots. It does not enable cooperative groups or advertise a
wire capability. Recovered cooperative groups reject eager membership and offset
commands until the cooperative protocol supplies session/grant validation.
Previously decided transaction offsets retain their existing recovery path.

## Kafka comparison

[KIP-848](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
and the implemented
[Kafka 4.1.0 CurrentAssignmentBuilder](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/modern/consumer/CurrentAssignmentBuilder.java)
keep current ownership separate from target assignment. Recovery must restore
both, including unresolved revocations, before reconciling ownership.

Cursus retains complete lifecycle snapshots in `__consumer_offsets`, rather
than copying Kafka's record schema. A version-6 lifecycle snapshot contains the
ownership state. Its revision is the cooperative revision and must exceed the
last eager generation. Ordinary version-4 lifecycle records remain unchanged.
Returning to eager operation within the same registration epoch is forbidden;
a future migration procedure must create and fence a new registration epoch.

Equal cooperative revisions with different contents are corruption, even when
physical log order is available. Recovery must not repair them by choosing the
last record. Version-4 historical replay keeps its original compatibility rules.

Raft writes version 12 only when a snapshot carries cooperative ownership;
ordinary snapshots continue to use version 11. Version 12 prevents an old binary
from silently dropping ownership fields. Draining a group alone does not make
binary downgrade safe: retained version-6 records and version-12 snapshots still
require compatible readers.

## Regression and recovery checks

Before enabling runtime use, verify all of the following:

- Full metadata replay and Raft/coordinator snapshot round-trips preserve grant
  epochs and pending revocations without running the assignor.
- Same-revision conflicts, mismatched registration epochs, inconsistent member
  projections, and old-format snapshots carrying new state fail before publish.
- Candidate copies and failed lifecycle writes leave installed ownership intact.
- Admission refresh compares ownership identity; historical compatibility state
  cannot acquire cooperative ownership through a legacy replay path.
- Eager membership, ownership validation and ordinary commits reject cooperative
  state; an already decided transaction may still finish materialization.
- Tests count serialized bytes and recovered grant/revocation entries, including
  repeated recovery, rather than only counting log requests.

This stores a complete ownership image per transition, so replay bytes and work
grow with the number of partitions. Compaction can retain the latest image, but
this layer makes no claim of bounded recovery bytes or uninterrupted processing.

The six-partition recovery fixture reads two metadata records (approximately
2.1 KB), restores six grants and three pending revocations, and serializes a
1,904-byte coordinator snapshot. The two-partition Raft fixture serializes
1,248 bytes and preserves two grants and one pending revocation. Metadata payload
size includes timestamp text and can vary slightly. Neither test measures
application data replay or end-to-end failover latency.
