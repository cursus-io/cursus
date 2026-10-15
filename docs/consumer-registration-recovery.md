# Consumer registration activity recovery

## Kafka comparison and decision

Apache Kafka 4.1.0 implements classic-group replay using persisted generation,
members, and state timestamp, retaining `-1` as unknown. Its `onLoaded` path
establishes member sessions separately from durable group history:
[GroupMetadataManager](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L5545).
Election schedules partition loading in
[GroupCoordinatorService](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java#L2020),
and [CoordinatorRuntime](https://github.com/apache/kafka/blob/4.1.0/coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java#L650)
transitions loading to active or failed. These are implemented Kafka behaviors,
not proposals for changing Cursus ownership or session handling.

Cursus adopts recovery from a durable observed timestamp: distributed replay
sets registration-only group activity from the selected registration record.
Lifecycle activity remains authoritative, with the existing last-rebalance and
lifecycle-record timestamp fallbacks. Existing nonzero in-memory activity is
preserved when no matching lifecycle snapshot exists; existing zero activity
does not erase recovered activity. Standalone registration replay keeps its
existing unknown-activity contract.

Cursus differs because live groups in its Raft snapshot format require nonzero
activity. Snapshot creation must validate exported groups using the same
validator as restore, refusing an unrestorable snapshot before publication.
Restore validation remains strict; already persisted invalid snapshots are not
automatically repaired. No wire, authorization, bootstrap, offset, or session
semantics change.

## Safety, availability, and recovery cost

Historical zero registration timestamps remain accepted as unknown during
replay. Recovery does not invent wall-clock activity or change record validation.
Such groups prevent snapshot creation until a real lifecycle transition supplies
activity, or a separately designed migration handles them. This preserves replay
logs instead of allowing Raft to trim them behind an unrestorable snapshot, but
can cause continued log growth and increased restart recovery work. Automatic
repair and migration are outside this change.

The timestamp is already in the selected record: this change adds no recovery
read requests, records scanned, or payload bytes. Materialization adds constant
work per registration; snapshot validation adds work proportional to exported
group state, including member assignments and offsets. No recovery-efficiency
claim relies on request counts alone.

## Regression and fault-recovery checks

- Recover single-topic and subscription registrations through actual FSM
  snapshot, persist, and restore; require exact activity, unchanged epoch and
  partitions, generation zero, and empty members and offsets.
- Count reader calls, records returned, and payload bytes to reject extra scans.
- Check lifecycle activity, last-rebalance fallback, and lifecycle timestamp
  fallback precedence over registration activity.
- Preserve existing nonzero activity and heal existing zero activity; keep the
  standalone unknown-activity regression unchanged.
- Accept historical zero timestamps during replay but reject snapshot creation.
  Reject existing zero-activity snapshots without replacing prior live state.
- Run coordinator and FSM packages, focused race checks, and repository lint.
