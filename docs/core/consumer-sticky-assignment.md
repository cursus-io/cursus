# Sticky consumer assignment

## Scope and Kafka comparison

The live coordinator preserves valid owners while balancing a shared group
subscription. This is the sticky-assignment stage of incremental rebalance work;
the eager generation and SDK worker lifecycle remain unchanged.

[Accepted KIP-429](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/103090108/KIP-429+Kafka+Consumer+Incremental+Rebalance+Protocol)
separates assignment preservation from cooperative transfer. In
[Kafka 4.1.0 CooperativeStickyAssignor](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/CooperativeStickyAssignor.java),
transferring partitions are removed from the recipient's assignment until their
previous owner revokes them. Cursus adopts preservation of valid owners here;
it does not yet implement that transfer protocol.

[Accepted KIP-848](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
provides the coordinator-driven target/current reconciliation model for the
later handoff stage. Its server-side consumer protocol is implemented in Kafka;
that does not imply every optional feature in the KIP is implemented. Cursus
already assigns on the server and keeps its own wire format and durable
consumer-metadata model.

## Assignment and recovery invariants

- Every eligible partition has exactly one owner when members exist. All members
  share the group's subscription, and their loads differ by at most one partition.
- Preserve as many valid previous owners as possible subject to that balance.
  Give remainder slots to members with more existing partitions before assigning
  unowned or excess partitions. Canonical member and partition ordering resolves
  ties without depending on map iteration or input slice order.
- A known member rejoining retains its previous assignment as input, but still
  increments the group generation. A fresh wire JOIN receives a new member ID;
  static membership across application restarts is not added.
- Publish assignments only after the lifecycle snapshot succeeds. Failed writes
  retain the previous generation and assignment. Snapshots and modern log replay
  recover the stored assignment without rerunning an assignor.
- Historical Raft membership commands still use their original range algorithm.
  Changing historical replay would change ownership for subsequent fenced offset
  records and could invalidate an otherwise valid recovery history.

The current metadata format already stores arbitrary valid assignments. Older
brokers can read these records; an older coordinator may return to range assignment
on the next membership change, which still increments and fences the generation.
No feature negotiation or persisted-format change is needed for this stage.

## Safety, availability and work

Generation checks still reject all stale sessions, even on retained partitions.
Go, Java and Python consumers still stop/rejoin through their existing paths.
Reduced assignment movement therefore does not prove a shorter processing gap,
less application replay, or uninterrupted worker execution.

The assignor adds ownership maps and member ordering compared with range slicing.
Its additional memory is proportional to partition and member counts; member
ordering costs O(M log M), and canonical partition sorting costs O(P log P).
The benefit is fewer changed owners, with a higher coordinator computation and
allocation cost. The membership snapshot remains complete, so sticky assignment
alone does not reduce metadata log volume or recovery decoding work.

## Validation

Regression checks are defined independently of the implementation:

- Enumerate ownership histories for up to four members and six partitions.
  Check unique complete coverage, balanced loads, canonical ordering, no-op
  stability and the independent upper bound on retained owners.
- Exercise duplicate/stale partitions, multi-topic identity, empty subscriptions,
  member rejoin, departure and batched expiration.
- Inject a failed lifecycle append; recover from a snapshot and from encoded
  metadata records. Check assignments and stale-generation rejection after each.
  Report both records and encoded payload bytes in the replay fixture.
- Replay historical joins, rejoin, leave, expiration and fenced commits, checking
  the independent historical authority envelope.
- Run the coordinator, controller and Go SDK suites with race detection.
  `BenchmarkStickyAssignmentJoin` compares movement and allocation/work for
  120 and 12,000 partitions when a balanced three-member group gains a fourth.
  This benchmark measures assignment work, not application replay.

Cooperative handoff remains separate work: explicit current/target assignment,
durable revocation and ownership tokens, transaction reservation handling,
capability/metadata upgrade barriers and delta reconciliation in all three SDKs.
That stage must measure processing gaps, replay records/bytes, worker/state
rebuild work and fault recovery before claiming uninterrupted retained ownership.
