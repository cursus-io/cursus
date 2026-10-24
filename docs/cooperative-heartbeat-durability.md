# Cooperative heartbeat liveness during durable commits

## Failure and reproduction

The cooperative broker/Go-client job on main commit `9139bf3` failed during its
second rolling restart with `cooperative_fenced`, after heartbeat request
timeouts. The coordinator held its global read lock and group offset lock while
waiting for an offset append. Both the controller's offset snapshot export and
the membership handler's exclusive locks could block a live heartbeat. If an
expiration scan acquired the lifecycle lock first, it removed the session before
the queued heartbeat could refresh it.

`TestCooperativeHeartbeatRefreshesBeforeQueuedExpiration` reproduces that ordering
on the unmodified baseline: pause the durable writer, queue expiration, then send
a valid heartbeat. Releasing the writer produces `cooperative_fenced`. The Wire
regression also fails on that baseline with a heartbeat read timeout. This
establishes the causal lock path; the CI logs alone do not capture lock ordering.

## Kafka comparison and Cursus constraints

Kafka 4.1.0's [GroupMetadataManager](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java)
validates heartbeat membership and refreshes a session timer. Its unchanged
classic heartbeat path can return without records. Cursus adopts that separation
of volatile liveness from durable membership changes.

Kafka's [OffsetMetadataManager](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java)
validates commit membership and generates offset records for its coordinator
runtime to write and replay. Cursus keeps its synchronous durable writer and
existing snapshot records instead of introducing that runtime architecture.
Ownership validation and successful durable append remain prerequisites for a
commit ACK.

## Implemented locking and safety

The controller validates the single-topic subscription without exporting offsets.
An unchanged heartbeat updates only volatile clocks under the short state lock.
It must match registration, member/session and authoritative topology, have no
revocation acknowledgements, and require no assignment change even with all
reservation barriers removed. Pending lifecycle publication uses the existing
transition path. Invalid sessions remain fenced.

Cooperative commits hold shared state-mutation exclusion across validation and
durable append, but release the short state read lock before I/O. A second commit
waiting for the group offset lock also releases the state read lock. Normal
membership mutation and publication acquire exclusive exclusion before state;
expiration rechecks the clocks only after acquiring both. Retained grants remain
committable while a durable handoff is pending; moving grants remain blocked.
Unrelated groups can still append concurrently.

FSM restore intentionally bypasses the append exclusion: waiting for a writer
that itself needs Raft could deadlock restore. Commit completion therefore checks
the import generation, installed group identity and recovery readiness before
ACK. A completed append to a detached group returns recovery-unavailable, leaving
the newly installed offset state intact. The existing recovery authority,
provenance, ownership-route and durable-read barriers remain in force.

Increasing timeouts would only postpone false expiration. Releasing every fence
would allow ownership changes during a commit. Holding the lifecycle mutex over
the append would block retained grants during a pending handoff. None of those
alternatives is used.

## Cost and validation contract

There is no new checkpoint, record version, scan or persisted heartbeat field.
An unchanged heartbeat adds zero recovery records and zero recovery bytes; offset
snapshots retain their existing encoding and size. Assignment comparison still
performs work proportional to group assignment state. The extra commit fence
changes scheduling, not recovery decoding or replay work. General admin snapshots
and reservation operations retain their existing locking; this change does not
claim a bound on all coordinator operation latency.

Regression and fault checks cover:

- A queued expiration cannot fence a valid heartbeat behind a paused append.
- Real Wire heartbeats continue beyond the fixture's unchanged session timeout;
  offsets remain at the last acknowledged value and commit ACK waits for append.
- A failed append never advances the visible offset; stale grants still fail.
- FSM restore completes during a paused append and fences detached completion.
- Retained grants, moving grants, revocation ACKs and reservation barriers keep
  their existing regression coverage.
- Existing recovery-work tests count encoded records/bytes and prove unchanged
  heartbeats add no durable work; the broker/Go-client fault test retains its
  rolling restarts, partition faults, original deadlines and convergence checks.

All deterministic fixtures use synthetic data. No original backup payload belongs
in this document, Git, CI or PR artifacts.
