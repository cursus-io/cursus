# Catch-up for a follower that remains in ISR

## Observed failure

The November 10 Docker campaign verified nine rounds before an interrupted
catch-up in round ten left broker 3 at local LEO/HWM 340. Raft metadata retained
all three ISR members and committed HWM 341. For over a minute, materialization
reported that the local replica had not reached the committed watermark.
Readback correctly refused this replica as a source. This is distinct from the
earlier canceled-rollback fence.

The request builder intentionally supports a lagging ISR member through legacy
incremental catch-up. The receiver's recovery fence nevertheless rejects every
ISR member. A quick restart can therefore repeatedly fetch the missing suffix
without being permitted to apply it.

## Kafka comparison and implemented boundary

Compared before implementation with the official
[Kafka replication design](https://kafka.apache.org/40/design/design/) and
[Kafka 4.1.0 ReplicaFetcherThread.processPartitionData](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala).
Kafka followers continue fetching and appending while ISR membership is managed
separately. Adopt this distinction between membership and local replication
progress. Cursus keeps its separate Raft committed watermark, push replication,
authenticated catch-up, and receiver content-generation witness.

Permit only legacy, non-compacted, non-truncating incremental catch-up while the
receiver remains in ISR. Keep indexed rollback and retained bootstrap excluded
until the receiver is outside ISR. Require the original local request authority
and unchanged membership, partition, topic definition and broker incarnations
under the recovery fence; rejoining ISR during a fetch must still reject.
Preserve source membership, leader/lifecycle,
committed boundary, digest, local start-offset and witness checks. Do not relax
the source's local-HWM guard or add a new ISR admission path.

This restores availability for an existing member with a committed suffix gap
without copying its prefix again. Recovery transfers the missing records and
the normal proof/batch envelope; local work includes the existing last-record
challenge, append, checkpoint and verification. It introduces no truncation,
bootstrap, full retained-prefix scan, or metadata membership write.

## Regression contract defined before implementation

- Exercise the actual request builder, source fetch and receiver apply with a
  one-record committed gap and unchanged full ISR, for legacy and capable peers.
  Assert readiness, stored payloads, unchanged membership, one transferred
  record and measured serialized batch bytes.
- Confirm stale local content, leader/lifecycle changes, unavailable sources,
  indexed rollback and bootstrap still reject under their existing fences.
- Run Docker regression tests and a short compound campaign on both recovery
  fixes together before restarting a fresh 24-hour run.

The original fence fails both deterministic gap cases before the fix. After the
fix, each transfers one eight-byte payload in a 747-byte serialized batch, with
no prefix replacement. The ISR and receiver-witness tests passed ten repetitions
in Docker (21.245 seconds), and the full replication FSM package passed (19.746
seconds). Compound execution is a separate gate before a fresh 24-hour run.

## Follow-up: automatic quarantine for marked or divergent history

Before implementation, compare Kafka 4.1.0
[Partition.maybeShrinkIsr](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/cluster/Partition.scala)
and `ReplicaFetcherThread.processPartitionData`. Kafka treats stalled followers
as candidates for ISR shrink and submits the membership decision outside its
partition lock. Cursus must use its existing Raft `ISR_QUARANTINE` command and
exact leader/lifecycle/HWM/membership fences, rather than copying Kafka's lag
timer or permitting marked history through legacy append.

Implemented behavior: a validated legacy response requiring a compacted range or
truncation may request quarantine of its own follower. Require the original
local request, unchanged authority and local content witness, active capable
participants, and a non-leader receiver. Release all metadata/partition gates
before submitting Raft. Never apply that old batch after the transition: retry
with a fresh retained/indexed request. Failures and uncertain submission do not
authorize mutation; only locally applied metadata enables subsequent recovery.

Availability improves for idle compacted followers without waiting for a new
client write. Quarantine can temporarily reduce ISR and reject writes below
minISR, as the existing command requires. The bounded first response is discarded;
the next transfer reuses a matching retained prefix and copies only its suffix.
Do not claim zero duplicate bytes or a universal bootstrap-free repair.

Regression contract: run actual catch-up scheduler passes over real marked disk
storage with no client writes; assert automatic quarantine, fresh protocol,
payload/HWM restoration and witness-based ISR reentry. Reject corrupt responses,
stale leader/lifecycle/membership/local generation, missing local authority,
unsupported capabilities and leader self-quarantine. Measure response bytes and
retained payload work. Existing ordinary ISR catch-up must remain unchanged.

The deterministic marked-log case completes in two scheduler passes with no new
producer writes: one quarantining response and one retained-prefix suffix response.
They serialize to 1,818 bytes total, including a 1,050-byte recovery response
carrying one seven-byte payload. Both replicas re-admit the follower through its
actual recovery witness. Stale authority, unsupported participants, failed
submission and divergent-history handoff checks passed three repetitions.
These are in-process scheduler/FSM tests over real disk, not a completed network
fault campaign. Quarantine requires an obtainable, validated source response;
a legacy request that fails before fetch (for example, a missing previous record)
still needs its separate recovery path. The running 24-hour campaign uses the
previous frozen revisions and does not certify this follow-up.
