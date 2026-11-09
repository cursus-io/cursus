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
