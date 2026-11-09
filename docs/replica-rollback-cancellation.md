# Replica rollback cancellation

## Failure and evidence

The November 9 Docker compound-fault run completed 78 rounds. In round 79,
broker 1 stayed outside ISR after network isolation and quorum loss recovered.
Its materializer repeatedly reported `replica rollback requires partition
recovery` followed by `context canceled`. Broker 2 remained leader at epoch 55;
the committed watermark advanced to 2461 while broker 1 had a local end of 2457.
The campaign failed waiting for all three replicas, not on a history mismatch.

`repairReplicaTail` treats every failure after successful physical truncation as
an uncertain repair. It then passes the caller's cancellable recovery-turn
context into generation capture, retained-log reconstruction and publication.
An owner cancellation in this interval permanently fences the partition even
when the disk operation succeeded. The log establishes this failure class;
it does not identify the exact instruction at which cancellation arrived.

## Kafka comparison and design

Sources checked before implementation:

- [Accepted KIP-101](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/67634337/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation)
  requires leader-epoch evidence for divergence truncation rather than a stale
  local high watermark.
- [Kafka 4.1.0 ReplicaFetcherThread](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala)
  uses a non-interruptible fetcher and synchronous local truncation. This is a
  useful model for completing an admitted storage transition; it is not a claim
  that Kafka and Go context cancellation are identical.

Implemented change: keep request cancellation through validation, lock acquisition
and the checked disk truncation. Once truncation succeeds, finish the already
admitted local reconstruction and checkpoint publication without caller
cancellation. Preserve context values, including recovery accounting. Keep all
existing lifecycle, metadata, partition, generation and checkpoint fences.

Cursus differs from Kafka by combining a separate Raft metadata authority with
push replication, indexed prefix proofs, and transaction/producer checkpoints.
Those checks remain necessary. This change neither elects a leader nor grants
ISR membership; subsequent catch-up still needs fresh authority and proof.
Real disk, generation, checkpoint and partition-close failures remain fenced.

The availability tradeoff is that an admitted repair finishes before the owner
can release its worker or shutdown join. It may exceed the recovery-turn
deadline. It performs one truncation and one scan of the retained prefix, with
O(retained bytes) local read work, existing checkpoint writes and zero extra
network transfer bytes. It introduces no retry scan or repeated transfer.
Large-prefix latency and operating-system I/O stalls are not solved here.

## Regression and fault checks defined before implementation

1. Cancel immediately after successful truncation, after retained-state scan,
   and during checkpoint persistence. Check both canonical rollback and
   speculative-tail repair: no permanent fence, correct HWM, producer and
   transaction state, durable restart and subsequent replica append.
2. Count truncations, scans, retained records and retained payload bytes in the
   deterministic fixture. Confirm the caller context values survive completion.
3. Retain pre-mutation cancellation, stale-generation, partial disk failure,
   independent reconciliation failure and checkpoint failure regressions.
4. Run topic, disk and replication/controller checks in Docker, then the short
   Docker compound-fault campaign with acknowledged-history validation before
   a fresh 24-hour campaign. Previous elapsed time does not count toward it.

The six deterministic cancellation cases failed in Docker before the change
with the same permanent recovery error. After the change, the rollback and
tail-repair suites passed ten repetitions, including checkpoint, partial-disk,
generation and close failure checks. Each cancellation case verifies one
truncation, one retained-state scan, one retained record and eight retained
payload bytes, then reopens the partition and resumes replication. Payload
bytes exclude framing and checkpoint I/O; the existing recovery-work metrics
measure physical reads separately. This regression result is not a completed
24-hour fault campaign.
