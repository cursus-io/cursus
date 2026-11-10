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

## Follow-up: prepare retained reconstruction outside broker-wide gates

Before implementation, compare Kafka 4.1.0
[Partition.truncateTo](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/cluster/Partition.scala)
and `ReplicaFetcherThread`: the truncate boundary is protected by a partition
lock, and admitted disk work remains synchronous. Cursus cannot simply release
its Raft fence halfway through mutation: concurrent metadata application may
wait on the same partition and invert lock order.

The implementation prepares the retained transaction/producer maps
before taking the FSM gates, under the original disk generation and local tail.
Validate the prefix proof there, then revalidate preparation ownership, tail,
attempt and content generation under the existing mutation fence. A stale or
canceled preparation makes no disk change. After successful truncation, install
the prepared maps and checkpoints without a second retained-prefix scan; still
finish admitted work despite cancellation. Confirm exactly one destructive
generation transition, so a rewrite between truncation and publication cannot
certify stale prepared state. Re-evaluate transaction decisions at installation.

Adopt partition-local expensive preparation, while retaining Cursus Raft,
lifecycle, committed-boundary and durable checkpoint rules. This removes the
O(retained bytes) read from broker-wide exclusion. Segment truncation, map
installation and checkpoint I/O still run inside the fence; this is not a claim
that arbitrary disk stalls or all recovery latency have been eliminated.

Regression contract defined before implementation: block real retained scanning
and verify unrelated Raft application completes before releasing the scan;
change leader/local generation during preparation and assert no truncation;
cancel preparation and assert retryability; cancel after admitted truncation and
assert completion; reject foreign/reused preparations; preserve transaction,
producer, restart and checkpoint-failure checks. Count scans and bytes at
1/16/64 MiB and compare unrelated commit delay with the earlier 500ms hold.

The deterministic controller check now lets unrelated metadata application
finish while the retained scan is paused; changing the leader during that scan
rejects the old response before truncation. The 1/16/64 MiB retained-prefix
checks each read exactly once: 64/1,024/4,096 records and
1,048,576/16,777,216/67,108,864 payload bytes. Unrelated partition commits
completed before scan release in 116/94/114 ms; the earlier implementation
blocked them until release (528/544/627 ms with the injected 500 ms hold).
These single-run timings came from a shared, CPU-limited Docker host and are not
latency guarantees or a controlled throughput comparison. The ordering assertion
and counted work are the regression contract; payload bytes exclude framing,
lineage verification and checkpoint writes.

Prepared canonical and speculative repairs retain restart, transaction, producer
and post-truncate cancellation checks. Preparation cancellation is retryable;
changed owner, tail, HWM, proof, consumed preparation and disk generation reject
without truncation. Checkpoint failure and extra destructive work after truncate
remain unavailable. The prior frozen 24-hour campaign continues separately and
does not certify these new changes.

The disk-backed transaction fixture also uses a 100ms background sync interval.
Its previous 1ms interval reproduced a cleanup-test timeout on the unchanged
baseline under a one-CPU Docker limit, with proof readers repeatedly waiting
behind periodic sync. Changing only that fixture interval completed all four
cleanup cases in 1.305 seconds on the same disk and CPU limit. Acknowledged
writes still synchronize explicitly; production defaults are unchanged.
