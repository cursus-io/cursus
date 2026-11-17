# Stream transient offset fence backoff

## Status

Approved for implementation on 2026-11-17. This document describes the
follow-up to the connection-retention change already present on the branch.

## Problem

A stream checks its group coordinator before every read so a durable Saga
offset reservation cannot be bypassed. While a transaction is open,
`GetStableOffset` returns `unstable_offset_commit`. The current branch keeps the
stream connection and cursor, but the normal 100 ms stream poll remains active.
Long reservations therefore replace reconnect churn with ten coordinator
lookups per second per blocked stream. Partition append notifications can cause
additional immediate checks even though appends cannot clear the reservation.

The safety fence is correct and remains mandatory. The problem is the amount of
work performed while waiting for the transaction decision.

## Kafka comparison

Kafka `read_committed` fetches stop at the last stable offset until an open
transaction completes. Kafka also exposes `UNSTABLE_OFFSET_COMMIT` when a
stable offset fetch cannot yet return a final committed position. It classifies
that condition as transient rather than advancing past the uncertain offset.

For repeated transient requests, Kafka's client defaults use a 100 ms initial
retry backoff, exponential growth with base 2, a 1 second maximum, and 20%
jitter. The versioned source used for this comparison is Apache Kafka commit
`811611f74a8e2ac7d8435cf9396ca852f3869f8e`:

- [`ConsumerConfig.java`](https://github.com/apache/kafka/blob/811611f74a8e2ac7d8435cf9396ca852f3869f8e/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java)
  documents that `read_committed` consumers stop at the last stable offset.
- [`CommonClientConfigs.java`](https://github.com/apache/kafka/blob/811611f74a8e2ac7d8435cf9396ca852f3869f8e/clients/src/main/java/org/apache/kafka/clients/CommonClientConfigs.java)
  defines the 100 ms initial retry, 1 second maximum, base-2 growth, and 20%
  jitter.
- [KIP-851](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217389402/KIP-851%2BAdd%2BrequireStable%2Bflag%2Binto%2BListConsumerGroupOffsetsOptions)
  records the accepted stable-offset-fetch contract for pending transactional
  offset commits.

Cursus should adopt Kafka's bounded retry behavior and its rule that an open
transaction withholds progress. Cursus cannot adopt Kafka's exact fetch path:
the input partition leader checks a separately routed group coordinator, while
Kafka's log fetch response carries the last stable offset. A zero-poll push
design would therefore require a new cross-broker reservation-watch protocol,
including coordinator movement and lost-notification recovery. That protocol
is not part of this change.

## Chosen design

`StreamConnection` will have two scheduling modes.

1. In the normal mode, existing partition notifications and the 100 ms catch-up
   poll retain their current behavior.
2. On the exact `unstable_offset_commit` error, the stream enters a transient
   blocked mode without changing its connection or delivered offset.
3. Fence checks use exponential backoff: 100 ms, 200 ms, 400 ms, 800 ms, then a
   1 second cap. Each delay receives independent jitter in the range -20% to
   +20%.
4. Partition append notifications update the observed message generation while
   blocked but do not call the coordinator. An append cannot resolve a group
   offset reservation.
5. Keepalive writes remain scheduled independently and never trigger a stable
   offset lookup.
6. The first successful stable-offset check exits blocked mode, resets the
   backoff, and reads from the same delivered cursor. A committed transaction
   may move the stable offset forward; an abort does not invent or increment an
   offset.
7. Stream stop, replacement, timeout, or connection failure cancels future
   checks by ending the existing stream goroutine. No new goroutine is created
   per blocked stream or retry.
8. Every other protocol error, including coordinator movement and generation
   mismatch, keeps its existing close-and-reconnect handling.

The random jitter source will be injectable at the stream unit-test boundary.
Production uses the process-safe random source. Backoff state remains owned by
the stream goroutine; scheduler-visible deadlines remain protected by the
existing stream mutex.

## Observability

The existing `cursus_broker_stream_transient_offset_fences_total` remains an
episode counter and increments once when a stream first becomes blocked.

The change adds:

- `cursus_broker_stream_transient_offset_fence_rechecks_total`, incremented for
  each coordinator recheck after the initial fence response;
- `cursus_broker_stream_transient_offset_fence_duration_seconds`, observed once
  when a blocked episode clears or the stream stops.

These metrics distinguish frequent short Saga fences from a small number of
long-lived reservations without high-cardinality group, topic, transaction, or
partition labels.

## Safety, availability, and recovery cost

Safety is unchanged: `GetStableOffset` remains authoritative, the same offset
is retried after an unknown transaction outcome, and no read may pass a live
reservation. Commit advances only to the coordinator's materialized offset.
Abort plus handler failure or client disconnect continues to reconnect from the
unchanged broker offset and redeliver the input.

Availability improves because a fence does not close the stream and a large
set of blocked streams no longer synchronizes ten coordinator requests per
second. Jitter reduces retry bursts when many Saga transactions begin together.

For one continuously blocked stream, the existing branch performs about 600
stable-offset requests per minute. After the first second, the new steady-state
cost is approximately 60 requests per minute, with no reconnect handshake,
post-disconnect offset fetch, or replay bytes. The trade-off is a worst-case
release-detection delay of 1.2 seconds at the jittered cap. The stream keepalive
continues at its existing interval and is not counted as coordinator recovery
work.

An event-driven cross-broker reservation watch could remove the remaining
steady-state requests. It is a separate proposal because it needs a versioned
protocol, cancellation, coordinator failover, notification-loss fallback, and
resource admission for watchers.

## Regression and fault-recovery checks

Implementation is complete only when the following checks pass:

- a short fence starts at the 100 ms retry interval and resumes on the same
  connection and offset;
- a long fence reaches the one-second cap, makes a bounded number of
  coordinator checks, and does not reconnect;
- repeated partition appends during a fence do not cause extra stable-offset
  checks;
- keepalives continue while blocked without triggering offset checks;
- a committed reservation resumes from the materialized stable offset;
- abort followed by handler failure or client disconnect leaves the broker
  offset unchanged and redelivers it on the next stream;
- stream cancellation, timeout, replacement, and connection failure schedule
  no further fence checks;
- successful ordinary streams retain the existing 100 ms catch-up behavior;
- non-fence protocol errors still close the stream;
- episode, recheck, and duration metrics have the defined cardinality and
  increment semantics;
- relevant stream and controller tests pass under the race detector where the
  local toolchain supports it.

## Non-goals

- removing or weakening `GetStableOffset` fencing;
- changing the meaning of `unstable_offset_commit`;
- changing transaction commit or abort semantics;
- changing broker idle timeouts;
- introducing a cross-broker reservation notification protocol;
- changing SDK reconnect or offset-redelivery contracts.
