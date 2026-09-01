# Topic Replication and Publish Acknowledgement Policy

## Status

Implementation design for topic-specific replication safety and request-specific
publish acknowledgement behavior.

## Goals

- Persist an optional `min_in_sync_replicas` override in each durable topic
  policy.
- Fall back to the broker `MinInSyncReplicas` value when the override is absent.
- Give `acks=0`, `acks=1`, and `acks=all`/`acks=-1` distinct completion
  contracts for single and batch publish requests.
- Keep uncommitted leader-only records beyond the committed HWM and invisible to
  committed consumers.
- Preserve committed/uncommitted boundaries through restart, snapshot restore,
  fencing, and leader failover.
- Reject idempotent publishes unless their acknowledgement mode is `all` or
  `-1`.

## Non-goals

- Compatibility with another broker's wire protocol, client API, or defaults.
- ACL, authentication, CDC, cluster configuration, deployment, or image changes.
- Storing a publisher's `acks` choice in topic metadata.

## Topic configuration model

`topic.Policy` gains an optional integer `MinInSyncReplicas` field. A pointer is
used so an absent override remains distinguishable from an explicit value. JSON
uses `min_in_sync_replicas` with `omitempty`; old manifests and snapshots that
omit the field decode to `nil` and continue to use the broker default without a
metadata rewrite.

The effective value is:

```
topic policy override, when present
otherwise broker MinInSyncReplicas
```

Non-positive broker defaults are normalized to one at the usage boundary. An
explicit topic value must be at least one and no greater than the topic's actual
replication factor. In distributed mode the actual replica set recorded in
partition metadata is authoritative. A standalone topic has one actual replica.

`CREATE` accepts `min_in_sync_replicas=N`. Repeating `CREATE` without the field
does not clear or replace the existing override. Explicit runtime changes use a
separate administrative command:

```
ALTER_TOPIC_CONFIG topic=<name> min_in_sync_replicas=<N|default>
```

`default` deletes the override. Both set and delete operations are serialized by
the standalone topic metadata lock or the distributed Raft FSM. Validation occurs
before the durable definition changes. No partial update is exposed if local
materialization or persistence fails.

CREATE and METADATA responses report the configured override (or `default`) and
the current effective value. They never report `acks` as topic metadata.

## ACK normalization and validation

A shared policy parser trims and lower-cases acknowledgement values. Empty values
use the existing default `1`; `-1` canonicalizes to the `all` execution path while
the accepted alias remains documented. `0`, `1`, `all`, and `-1` are the only
valid inputs.

The SDK validates the publisher configuration before opening connections or
creating a topic. The broker repeats validation before topic lookup, sequence
validation, local append, or producer-state mutation. An effectively idempotent
request (request, broker, or durable topic idempotence enabled) requires `all` or
`-1`; conflicting values fail instead of being rewritten.

Single and batch handlers build the same internal publish plan and use the same
acknowledgement execution path. Forwarding preserves the original mode. Internal
transactional publishes use the required strong mode.

## Distributed publish pipeline

Each partition has one ordered replication lane owned by the shared command
handler. A fixed-capacity queue bounds outstanding replication work. Queue space
is reserved before local append. This provides backpressure without allowing a
successful append to be followed by a queue-full error.

The request path holds the existing partition append lock while it:

1. validates leadership and captures leader ID, leader epoch, replica set, and
   ISR;
2. checks `effectiveMinISR` when the mode is `all`, before reconciliation or
   any other state change;
3. reconciles the committed HWM;
4. reserves queue capacity;
5. performs the durable leader append; and
6. enqueues a replication task with an exact commit HWM of last offset plus one.

The ordered lane replicates tasks sequentially so followers never observe an
offset gap caused by concurrent requests. Each materialized partition owns one
worker and a fixed-capacity lane rather than creating request-scoped goroutines.
Completion is based only on the captured ISR; non-ISR catch-up work is best
effort and never delays a strong acknowledgement.

Before commit, the worker confirms that the local broker is still the leader at
the captured epoch and that every member of the task's ISR has acknowledged.
`effectiveMinISR` is an admission condition only for `all`/`-1`; it is never
retroactively imposed on an accepted `acks=1` task. The Raft partition-commit
command carries the captured leader and epoch. The FSM rejects stale or fenced
commits. The local HWM advances only after the durable cluster commit succeeds.

## ACK completion contracts

### `acks=0`

The external server writes no response frame. The SDK returns after writing the
request and never reads an acknowledgement. Internal forwarding connections do
receive a private routing response so a non-leader can finish forwarding without
timing out; the originating external connection still receives no frame.

The request does not wait for follower replication. Processing failures and retry
outcomes are not guaranteed to the producer. Invalid requests are rejected before
append, but a producer using this mode does not receive the rejection.

### `acks=1`

The response follows successful durable leader append and queue submission. It
does not wait for any follower response and does not reject because the current
ISR is below `effectiveMinISR`. The ordered worker continues replication after the
response. A leader failure before commit can lose the acknowledged tail.

### `acks=all` and `acks=-1`

The handler rejects before append when the current ISR is smaller than
`effectiveMinISR`. After append it waits for the ordered task to be acknowledged
by every ISR member captured for the request, followed by the fenced Raft commit
and local HWM update. A non-ISR replica cannot delay success. A leader or epoch
change returns failure, never success.

## Standalone behavior

A standalone partition has one replica: the local broker. `acks=0` receives no
response and uses the existing asynchronous local append contract. `acks=1`
responds after the existing synchronous local append contract. `all` and `-1`
also use the synchronous local contract, but reject before append when the
effective minimum exceeds one. There is no follower wait.

## HWM, recovery, and visibility

Leader append changes LEO but not HWM. Replication tasks commit their own exact
end offset in order. Read-committed consumers remain bounded by HWM. Follower
append also leaves HWM unchanged until the commit command arrives.

The committed HWM remains in partition checkpoints and FSM snapshots. Leadership
preparation and snapshot restore reconcile each local log to that watermark,
truncating any uncommitted tail. Async tasks are canceled on fencing or shutdown;
their records stay uncommitted and are eligible for truncation rather than being
promoted implicitly.

Partition metadata written before this contract has no committed-HWM field. It
is not decoded as an explicit zero: field absence remains a legacy/unknown
marker, so upgrade recovery does not truncate previously accepted data. Before
the partition leader accepts its first new append, it preserves the legacy local
committed boundary and commits that exact value through Raft at the current
leader epoch. New metadata always records even an explicit zero, allowing later
restart and failover to distinguish a safe empty HWM from legacy absence.

## Backpressure and shutdown

Queue admission observes the request context and handler shutdown. The handler
stops accepting work before cancellation, wakes blocked admissions and strong-ack
waiters, cancels outstanding replica RPCs, and waits for workers to exit. No
request creates an unbounded goroutine. Queue saturation is classified separately
from replication, fencing, cancellation, and shutdown failures.

## Observability and logging

Counters distinguish publish outcomes by canonical ACK mode and result. Async
replication failures have a separate counter labeled by topic and bounded error
class. Structured errors log topic, partition, offset boundary, leader epoch, and
error class. Message payloads, authentication tokens, and credentials are
redacted or omitted.

## Compatibility

- The SDK default remains `acks="1"`.
- Missing topic overrides continue to use the broker default.
- Old standalone manifests and FSM snapshots decode without the new field.
- Old partition metadata without `committed_hwm` preserves its prior recovery
  semantics and is promoted once through an epoch-fenced Raft commit before a
  new publish append.
- Snapshot restore, topic materialization, topic/group/offset recovery, and
  existing HWM checkpoints retain their current formats and invariants.
- A snapshot format bump is only required if restore validation cannot safely
  distinguish the additive optional field; otherwise the JSON addition remains
  backward-decodable and version 6 is retained.

## Verification

Tests cover topic creation, alteration, reset-to-default, independent topic
overrides, invalid values, legacy metadata, restart and snapshot restore; ACK
normalization, single/batch parity, response framing, follower barriers, queue
backpressure, async continuation, exact HWM progression, ISR membership,
fencing, alias parity, idempotence validation, shutdown, race safety, and
failover truncation. Relevant package tests run first, followed by `go test
./...`, race tests for changed concurrency packages, formatting, vetting, and the
repository's existing validation targets.
