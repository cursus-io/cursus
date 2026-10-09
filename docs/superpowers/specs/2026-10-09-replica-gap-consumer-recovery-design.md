# Replica Gap and Consumer Recovery Design

## Scope and safety boundaries

This work starts from `origin/main` at `432ce6b` on the independent branch
`fix/replica-gap-consumer-recovery`. It reproduces and fixes a three-broker
replica offset gap and the consumer-group disruption observed at the same time.

Public commits, tests, logs, and pull-request text must use generic broker,
topic, consumer, and network identifiers. They must not contain internal
project names, production topic names, addresses, or other environment details.

The production cluster is out of scope for mutation. This work must not issue
additional truncations, alter internal offsets, edit PVC contents, or modify
Raft files. Any production recovery procedure is advisory until a consistent
offline backup and the affected boundaries have been verified.

The stream-index recovery defect handled separately must not be presented as
the same root cause as this replication defect.

## Confirmed code-path hypothesis

The current publish path releases the partition write lock when the client
handler returns, while an already submitted replication task continues retrying
in its partition lane. Partition preparation reconciles local state to the
Raft-committed high watermark only when the cached leader fence changes. A
second append in the same leader and lifecycle epoch can therefore allocate an
offset after an unresolved local tail. If a follower is behind that tail, the
next replication can produce an offset-gap response.

The first reproduction step will prove this path with timing and boundary
evidence. If it does not reproduce, the implementation stops and the observed
logs, per-replica boundaries, and actual causal path replace this hypothesis.
Network failures are recorded as a possible trigger, not assumed to be the
cause of the offset gap.

## Considered approaches

### 1. Keep the write lock across unlimited retries

This prevents a second append from overtaking the task but can block a
partition forever. It also leaves a divergent follower in ISR and permits
health/readiness to report a false success. This is rejected.

### 2. Quarantine the divergent replica and resolve from Raft state

This is the selected approach. A typed replica-gap error identifies the target
broker. The leader excludes that replica from ISR through a fenced Raft
transition, reads the authoritative committed HWM, and resolves the existing
task before allowing another write. Existing catch-up and prefix-proof paths
are the only way to re-enter ISR.

This approach reuses the current log transfer and proof contracts, keeps the
public Wire protocol and topic storage format unchanged, and bounds the gap
recovery path without skipping or inventing offsets.

### 3. Force a leader-epoch change for every gap

An epoch change would fence the old task but disrupt healthy replicas and does
not by itself prove which record committed. It adds unnecessary failover risk
and is rejected.

## Recovery lifecycle

The partition moves through the following internal lifecycle:

1. **Normal**: local LEO/HWM and the Raft committed HWM are compatible, and the
   configured replication policy admits a write.
2. **Unresolved task**: a local append owns the partition write boundary until
   replication reaches a decision. Client cancellation does not cancel or
   release this ownership.
3. **Gap detected**: a typed `replica_offset_gap` response records the divergent
   broker. New writes fail closed while the recovery decision is pending.
4. **Replica quarantined**: a leader-, lifecycle-, and expected-membership-
   fenced Raft command removes the divergent broker from ISR without removing
   it from the replica assignment.
5. **Task resolved**: the leader rereads Raft metadata. If committed HWM already
   covers the task boundary, the task is committed. Otherwise only the local
   tail above committed HWM is reconciled away. A timed-out request remains an
   unknown outcome until this check finishes; it is never retroactively
   reported as a definite success.
6. **Catch-up pending**: the quarantined replica remains outside ISR and the
   partition remains unhealthy and not ready. No unverified inline backfill,
   offset skip/reset, or internal-topic deletion is allowed.
7. **Verified**: the existing bounded catch-up copies committed records,
   verifies the prefix at the authoritative HWM, and emits the existing proof.
   Only the fenced proof can re-admit the replica to ISR.
8. **Normal**: writes and readiness resume after the recovery fence has cleared.

An already acknowledged leader-only write must not be silently discarded. Its
task remains recovery-pending until Raft HWM proves it committed or the normal
replication contract completes on the surviving ISR. A client-timeout path
does not gain this prior-success constraint and may resolve as uncommitted by
rolling back only the tail above the Raft HWM.

## Components and data flow

### Typed replica failures

The cluster replication layer will preserve the target broker ID, protocol
error code, retry classification, and original response in a typed error.
Only an authenticated `replica_offset_gap` from a configured target enters the
quarantine path. Transport failures stay distinct so the implementation does
not claim an unproven network-to-gap causal relationship.

### Fenced ISR exclusion

A dedicated internal Raft command will remove one replica from ISR only when
the topic, partition, leader, leader epoch, lifecycle epoch, configured replica
set, and expected ISR still match. Replaying the same exclusion is idempotent.
The command never removes the replica assignment and never admits a replica.

### Partition write gate and HWM reconciliation

The replication lane owns a per-partition recovery gate. Partition preparation
checks that gate and compares Raft committed HWM, local LEO/HWM, leader epoch,
and lifecycle epoch before every post-failure or post-restart append. It keeps
committed records, truncates only an uncommitted tail, and rejects writes while
quarantine or task resolution is incomplete.

The gate must be reconstructable after restart from durable ISR and local
boundary state. A configured replica outside ISR with unfinished catch-up is
treated conservatively as recovery-pending until proof-based admission.

### Health and readiness

`CLUSTER_STATUS` must remain unhealthy during recovery and expose enough
generic counts/reasons to distinguish under-replication, catch-up pending, and
local materialization pending. Node readiness will include cluster topology and
replica-recovery checks. A writable minimum ISR alone is not sufficient while
an offset-gap recovery fence is active.

The change must preserve the existing behavior that an unrelated inactive
replica can be reported as degraded without falsely labeling it as an active
gap recovery. The readiness gate is tied to recovery/catch-up state, not every
generic availability reduction.

### Consumer-group behavior

The reproduction records durable group registration, offset commits, heartbeat
and leave behavior, and resume position for an unrelated consumer. If these
fail only while the internal offset partition is blocked, they are documented
as downstream impact of the replication defect. If they fail independently,
the separate causal path receives its own focused fix and tests in this branch;
the network symptom alone is not treated as proof.

## Test design

### Deterministic unit and component tests

- A timed-out task retains partition ownership, and a second append in the same
  leader epoch cannot overtake it.
- A replica-gap response identifies and quarantines only the divergent replica.
- ISR exclusion is fenced, idempotent, and committed through Raft.
- Task resolution preserves committed data and removes only a local tail above
  the authoritative HWM.
- An acknowledged leader-only task is not silently rolled back.
- A quarantined replica cannot rejoin ISR without completed catch-up and prefix
  proof.
- `CLUSTER_STATUS` reports unhealthy and readiness fails throughout recovery,
  then both recover after verified ISR admission.
- Restart between gap detection, ISR exclusion, HWM resolution, and proof
  admission reconstructs a fail-closed state.
- Consumer durable registration, commit, heartbeat/leave, and resume continue
  for an unrelated group after recovery.

### Three-broker Docker scenario

One scenario will perform, with generic test identifiers:

1. consecutive topic truncations;
2. a deliberately delayed response whose server-side application is verified
   from revision, lifecycle epoch, and every replica's LEO/HWM;
3. a broker stop and restart using the same test volume;
4. internal consumer-offset replication with an injected follower boundary
   mismatch;
5. a second append in the same leader epoch while the first task is unresolved;
6. explicit ISR exclusion, unhealthy/not-ready assertions, and write rejection;
7. committed-HWM resolution, catch-up, prefix proof, and ISR re-admission;
8. an unrelated consumer's registration, offset commit, restart/resume, and
   exact offset progression.

Success requires exact per-replica LEO/HWM, ISR membership, proof status, and
consumer offset progress. `healthy=true`, process liveness, and container
readiness alone are never sufficient.

Failure-injection variants cover response timeout, follower restart, transport
failure without a gap, leader restart during recovery, and shutdown/restart
after an unresolved task. The suite must prove that no test repairs state by
deleting an internal topic or editing stored offsets.

## Compatibility

The public command grammar, Wire payloads, normal offset allocation semantics,
and topic log storage format remain unchanged. New control data is internal to
Raft replication and must be backward-safe for rolling restart or guarded by
the repository's existing protocol-version checks.

## Production recovery proposal boundaries

No production recovery action is executed by this work. The eventual runbook
will require this order:

1. freeze mutating recovery commands and identify affected partitions;
2. record Raft leader/epoch/lifecycle metadata, ISR, committed HWM, and every
   replica's LEO/HWM;
3. obtain a consistent offline backup of all three broker data volumes and Raft
   state before any further truncation or repair;
4. validate backup consistency and blast radius in an isolated environment;
5. deploy the verified fix using the repository's supported rolling procedure;
6. allow Raft quarantine, catch-up, and prefix proof to converge without manual
   offset edits;
7. verify group registration and exact consumer resume positions before
   restoring normal traffic.

Post-deployment observation includes replication retries by class, quarantined
or catch-up-pending replicas, ISR size, per-replica LEO/HWM, accepted/rejected
prefix proofs, readiness failures, durable group-registration errors,
heartbeat/leave timeouts, and consumer offset continuity.
