# HWM Compatibility and ISR Recovery Design

## Context

PR #169 made the Raft partition metadata high-water mark (HWM) authoritative so
that a newly elected leader can remove an uncommitted local tail. Two upgrade
compatibility gaps remain:

1. Before PR #169, `PartitionMetadata.CommittedHWM` was always encoded as
   `committed_hwm`, including when its value was zero. The current decoder uses
   field presence as the `CommittedHWMKnown` signal, so a version 7 or 8
   snapshot written by an older binary can turn a compatibility zero into an
   authoritative zero and truncate a durable log during restore.
2. The ISR manager removes stale replicas but deliberately considers only the
   current ISR. A successful follower append updates liveness only. There is no
   independent, fenced proof that permits a caught-up replica to re-enter ISR,
   so a rolling restart can leave every partition permanently under-replicated.

This change fixes both gaps without rolling back PR #169, changing an operator
configuration, writing production data, or modifying consumer offsets.

## Goals

- Preserve the durable tail of version 7 and 8 snapshots when an unmarked HWM
  zero cannot be distinguished from the historical compatibility value.
- Preserve authoritative nonzero HWMs and the existing truncation of tails
  beyond an authoritative HWM.
- Make new zero HWMs unambiguously authoritative on the wire.
- Keep historical Raft command replay semantics independent of the binary that
  replays the command.
- Re-admit a replica only after it proves that its local LEO and HWM exactly
  equal the current Raft-authoritative committed HWM under current leader and
  topic lifecycle epochs.
- Allow an already-synchronized restarted broker to re-enter ISR without a new
  publish.
- Preserve mixed-version rolling-upgrade behavior.

## Non-goals

- No production cluster, cluster configuration, topic data, or consumer offset
  mutation is part of this code change.
- This change does not add unclean leader election or infer synchronization
  from heartbeat liveness.
- This change does not alter ACK, idempotence, or topic lifecycle contracts.
- This change does not merge the resulting pull request.

## HWM Wire Semantics

### Partition metadata marker

Partition metadata gains an additive `committed_hwm_version` field. Version 1
means that `committed_hwm`, including zero, is authoritative. The in-memory
`CommittedHWMKnown` flag remains the behavior switch, while JSON decoding uses
the following compatibility table:

| HWM value | Marker | Meaning |
| --- | --- | --- |
| absent or zero | absent | legacy/unknown boundary |
| nonzero | absent | authoritative legacy nonzero boundary |
| any value | version 1 | authoritative boundary |
| any value | unsupported version | reject |

This deliberately treats an unmarked zero in every version 8-or-earlier
snapshot as unknown. The ambiguity cannot be resolved safely from the legacy
bytes, and preserving a durable tail is safer than destructive truncation.

When authoritative metadata is encoded, version 1 and the numeric HWM are both
written. Unknown metadata omits both. A nonzero legacy HWM becomes explicitly
versioned the next time a current snapshot is written.

### Snapshot version 9

The current snapshot format becomes version 9. Restore accepts versions 0
through 9. Versions 8 and earlier are migrated through the marker rules above;
they never gain authoritative-zero meaning merely because `committed_hwm: 0`
was present.

To retain mixed-binary rolling compatibility, broker registration advertises
partition-recovery protocol version 1. While an active broker lacks that
capability, a current broker continues writing the prior version 7/8 envelope,
using only additive fields older readers ignore. Once all active brokers
advertise version 1, snapshots are written as version 9. A cluster composed
only of current brokers, or a snapshot without registered active brokers,
writes version 9 immediately.

### Topic Raft command marker

`TOPIC` gains an additive `committed_hwm_version` field. Current topic command
producers set it to version 1. A newly created partition therefore starts with
an explicit authoritative HWM zero.

Historical `TOPIC` log entries contain no marker. Replaying those exact bytes
creates partition metadata with an unknown HWM, preserving the historical
durable-tail behavior. Adding partitions with a current command creates only
the new partitions with authoritative zero; retained partitions keep their
existing HWM state.

### Legacy HWM migration without publish

An unknown boundary cannot produce an ISR catch-up proof. On each leader-side
ISR refresh, the current partition leader checks its local materialized
partition. If the Raft HWM is unknown, it submits the local durable HWM using
the existing epoch- and lifecycle-fenced `PARTITION_COMMIT` command. This is the
same compatibility boundary previously migrated before a publish, but it no
longer requires application traffic. Once committed, all replicas observe the
authoritative boundary and can prove synchronization.

## Catch-up Proof

The proof is an internal authenticated wire object containing:

- topic and partition;
- broker ID;
- Raft-authoritative committed HWM;
- locally observed LEO and HWM;
- leader epoch; and
- topic lifecycle epoch.

A broker builds a proof only for a partition where it is a configured replica,
is currently outside ISR, the committed HWM is authoritative, the local topic
generation matches, and `local LEO == local HWM == committed HWM`. A broker does
not truncate or advance local state merely to manufacture a proof.

### Transport

The existing authenticated cluster heartbeat receives an optional list of
catch-up proofs. Only partitions eligible for re-entry are included, so normal
heartbeats remain small. Heartbeat delivery continues to establish liveness
only; it never directly changes ISR.

The receiving broker verifies that every proof's broker ID matches the
heartbeat node ID. Only the current Raft leader proposes an ISR change. A broker
sends its heartbeat to its own discovery endpoint as well as peers, ensuring
that a restarted broker that is also the Raft leader can submit its proof.

### Raft command compatibility

ISR changes continue to use the existing `PARTITION` Raft command so older
followers participating in a rolling upgrade understand the state transition.
The flat JSON payload gains additive `partition_update_version` and
`catch_up_proof` fields.

- Historical `PARTITION` bytes have no update marker and retain legacy replay
  semantics.
- Current version 1 commands may remove ISR members without a proof.
- A current version 1 command that adds any ISR member must carry a valid proof
  for that member.

Before proposing the command, the leader validates the proof against its current
FSM view. The FSM repeats validation when the committed command is applied. The
proof is transient and is not retained in partition metadata or snapshots.

### FSM validation and idempotence

For an ISR addition, the FSM verifies:

1. the partition metadata and topic generation exist;
2. the broker is in the replica set;
3. the authoritative HWM is known and equals the proof's committed HWM;
4. proof leader epoch equals current leader epoch;
5. proof lifecycle epoch equals current lifecycle epoch;
6. proof local LEO and local HWM both exactly equal the authoritative HWM; and
7. the requested metadata does not regress the HWM or violate existing leader
   and lifecycle fencing.

A non-replica, mismatched HWM, behind LEO, ahead/uncommitted LEO, stale epoch,
or forged heartbeat identity is rejected. Adding a broker already in ISR is a
successful no-op. ISR ordering follows replica-set order for deterministic
snapshots and replay.

## Failure Handling

- Malformed or fenced proofs are rejected and logged without changing ISR.
- A proof racing with a HWM commit or lifecycle/leader change fails closed; the
  next heartbeat can submit a fresh proof.
- Heartbeat success without a valid proof can only update liveness.
- If the local replica is behind the authoritative HWM, no proof is produced;
  normal replication must catch it up first.
- If local state is ahead of the authoritative HWM, existing leadership or
  replica preparation reconciliation remains responsible for truncating the
  uncommitted tail before a later proof can be produced.

## Test Strategy

### Snapshot and replay compatibility

- Restore version 7 and version 8 snapshots containing an unmarked
  `committed_hwm: 0`, a durable tail of one, and no HWM checkpoint; assert LEO
  and HWM remain one.
- Replay historical raw `TOPIC` command bytes without the marker; assert zero is
  not authoritative.
- Apply a current `TOPIC` command with marker version 1; assert authoritative
  initial HWM zero.
- Preserve an unmarked authoritative nonzero HWM.
- Restore authoritative HWM one with local tail greater than one; assert the
  tail is truncated to one.
- Replay historical `PARTITION` bytes without an update marker and verify their
  legacy state transition remains unchanged.

### ISR proof behavior

- A heartbeat with no proof cannot add a replica to ISR.
- A proof whose local LEO is below the authoritative HWM is rejected.
- A proof with matching local LEO/HWM and all fences re-enters ISR.
- Stale leader epoch and stale lifecycle epoch proofs are rejected.
- A non-replica broker and HWM mismatch are rejected.
- Repeating an accepted proof is idempotent.
- Proof generation includes only synchronized, out-of-ISR local replicas.

### Regression and E2E

- Retain existing ACK, idempotence, topic lifecycle, snapshot restore, and FSM
  suites.
- Add an opt-in three-node Docker E2E that publishes durable data, restarts each
  broker sequentially without another publish, waits after every restart for
  ISR size three, and asserts
  `cursus_cluster_under_replicated_partitions == 0`.
- Run `gofmt`, targeted non-cached tests, `go test ./...`, `go vet ./...`,
  `git diff --check`, and the rolling-restart E2E when Docker is available.

## Rollout Boundary

The pull request is the terminal action in this task. It must not be merged by
the implementation agent. No cluster-config image pin or production rollout may
begin until the PR is merged by its owner and an immutable image digest from the
resulting `main` commit exists. The subsequent operator-owned sequence is:

1. merge after CI and review approval;
2. build and record the immutable `main` image digest;
3. update the cluster-config pin to that digest;
4. roll one broker at a time, waiting after each for readiness, ISR=3, and zero
   under-replicated partitions;
5. verify durable topic LEO/HWM and consumer offsets without writing test data;
6. resume Commerce CDC only after the cluster has remained converged.
