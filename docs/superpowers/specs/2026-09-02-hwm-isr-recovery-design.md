# Clean-Bootstrap HWM and ISR Recovery Design

## Context

PR #169 made the Raft partition metadata high-water mark (HWM) authoritative so
that a newly elected leader can remove an uncommitted local tail. Two gaps are
visible in the currently deployed state:

1. An old version 7 or 8 snapshot can encode `committed_hwm: 0` without saying
   whether zero is authoritative. A current decoder can therefore truncate a
   durable tail when replaying ambiguous legacy metadata.
2. The ISR manager removes stale replicas but has no fenced path for a caught-up
   replica to re-enter ISR. Heartbeat proves liveness only, so a rolling restart
   can leave every partition permanently under-replicated.

The sole operator of the deployment has confirmed that the current Cursus
topics, Raft state, snapshots, and consumer offsets contain no data that must be
preserved. This design therefore establishes a deliberate clean-bootstrap
format boundary instead of carrying legacy migration code.

## Goals

- Make the first supported recovery format unambiguous about authoritative HWM
  zero.
- Refuse ambiguous legacy snapshots instead of guessing or truncating data.
- Preserve authoritative HWM reconciliation, including removal of an
  uncommitted local tail.
- Re-admit a replica only after it proves that its local LEO and HWM exactly
  equal the current Raft-authoritative committed HWM under current leader and
  topic lifecycle epochs.
- Allow an already-synchronized restarted broker to re-enter ISR without a new
  publish.
- Verify that a three-node cluster bootstrapped entirely with the new binary
  converges to ISR=3 and zero under-replicated partitions after every later
  rolling restart.

## Non-goals

- The PR does not migrate, import, or preserve version 8-or-earlier snapshots
  or markerless historical Raft logs.
- The initial transition is not a mixed-version rolling upgrade. All Cursus
  persistent state must be removed while all brokers are stopped, then all
  brokers must start with the new image.
- No production cluster, cluster configuration, topic, consumer offset, PVC,
  or deployment is changed as part of this coding task.
- The implementation does not add unclean leader election or infer log
  synchronization from heartbeat liveness.
- The implementation agent does not merge the resulting pull request.

## Recovery Format Boundary

### Snapshot version 9

All new snapshots use version 9. Restore accepts version 9 only. Versions 0
through 8 fail closed with an error that identifies the unsupported legacy
snapshot and the clean-bootstrap requirement. Unknown future versions also
fail closed.

This is an intentional breaking storage-format boundary. A deployment must not
start the new binary against any retained old Raft directory because historical
log entries can be replayed before a snapshot is installed. The operator must
remove the complete Cursus Raft, snapshot, topic-log, HWM-checkpoint, producer,
and consumer-offset state as one unit. Partial deletion is unsupported.

### Authoritative HWM marker

Partition metadata gains an additive `committed_hwm_version` field. Version 1
means that the accompanying `committed_hwm`, including zero, is authoritative.
Version 9 snapshots require version 1 on every partition.

The decoder uses the following rules:

| HWM marker | Meaning |
| --- | --- |
| version 1 with a numeric HWM | authoritative boundary |
| absent | unsupported legacy metadata |
| unsupported version | reject |
| marker present without numeric HWM | reject |

The in-memory `CommittedHWMKnown` flag remains the behavior switch but must be
true for every partition in a version 9 state. When metadata is encoded, both
the version and numeric HWM are written.

### Topic Raft command marker

`TOPIC` gains a required `committed_hwm_version` field. Current topic command
producers set it to version 1. A newly created partition therefore starts with
an explicit authoritative HWM zero.

Markerless `TOPIC` commands return an unsupported recovery-protocol error.
There is no compatibility replay path because deployment of this binary
requires an empty Raft log.

### Authoritative truncation

Existing `ReconcileCommittedHWM` behavior remains unchanged for supported
metadata. If local LEO is above the authoritative HWM, the uncommitted tail is
truncated. If local LEO is below the HWM, recovery fails closed. A local LEO and
HWM already equal to the boundary require no mutation.

## Catch-up Proof

The catch-up proof is an internal authenticated wire object containing:

- topic and partition;
- broker ID;
- Raft-authoritative committed HWM;
- locally observed LEO and HWM;
- leader epoch; and
- topic lifecycle epoch.

A broker builds a proof only for a partition where it is a configured replica,
is currently outside ISR, the committed HWM is authoritative, the local topic
generation matches, and `local LEO == local HWM == committed HWM`. A broker
does not truncate or advance local state merely to manufacture a proof.

### Transport

The existing authenticated cluster heartbeat receives an optional list of
catch-up proofs. Only synchronized partitions currently outside ISR are
included. Heartbeat delivery itself continues to update liveness only and can
never change ISR.

The receiving broker verifies that every proof's broker ID matches the
heartbeat node ID. Only the current Raft leader submits the proof to Raft. A
broker sends heartbeat to its own discovery endpoint as well as its peers so a
restarted broker that is also the Raft leader can submit its proof.

### Raft command

A new `ISR_CATCHUP` Raft command carries exactly one proof. The command is safe
because the supported deployment starts every broker with the same new binary
and an empty Raft store. Mixed-version replay is explicitly unsupported.

The leader validates a proof against its current FSM view before proposing the
command. The FSM repeats the validation when the committed command is applied.
The proof is not retained after the ISR transition.

### FSM validation and idempotence

The FSM verifies:

1. the topic and partition metadata exist;
2. the broker is in the configured replica set;
3. the authoritative HWM is known and equals the proof's committed HWM;
4. proof leader epoch equals the current leader epoch;
5. proof lifecycle epoch equals the current lifecycle epoch;
6. proof local LEO and local HWM both exactly equal the authoritative HWM; and
7. adding the broker does not violate existing leader, lifecycle, or HWM
   invariants.

A non-replica, HWM mismatch, behind or ahead LEO, stale leader epoch, stale
lifecycle epoch, or mismatched heartbeat identity is rejected. A broker already
in ISR produces a successful no-op. ISR ordering follows replica-set order for
deterministic snapshots and replay.

## Failure Handling

- Legacy snapshot or metadata detection aborts restore without materializing a
  topic or reconciling a local HWM.
- Malformed or fenced proofs are rejected without changing ISR.
- A proof racing with an HWM commit or lifecycle/leader change fails closed;
  the next heartbeat can submit a fresh proof.
- Heartbeat success without a valid proof only updates liveness.
- A replica below the authoritative HWM produces no proof and must catch up
  through normal replication.
- A replica above the authoritative HWM must pass existing reconciliation
  before it can later produce a proof.

## Test Strategy

### Format boundary and HWM behavior

- Restore version 7 and version 8 snapshots and assert they are rejected before
  local materialization or truncation.
- Restore version 9 metadata missing `committed_hwm_version` and assert it is
  rejected.
- Replay a markerless `TOPIC` command and assert it is rejected.
- Apply a version 1 `TOPIC` command and assert a new partition has explicit
  authoritative HWM zero.
- Restore authoritative HWM one with local tail greater than one and assert the
  uncommitted tail is truncated to one.
- Restore authoritative HWM above local LEO and assert recovery fails closed.

### ISR proof behavior

- A heartbeat with no proof cannot add a replica to ISR.
- A proof whose local LEO is below the authoritative HWM is rejected.
- A proof with matching local LEO/HWM and all fences re-enters ISR.
- Stale leader and lifecycle epoch proofs are rejected.
- A non-replica broker and HWM mismatch are rejected.
- Repeating an accepted proof is idempotent.
- Proof generation includes only synchronized, out-of-ISR local replicas.

### Regression and E2E

- Retain existing ACK, idempotence, topic lifecycle, supported snapshot restore,
  and FSM tests, updating fixtures to the version 9 format.
- Add an opt-in three-node Docker E2E that clean-bootstraps the new version,
  publishes test-fixture data, restarts each broker sequentially without another
  publish, waits after every restart for ISR size three, and asserts
  `cursus_cluster_under_replicated_partitions == 0`.
- Run `gofmt`, targeted non-cached tests, `go test ./...`, `go vet ./...`,
  `git diff --check`, and the rolling-restart E2E when Docker is available.

## Pull Request and Deployment Boundary

The pull request is the terminal action in this task. It must be ready for
review, must receive and address the initial automated reviews, and must not be
merged by the implementation agent. No cluster-config pin, PVC deletion, or
production rollout may begin until the PR is merged by its owner and an
immutable image digest from the resulting `main` commit exists.

The later operator-owned transition is destructive and requires explicit target
verification before deletion:

1. keep Commerce CDC stopped;
2. merge the PR after CI and review approval;
3. build and record the immutable `main` image digest;
4. stop all Cursus brokers together;
5. verify the exact Cursus-only Raft, snapshot, topic-log, checkpoint, producer,
   and consumer-offset storage targets;
6. delete those verified Cursus storage targets as one clean-bootstrap unit;
7. update the cluster-config image pin to the immutable digest;
8. start all three brokers and bootstrap a new cluster;
9. verify readiness, ISR=3, and zero under-replicated/offline partitions;
10. perform a same-version sequential restart validation and reconfirm ISR=3;
11. recreate required consumer groups and resume Commerce CDC only after the
    clean cluster remains converged.
