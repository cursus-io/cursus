# Three-broker cleanup rejection and recovery cost

## Scope and checks before implementation

Audit official main `9c43d398eecc387ae0011bb7900b54a74a0b0d8d`.
Reuse the existing cleanup failover, reply-loss, re-registration, snapshot and
guest-power-cut fixtures. PR 313 owns normal history scan/compaction changes;
this work does not alter that implementation. Legacy TRUNCATE is unchanged.

[Adopted KIP-496](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/122915859/KIP-496+Administrative+API+to+delete+consumer+offsets)
and Kafka [3.9.0 GroupCoordinator](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/coordinator/group/GroupCoordinator.scala)
reject deletion of subscribed offsets and validate group ownership/state.
Cursus retains the stricter whole-topic, shared/pattern subscription and
transaction-reservation guards. Kafka offset/prefix deletion is not a reset of
Cursus producer, consumer, transaction and event state.

The missing integration matrix will run real brokers, replicated metadata,
normal admission and disk page reads. An `e2e_faults`-only, runtime-gated hook
will alter one follower's returned page after the real read: omit a tombstone,
corrupt its payload, conflict its epoch, expose a record outside the committed
page, replay an obsolete epoch, or add a transactional envelope. The durable
log remains intact. An observation marker proves the selected record reached
the hook; pending status and rejected writes must prove no final publication.
This is a local read-response fault model, not on-disk sector corruption or a
claim that the source actually committed an invalid record. Removing the hook
and restarting that follower must recover from the unchanged authoritative log.

Separately, active single-topic, shared and pattern groups and prepared offset
reservations must reject both DELETE and TRUNCATE through real routed commands,
preserving revision, data, membership and offsets. Existing successful cleanup
tests do not substitute for these rejection assertions.

Measure a controlled three-broker run with foreground traffic. Report exact
phase boundaries, elapsed recovery, logical replay/read/write/transfer work,
kernel-accounted I/O and network bytes, traced fsync/fdatasync calls, CPU and
memory. Separate startup/setup, held-fault time, recovery and verification;
do not subtract process counters across restarts or claim kernel block bytes
are physical media writes. Tracing overhead and cache state must be explicit.

Safety and availability: invalid evidence keeps the materialization fence;
only honest recovery releases it. No ownership, durable ordering or admission
check is weakened. Cost: the negative matrix intentionally retries held faults;
record repeated work rather than disguising it as useful replay.

## Recreated-name replay regression discovered by the matrix

The second matrix case recreates the same topic name, then restarts a follower.
On the baseline, historical TRUNCATE materialization compares revision/epoch
numbers before resetting local storage, but omits instance identity. A new
instance at revision/epoch 1 therefore accepts an old instance's transition to
2. Snapshot restoration likewise checks immutable identity only after preparing
the destructive lifecycle reset. Both paths must reject mismatched identities
before changing the manifest, pending fence, handlers or data.

Adopt the identity-before-epoch principle from
[KIP-516](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/127406437/KIP-516+Topic+Identifiers)
and Kafka [3.9.0 ReplicaManager](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/server/ReplicaManager.scala)
(`becomeLeaderOrFollower` checks topic ID consistency before accepting the
leader epoch). Cursus must additionally protect its whole-topic reset and
snapshot materialization. This does not add identity to legacy client requests:
their existing revision-only compatibility remains unchanged. During historical
replay, mismatched local materialization stays pending until later authoritative
commands resolve the current instance; it cannot publish readiness early.

Before changing production code, reproduce both destructive paths with a fresh
record, unchanged manifest and pending-state assertions. Then verify log-only
and snapshot-plus-log replay and the same three-broker recreation sequence.
The guard adds a constant-time identity comparison and no storage reads; any
unresolved identity conflict trades availability for preservation of local data.

The real cold-start run exposed an additional boundary: distributed mode does
not load a standalone manifest. With an empty local registry, replaying the old
CREATE opens today's files under yesterday's identity, bypassing an in-memory
identity guard. Defer opening missing user topics until the committed replay
target is reached. Finalization must first materialize the final definitions,
then validate local committed floors, then release partition recovery. Internal
consumer metadata remains available for recovery; its identity cannot be deleted
or recreated by users. Retain pending topic cleanup throughout and use the
existing epoch marker and historical cleanup cut, never today's HWM as proof.
Cold-registry log-only and snapshot-plus-log tests must accompany the warm
registry tests. A path metadata lookup detects unopened existing storage; no
records are scanned for that decision. This shifts existing disk-open work to
finalization, without adding a storage format. Entirely new storage and the
internal metadata topic retain their existing initialization behavior.

## Acceptance reconciliation

The previous [acceptance audit at `fc2d82e`](https://github.com/cursus-io/cursus/blob/fc2d82e5e9dda90881095588d8260f25cd5d1f27/docs/core/truncate-acceptance-audit.md)
contains the original requirement-to-code/test matrix and merge-state inventory.
Reuse it together with the official-main source above. Its missing integration
and operation-cost evidence are the focus here; its SDK receipt validation is
still a separate, unmerged change in PR 312. The prefix retry fix is independent
and reviewed in PR 314. No history compaction implementation from PR 313 is
copied into this change.

| Contract | Implementation / evidence | Current interpretation |
|---|---|---|
| Preserve definition and identity; explicit reset | `pkg/topic/truncate.go`, controller/FSM topic commands; existing lifecycle tests | No DELETE+CREATE substitution or desired-list omission behavior is introduced. Warm and cold recreated-instance replay are additionally protected here. |
| Revision and retry fencing | `truncate_receipt.go`, FSM receipt tests, SDK admin truncate tests in PR 312 | Legacy revision-only completed retry still conflicts. `TRUNCATE_INSTANCE` / `ExpectedInstance` succeeds only from a retained exact receipt; later definition mutation invalidates it. A receipt replay cannot clear new records, memberships or dependency state. |
| Local reset ordering | Existing manifest/epoch marker, consumer cleanup, producer state, transaction journal, event index/snapshot tests | Reuse the 11 lifecycle and 6 journal process-crash boundaries. Opening historical cold definitions is deferred until replay authority is final; local completion still waits for dependency cleanup. |
| Evidence rejection and group protection | `consumer_cleanup_rejection_e2e_test.go`, existing prepared-reservation fixture extended here | Real broker TCP and routed requests exercise missing/malformed/conflicting/out-of-cut/obsolete/transactional evidence, active/shared/pattern subscriptions and departed-member prepared reservations. The page hook is a returned-read fault, not an invalid durable append. |
| Registration races, failover, snapshot, compaction | Existing #298 re-registration/cut tests, #302 certified-compaction tests, PR 312 rerun of full cleanup failover/snapshot fixture | Preserve those proofs rather than duplicating cleanup code or rerunning unchanged durable phases. Old snapshots without a historical truncate cut remain pending; current HWM cannot supply it. |
| Recovery cost with normal traffic | `cleanup_cost_e2e_test.go`, raw samples and analyzer | Measure the full live reset-to-all-replica-convergence interval, including held-fault repeated work. Startup/setup is reported separately. This does not claim that every historical crash/restart/snapshot benchmark acquired all these counters. |

Upgrade/downgrade: this correction adds no protocol or storage format. Existing
wire/lifecycle/manifest/snapshot gates remain unchanged. Upgrade every broker
before relying on the recreated-instance replay protection; returning to a
binary containing the replay defect can reproduce it. Existing written-format
downgrade restrictions still apply. Never edit epoch markers, clear shared
offsets, delete live volumes or relax storage/admission checks to recover.

The original unrestricted same-revision promise cannot be retrofitted onto a
revision-only client: it cannot distinguish a newly recreated topic with the
same counter. Preserve that legacy behavior and explicitly scope the safe retry
contract to instance proof and receipt lifetime. Issue closure must account for
this compatibility interpretation and the remaining unmerged dependencies.
