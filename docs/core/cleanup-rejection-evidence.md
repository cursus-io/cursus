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
and submitted separately in PR 314. No history compaction implementation from PR 313 is
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

## Reproduction and evidence boundaries

The production correction was tested at
`9b657f11893b8b7f66740a139b41e43ef3cc2c89` (official main plus this change).
`TestHistoricalTruncateCannotResetRecreatedInstance` fails on the original
truncate and restore paths and passes with the identity guard. The unchanged
`TestColdTruncateReplayPreservesRecreatedTopic` also fails for both log-only
and snapshot-plus-log replay when copied onto `011ed0f` (main plus only the
independent prefix retry fix); both variants pass with deferred opening.
`TestHistoricalTruncateReplayPreservesRecreatedTopic` covers the warm registry.
The Linux Go 1.25 race run passed the complete `pkg/topic`, replication FSM and
Raft replication packages (146.819 s, 95.937 s and 26.876 s respectively).
These are process-level test durations, not recovery latency measurements.

The prepared-reservation fixture deliberately keeps output minISR=3 with one
replica unavailable. The readiness reporter repeats its topology failure in
the aggregate `observation` check. The fixture accepts that aggregate only when
it exactly equals the separately reported topology failure; stale observations,
timeouts and every other dependency failure remain rejected. Full readiness
and full ISR are required again after all brokers return. This is a correction
to the test's interpretation of the current health response, not an admission
or durability change.

Run the isolated broker fixtures with an unused port offset and disposable
test volumes. Never point these compose projects at an operational data path:

```sh
RUN_E2E_CLEANUP_REJECTION=1 CURSUS_CLEANUP_PORT_OFFSET=33000 \
  go test -v -count=1 -timeout 20m ./test/e2e-cluster \
  -run '^(TestConsumerCleanupRejectsInvalidEvidenceOnThreeBrokers|TestTopicLifecycleRejectsSubscriptionsOnThreeBrokers)$'
RUN_E2E_CLEANUP_PROFILE=1 CURSUS_CLEANUP_PORT_OFFSET=33000 \
  CURSUS_CLEANUP_PROFILE_OUTPUT=/tmp/cleanup-profile.json \
  go test -v -count=1 -timeout 15m ./test/e2e-cluster \
  -run '^TestTruncateRecoveryCostWithForegroundTraffic$'
python3 test/e2e-cluster/analyze_cleanup_profile.py /tmp/cleanup-profile.json
RUN_E2E_CHAOS=1 CURSUS_CLEANUP_PORT_OFFSET=33000 \
  go test -v -count=1 -timeout 15m ./test/e2e-cluster \
  -run '^TestPreparedOffsetsRecoverAfterMemberDepartureAndCoordinatorFailure$'
```

Process I/O is `/proc/1/io`, while cgroup CPU and block accounting include the
test tracer and collection processes. Network counters include replication,
normal traffic, readiness and metrics requests. Per-node samples are sequential,
not simultaneous: preserve their start/completion timestamps and the wider
trace windows. RSS/heap peaks are sampled, and `VmHWM` is a process-lifetime
high-water mark. Cgroup v1 without a blkio controller reports unavailable block
accounting, not zero bytes. Kernel-accounted writes do not measure physical
media write amplification or prove power-loss persistence. These controls make
the live-operation profile auditable without assigning every byte to TRUNCATE.

## Completed three-broker evidence

[CI at `9b657f1`](https://github.com/cursus-io/cursus/actions/runs/40073952746/job/120813208596)
passed the exact-current-epoch, exact-rejection-reason matrix (258.24 s), all
three subscription modes (111.07 s), the live cost profile (65.28 s) and the
departed-member prepared-reservation/coordinator-failure scenario (33.37 s).
These durations include fixture work; use the phase timestamps below for
recovery latency. Every executable CI check passed at this commit. An automatic
CodeRabbit status reported a review rate limit, which is not a completed review.

The negative matrix verifies that the selected follower actually observed each
fault, kept its pending cleanup fence, rejected three successive offset reads,
and recovered after removing the hook and restarting. The out-of-cut case is
an uncommitted *read response*, not an actual uncommitted Raft append. Topic
recreation between cases also checks that the fresh record survives startup.
An unrelated active membership, record and producer sequence survive every case.
Subscription guards run through all three seeds; prepared reservations are
checked through both surviving seeds while the third replica is intentionally
offline. No successful cleanup test is counted as rejection evidence.

## Full live-operation cost

Raw samples and independently reproducible summaries are retained for both
runs at production commit `9b657f1`:

- CI Ubuntu, cgroup v2: [raw](../benchmarks/cleanup-live-ci-2026-10-24.json),
  [summary](../benchmarks/cleanup-live-ci-summary-2026-10-24.json).
- Windows host / Docker Desktop Linux, cgroup v1:
  [raw](../benchmarks/cleanup-live-desktop-2026-10-24.json),
  [summary](../benchmarks/cleanup-live-desktop-summary-2026-10-24.json).

Each broker was limited to 2 CPUs / 1536 MiB in the profiling compose overlay.
Both broker images use Go 1.25.0; the CI driver uses Go 1.26.2 and the Windows
driver uses Go 1.25.0. Raw-file SHA-256 values (before any newline conversion):
CI `d4ee0ea6f516598552e256ae1274fd085647fdb950cfbd55122e9e3148d50158`;
Desktop `84055868fd28399949cb8d2a455cac81831fc8c4530eacaaa99702deca159ea2`.
The fault image adds `strace`; its ptrace capability is confined to this fixture.
The workload creates 100 target records and 256 pre-profile offset commits,
restarts a follower before measurement, then holds its cleanup read fault until
at least three failed attempts. Concurrent successful iterations each publish a
record with `acks=all`, commit its offset and heartbeat an unrelated membership.
All acknowledged payloads, final offset, membership identity and generation are
verified after convergence; a new target write restarts at producer sequence 1.

| Phase / useful work | CI | Docker Desktop |
|---|---:|---:|
| Setup, outside operation | 25.486 s | 244.393 s |
| Request to response (responding broker completion, not all replicas) | 0.019 s | 0.170 s |
| Request to fault-release start | 16.460 s | 27.530 s |
| Fault-release start to all replicas materialized | 19.465 s | 17.895 s |
| Entire live operation | 35.926 s | 45.426 s |
| Acknowledged publish/commit/heartbeat iterations | 163 | 82 |
| Iteration latency p50 / p99 / maximum | 201.682 / 240.864 / 241.844 ms | 516.474 / 798.957 / 974.432 ms |

CI per-node deltas below span the raw before/after sample windows, slightly
wider than the operation. Sync counts span their separately recorded trace
windows. Do not add network receive and transmit as distinct useful payloads,
or add process and cgroup write accounting as distinct writes.

| Counter | Broker 1 | Broker 2 (held follower) | Broker 3 |
|---|---:|---:|---:|
| Cleanup serialized bytes / records | 14,111 / 32 | 72,714 / 169 | 4,771 / 10 |
| Cleanup read attempts / scan initializations-or-invalidations | 4 / 1 | 6 / 1 | 2 / 1 |
| Process `rchar` / `wchar` bytes | 1,123,255 / 9,295,429 | 1,003,172 / 9,006,122 | 818,591 / 9,422,898 |
| Process kernel-accounted read / write bytes | 0 / 14,581,760 | 0 / 14,581,760 | 0 / 14,577,664 |
| Cgroup block read / write bytes | 0 / 10,563,584 | 0 / 10,244,096 | 0 / 10,588,160 |
| `eth0` received / transmitted bytes | 1,495,481 / 2,842,713 | 1,077,525 / 2,151,968 | 1,167,966 / 2,972,248 |
| Traced `fsync` / `fdatasync` calls (zero errors) | 2,884 / 656 | 2,885 / 656 | 3,205 / 656 |
| Broker process CPU / cgroup CPU seconds | 5.970 / 10.430 | 4.470 / 7.875 | 6.150 / 10.837 |
| Sampled peak RSS / Go allocated heap bytes | 138,739,712 / 365,358,080 | 121,430,016 / 348,464,888 | 123,985,920 / 430,498,560 |

Zero kernel reads in the CI run reflect warm cache accounting, not zero replay:
the cleanup counters report **91,596 serialized bytes and 211 record visits**.
Read attempts include the three injected failures, and the invalidation counter
also counts initial scan creation. Appends during the scan can require another
bounded pass before the proof applies; that repeated work and normal traffic
remain included. Aggregate background lineage failure counters increased by
443 / 447 / 448; their generic classification includes rebuild-required
refusals, so these are not counts of corrupt records or authenticated prefix
searches. No absent label is silently converted to zero by the analyzer.

The local run measured 606,208 process read bytes and 16,015,360 process write
bytes; cgroup block accounting was unavailable. Both runs include tracing and
collection overhead and are observations, not throughput or recovery SLOs.
The Desktop host also ran independent test workloads; it was not a dedicated
performance host. Use the separately retained CI run when comparing environments.
The profile covers the entire live cleanup operation, not an unmeasured device
power-loss recovery or an all-history snapshot export. Previously recorded
process-crash, full snapshot and QEMU guest-cut evidence remains separate.

## Completion decision

The remaining three-broker rejection and live-operation cost evidence for
cleanup is now present, and the recreated-instance replay defect has a paired
baseline/fix regression. An additional local run exposed a startup-order gap:
after the reservation checks passed, the returning coordinator exited from
`RecoverPreparedTransactions` with `insufficient_in_sync_replicas current=2
required=3`, preventing full ISR recovery. This run is a failure, not superseded
by the passing CI timing. A prior local attempt also timed out negotiating the
group-owner connection before LEAVE; the diagnostic rerun passed that point.
The dependent [startup retry correction](prepared-startup-recovery.md) now has
deterministic baseline/fix tests, a passing full server race suite and a passing
rerun of the same real-broker scenario. The acceptance reconciliation still requires the
unmerged SDK receipt validation in PR 312 and the independent prefix retry fix
in PR 314 to be delivered alongside this correction. The original unlimited
same-revision wording must explicitly retain the legacy compatibility exception
described above. Therefore this change uses `Refs`, not a whole-issue closing
reference. Merging remains an operator/reviewer decision; a bot rate-limit
success does not satisfy the initial-review requirement.
