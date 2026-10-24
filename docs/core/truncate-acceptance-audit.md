# Truncate acceptance audit (2026-10-24)

## Decision and revision boundary

**Partial completion; Refs #164, not Fixes #164.** Inspected official main
`9c43d398eecc387ae0011bb7900b54a74a0b0d8d`. The broker already implements
TRUNCATE. This audit adds SDK receipt validation and reconciles documentation;
it does not replace the lifecycle implementation or repeat the cleanup changes.
The original issue's unconditional same-revision retry wording is satisfied only
by the explicitly opted-in, retained instance receipt contract below. The
remaining cleanup integration evidence and measurement limits are listed here
instead of being silently treated as completed requirements.

Issue #164 and dependency #272 were open with no comments at inspection.
The latest completed #272 follow-ups are the #280 fault/recovery verification,
#298 registration-boundary verification and #296 large-record progress work.
Their evidence is reused at its recorded revisions. #302 subsequently adds
opt-in certified history compaction; it does not establish every #272 fault
interleaving. Open PR #311 adds a cooperative consumer runtime and is **not**
part of this audited source. The other open PRs (#161, #217, #218, #248, #249)
are dependency updates.

GitHub reports all the following PRs merged, and their merge commits are
ancestors of the inspected main:

| PR | Merge commit | Relevant contribution |
| --- | --- | --- |
| #168 | `9a4f39b89ed6` | Dedicated failure-safe topic lifecycle |
| #275 | `792f5b759d45` | Shared/pattern groups and reservations |
| #277 | `6b9284231115` | Committed follower cleanup |
| #280 | `aff3c938cb26` | Admission clocks, cleanup faults/status |
| #281 | `169113456188` | Final publication fence |
| #282 | `01114eaea8f2` | Instance-bound durable retry receipt |
| #283 | `ddb1c1bdbfbf` | Snapshot replay coverage and in-flight writes |
| #286 / #287 | `5255e2de964a` / `a49138e7922a` | Durable generation and cleanup continuations/checkpoints |
| #298 | `bb7dc59383c2` | Historical truncate cut and re-registration preservation |
| #296 | `74bdc7a82a95` | Reserved oversized-record progress |
| #297 / #302 | `d843a2997b2b` / `9c43d398eecc` | Disposable replay index / certified history compaction |

The earlier [verification ledger](truncate-contract-recovery.md) remains a
historical record. Its initial open-PR and unsupported-feature statements are
not a current backlog.

## Acceptance matrix before changes

Every implementation location below was inspected at `9c43d398`. Historical
test evidence is identified separately; a test's presence is not a fresh run.

| Acceptance condition | Implementation and tests | Recorded verification revision | Assessment / remaining boundary |
| --- | --- | --- | --- |
| Dedicated reset preserves definition; stale request cannot mutate another instance | `pkg/topic/truncate.go`, `truncate_receipt.go`; `TestTruncateTopicDurableResetsRecordsOffsetsAndProducerState`, `TestTruncateReceiptSurvivesRestartAndPreservesNewRecords`; controller `handleTruncate`; FSM topic lifecycle tests | #282 head `9015660`, #283 head `6aef9f2` | Implemented with instance command. Revision-only legacy requests cannot distinguish same-name recreation with a reused revision. Never present legacy as an instance fence. |
| Same-revision retries are idempotent | `Definition.CheckTruncate`, `TruncateRetryResult`, FSM `truncateReplayResult`, `sdk/admin.go`; receipt restart/snapshot tests, actual response-drop proxy in `test/e2e-cluster/truncate_reply_proxy_test.go` | `9015660`, `6aef9f2`, #280 head `2ed17b7` | Exact instance/input-revision receipt survives normal writes and recovery until a definition revision changes. Legacy completed retry conflicts. SDK checked instance/revision but incompletely validated the success envelope; corrected by this audit. |
| Every standalone durable phase has crash injection | `pkg/topic/truncate_crash_test.go` (11 boundaries), `pkg/transaction/truncate_journal_crash_test.go` (6), `cmd/broker/truncate_journal_recovery_e2e_test.go` (real TCP/startup); `truncate_publication_test.go` | `6aef9f2` | Process-crash coverage and six journal guest-power-cut boundaries exist. Guest cuts do not cover all lifecycle boundaries or physical device failure. |
| Three-node routing, traffic, failover, replay and full snapshots | `TestCommittedConsumerCleanupSurvivesFailoverAndSnapshot`, `truncate_traffic_test.go`, `TestDistributedTruncateRecoveryPreservesConsumerOffsetReplication` | `6aef9f2`, `2ed17b7` | Real leader loss, old-owner return, successful TCP response loss and acknowledged traffic covered. Cleanup fault matrix still has the distinct limits below. |
| No old records, offsets, sequences, transactions or event state after success; preserve new state | Publication tests; process/TCP crash checks; `TestTruncateInstanceRetryPreservesNewGroupAndRejectsRecreation`; `TestTruncateReregistrationConvergesDelayedFollower`; `topic_cleanup_registration_test.go` | `6aef9f2`, #298 head `0ea2372` | #298 adds actual retained-Raft and full-snapshot preservation of new membership/generation/offset 1. It supersedes the earlier claim that only delayed network registration was covered. Missing old snapshot cuts remain fail closed. |
| Admin-only/internal topic and dependency protections | Controller command registry and `topic_lifecycle_test.go`; FSM `TestBrokerFSMTopicTruncateRejectsUnsafeClusterAndActiveState`; coordinator topic lifecycle tests | #275 head `61fbed8`, `6aef9f2`, `0ea2372` | Active/shared/pattern groups, unresolved reservations and open/committing transactions stay protected. Unit evidence is not a complete three-node invalid-record injection matrix. |
| Protocol, API, SDK and reconciler guidance; omission never destructive | `docs/protocol-spec.md`, `docs/sdk-overview.md`, `docs/reference/api-reference.md`, `docs/operations/upgrade-and-recovery.md`; explicit admin-only SDK entry points | source inspection at `9c43d398` | API reference lacked `TRUNCATE_INSTANCE`; format and cleanup limitations were stale. Updated here, including explicit Kubernetes reconciler rules. No implicit deletion/reset is introduced. |

## Retry contract and compatibility decision

| Command | Success response lost | Same input revision after completion | Delete/recreate with reused revision |
| --- | --- | --- | --- |
| Legacy `TRUNCATE`; Go omits `ExpectedInstance` | SDK does not automatically retry an ambiguous response | Conflict; pending standalone work can resume before completion | No instance proof; an old request can match the recreated topic's revision. Use the instance command for this guarantee. |
| `TRUNCATE_INSTANCE`; Go supplies `ExpectedInstance` | Up to `MaxRetries + 1` attempts, exact saved pair, context/request deadlines; no downgrade | Historical success only while current definition revision equals retained truncate revision and epoch matches | Old instance conflicts without mutation |

Receipt storage is constant per topic, not an unbounded operation history. A
later definition mutation or legacy truncate invalidates the old receipt. A
missing receipt cannot be inferred from empty storage, current HWM, or a matching
name. Reads, records and memberships created after a completed reset do not
invalidate it. Replay returns historical `leo=0 hwm=0`, not current offsets.
Never refresh the revision to recover an unknown result: that would request a
different destructive operation.

The SDK now checks topic, instance, output revision, `truncated=true`, a valid
post-reset lifecycle epoch (at least 2), and zero historical LEO/HWM. This is
response-shape validation, not cryptographic proof or a substitute for the
broker's durable receipt. `replayed` may be false when a retry is the first
attempt to reach the broker; `cleanup_pending` may be true. Neither is required
to be false for an acknowledged committed transition. Legacy behavior is
unchanged. Both the intentional legacy limitation and the finite receipt
lifetime must be accepted explicitly before claiming the original issue closed.

## Ordered publication and recovery

Standalone `handleTruncate` runs under the lifecycle admission boundary and
preflights groups and transaction references. The topic manager closes derived
event state, durably replaces the definition manifest with the next revision,
epoch and optional receipt, records pending truncation, replaces partition
storage (including producer dedup and event files), and establishes LEO/HWM zero.
The controller then writes eligible group deletion metadata, rewrites and syncs
the pruned transaction journal and installs that state. Only then does
`CompleteTruncation` sync, replace and directory-sync the local epoch marker.
A storage/group/journal/publication error retains the pending fence. Startup
repeats unfinished cleanup; a completed receipt bypasses cleanup.

Distributed `TOPIC_TRUNCATE` is one serialized Raft decision. It records the
new definition/partition lifecycle and the authoritative consumer metadata cut.
`materializeTopicTruncate` fences/replaces local storage, validates committed
group cleanup against that cut, prunes transaction references, and finally
publishes the local epoch. The responding broker's `cleanup_pending` is not an
all-replica acknowledgement. Distinguish committed intent, responding-broker
completion, and every replica's topic/replica materialization and ISR readiness.

The #298 boundary is historical evidence, not a hint. A snapshot made after
truncate by older code may lack it. Today's HWM cannot recreate that evidence.
Recovery needs retained Raft history containing the transition and preceding
authoritative state, or a snapshot actually carrying the cut. Otherwise remain
pending; do not manufacture tombstones, clear offsets, edit volumes or loosen
admission. A binary upgrade alone does not guarantee recovery of such an incident.

## Kafka comparison and checks defined before the SDK change

Rechecked [DeleteRecords](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/Admin.html),
[accepted KIP-107](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/67636826/KIP-107+Add+deleteRecordsBefore+API+in+AdminClient),
[adopted KIP-98](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
and [adopted KIP-496](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/122915859/KIP-496+Administrative+API+to+delete+consumer+offsets).
Implementation references are pinned to **Kafka 3.9.0**:
[Partition.deleteRecordsOnLeader](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/cluster/Partition.scala),
[ProducerStateManager](https://github.com/apache/kafka/blob/3.9.0/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateManager.java),
[GroupCoordinator.handleDeleteOffsets](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/coordinator/group/GroupCoordinator.scala).

Adopt authoritative routing, bounded client attempts, explicit result validation,
durable progress and subscription/transaction fencing. Kafka advances a log
start and reports a replica low watermark; it does not reset all Cursus state.
Its producer manager distinguishes advancing log start from fully clearing
producer/transaction state. Its offset deletion protects subscribed partitions.
Cursus instead retains topic identity, resets all partitions to zero and resets
eligible consumer/producer/transaction/event state together. Shared/pattern
groups must remain blocked because Cursus does not yet provide Kafka's scoped
offset deletion semantics for this operation.

The SDK change costs a constant number of field comparisons and zero extra
requests. Rejecting malformed acknowledgements favors safety over availability:
it returns an error, not a guessed success or a new reset. Required regressions
are wrong topic, false success, impossible reset epoch, nonzero historical
offsets, wrong revision/instance, bounded exhausted reply loss, unchanged legacy
ambiguity, and genuine broker success-response loss. Recovery tests must verify
fresh records/membership survive replay and account for bytes and repeated work,
not just request counts. Existing durable-phase and cluster tests supply the
broker checks without introducing new cleanup implementation.

## Current cleanup and deployment boundaries

- #296 allows a valid oversized first record through an explicit exclusive
  reservation, bounded by the 64-MiB durable format limit. The ordinary turn
  budget remains 1 MiB. Damaged, truncated, over-limit or uncommitted records
  still fail closed; see [large-record costs](cleanup-record-progress.md).
- #302 implements default-off **certified** consumer history compaction.
  The generic distributed internal-topic compactor remains disabled. Do not
  describe both paths as disabled or treat a disposable replay index as
  deletion authority. See [history compaction](../consumer-history-compaction.md)
  for transaction-frame preservation, local-view requirements and measured cost.
- At this audited source: Wire 2, lifecycle protocol 10, CDM6, standalone
  manifest 4; FSM snapshot 11 or conditional cooperative snapshot 12; local
  Raft recovery marker 13. Receipt capability 7 and cleanup-boundary capability
  8 are minimum feature gates, not the current deployment version.
- Upgrade every registered broker (including offline members) before relying
  on snapshot-persistent truncate cuts. History activation additionally checks
  voters and bootstrap protocol 2. Disabling maintenance does not permit
  downgrade. Preserve immutable complete backups and use isolated restoration
  for a return to an older binary; never edit format markers or live data.

## Executed in this audit

The unchanged broker source at `9c43d398` passed
`TestCommittedConsumerCleanupSurvivesFailoverAndSnapshot` in **357.43 s**
(362.738 s package wall time). This includes image setup, two full all-broker
snapshot restores and teardown, not just recovery latency. The fixture used
private Docker project/ports (`CURSUS_CLEANUP_PORT_OFFSET=32000`); it did not
modify production data or other tests' volumes. It consumed a real successful
broker response before dropping it, retried the exact receipt, elected a new
actual Raft leader, recovered the old owner, and preserved all **25 acknowledged
concurrent records**. Fresh target producer sequence one, new group state and
unrelated event/producer duplicate identities survived. Temporary metadata read
errors remained pending until the fault was removed. This reuses the existing
fixture; no duplicate cleanup implementation or fault harness was introduced.

The [machine-readable phase samples](../benchmarks/truncate-acceptance-2026-10-24.json)
record absolute counters from each process. After failover, the two survivors
decoded 4/5 cleanup records and 2,051/2,436 serialized bytes in 3/4 read attempts;
each also scanned 4,096 retained bytes. After the truncate snapshot restore,
cleanup replay counters were zero but each broker scanned **13,916 retained
bytes**. After the later delete snapshot restore that was 19,334 bytes each.
CPU at the first sample was 9.05/6.52 process-seconds and heap allocation was
473,756,760/455,666,408 bytes. These are process totals/point samples, not
operation deltas or peak memory. The file separately records instrumented
lineage write bytes and sync boundaries when exposed. Missing series are not
measured zeros. Caches were warm, other host work ran concurrently, and counters
must not be subtracted across process restarts. Physical read/write and network
transfer bytes, complete fsync syscall counts and isolated recovery latency were
not measured by this run.

The same malformed Wire-receipt fixture failed on the unmodified SDK for six
cases: wrong topic, `truncated=false`, epoch 0/1 and nonzero LEO/HWM. A wrong
revision already failed correctly. After the fix all seven cases pass, as do
the wrong-instance test, exact lost-reply retry and exhaustion at 0/2 configured
retries (1/3 identical attempts). Legacy ambiguous-reply behavior still passes.
The fixture tests response validation; it is not the genuine broker reply-loss
experiment above. The initial Linux invocation also captured the new tests
before the SDK fix and failed only these same cases; it is retained as baseline
evidence, not reported as a successful suite.

On the final SDK source, Linux Go 1.25.0 race checks pass for topic, transaction,
controller, FSM and SDK tests matching `Truncate|Receipt|ConsumerHistorySnapshotPreservesTruncate`.
This includes the 11 lifecycle and 6 journal subprocess-exit boundaries.
The separate `e2e_faults` real broker TCP journal-crash check passes under race
in **28.494 s**. The complete SDK race suite passes in **5.949 s**; SDK vet and
broker build also pass. The container used two CPUs, a 3-GiB limit and a private
compiler cache; the existing dependency volume was mounted read-only.

The QEMU guest-power-cut evidence is reused from #283, not rerun locally or
relabeled as physical-device testing. The PR's regular
`standalone-guest-power-loss` CI also passed at `3c4139d`; final-head CI results
are tracked separately in the PR. Its six journal boundaries and
ext4/writeback fault model remain documented in `test/power-loss/README.md`.
The #298 paired baseline/fix registration scenarios are likewise reused at
`0ea2372`; this SDK/documentation patch does not alter their cleanup algorithm.

Reproduction:

```sh
go test -race -count=1 ./pkg/topic ./pkg/transaction ./pkg/controller ./pkg/cluster/replication/fsm ./sdk -run 'Truncate|Receipt|ConsumerHistorySnapshotPreservesTruncate'
go test -tags=e2e_faults -race -count=1 ./cmd/broker -run '^TestStandaloneBrokerTruncateJournalCrashRecovery$'
go test -race -count=1 ./sdk
RUN_E2E_CLEANUP_RECOVERY=1 CURSUS_CLEANUP_PORT_OFFSET=32000 go test -v -count=1 -timeout=35m ./test/e2e-cluster -run '^TestCommittedConsumerCleanupSurvivesFailoverAndSnapshot$'
```

## Remaining issue-level acceptance

The measurements above retain their original source commit. At the subsequent
integration point `b754bdd`, main includes #315 (`93896ce`), #316 (`17a196f`)
and #314 (`b754bdd`). The following assessment supersedes the original missing
matrix/profile list without relabeling the earlier measurements.

1. The three-broker rejection and subscription matrix is now supplied by #315,
   including missing, malformed, conflicting, out-of-committed-cut, obsolete
   and transactional evidence. #316 addresses the returning prepared
   coordinator's insufficient-ISR startup failure. See
   [cleanup evidence](cleanup-rejection-evidence.md) and
   [prepared startup recovery](prepared-startup-recovery.md). Reuse #298's
   re-registration/replay/snapshot proof and #302's compaction/restart proof;
   snapshots without a historical cleanup cut still remain fail-closed.
2. The live TRUNCATE profile now measures normal traffic, read/write/transfer
   bytes, replay/repeated work, sync syscalls, CPU, memory and convergence time;
   raw samples and measurement boundaries are recorded in the cleanup evidence.
   It includes instrumentation and foreground traffic and does not isolate
   recovery cost for every process-crash, guest-cut or snapshot-restore path.
   Missing device counters remain unavailable, not zero. #314 addresses repeated
   prefix refinement; see [prefix retry evidence](recovery-prefix-retry.md).
3. Process exit, guest kernel/page-cache loss and physical device power loss
   are different fault models. The historical QEMU journal tests cover only
   the second on a specified ext4/writeback setup; no physical controller,
   torn-sector or arbitrary filesystem-ordering guarantee is claimed.
4. The original unlimited same-revision wording and recreated-name protection
   cannot apply to revision-only legacy callers. Completion needs the explicit
   compatibility interpretation above, rather than silently changing clients.

No issue closure, deployment, merge or auto-merge is performed by this audit.
