# Consumer history acceptance evidence

The two implementation layers are independently gated and reviewed: the
transaction-aware replay index and format contract, followed by certified
history reclamation. An index hit is not evidence of physical reclamation.
The authoritative retained log must remain sufficient when every derived index
is removed. See the [format contract](consumer-transaction-history-format.md)
and [operational contract](consumer-history-compaction.md).

## Source and validation boundaries

- Foundation head: `78b0bf566b4cd23d2d9317c417fc377654374205`.
- Compaction and ordinary measurement source:
  `7c85be902f8e07c532e918be7d2afc5309ac35f4`, based on official main
  `d5b8d773ad215362c80ee46a2a8764c37ead9728`.
- Replay-crash instrumentation and freshness checks:
  `88b3e7d57a292efc22328574e3b5312cb3e897d7`.
- [Package race CI](https://github.com/cursus-io/cursus/actions/runs/40092762350/job/120868661688)
  tested merge `fd66ae9352e55ca4e6a4aec6f33ea54607bada90` for that head.
  The tests listed below passed in that run unless a different source is
  explicitly stated. All 33 reported checks passed, including bootstrap,
  snapshot restore, deployment, Helm and the dedicated crash experiment.

The local ordinary-broker experiment uses a separate frozen, clean checkout;
later instrumentation/report changes do not silently change its source. Local
Docker timing samples and hosted Linux CI samples are different environments.
The tested merge and PR head `88b3e7d` have the same Git tree
`c017799dadb5e197a58125d7b40cb3ae655d235a`.

An earlier local full-package run exceeded the 20-minute controller/disk
budgets under overlapping I/O and exposed a stale protocol-11 fixture. The
fixture now advertises the transaction protocol constant; focused controller
publication/restore and lifecycle-gate regressions passed locally in 57.112 s.
The current CI package race run passed. The timed-out local run is not counted
as a successful full suite.

## Correctness matrix

| Requirement | Passing regression evidence | Boundary checked |
| --- | --- | --- |
| Exact committed position and current-state suffix | `TestConsumerReplayIndexRealDiskStartupAndSuffix`, `TestConsumerReplayIndexSuffixAndBounds`, `TestConsumerReplayIndexRejectsChangingGeneration` | Partition/topic identity, committed cut and durable generation; no certification from a live mutable projection. |
| Missing, corrupt, incompatible or replaced index | `TestConsumerReplayIndexInvalidation`, `TestConsumerReplayIndexOldSuffixCannotHideCorruption`, `TestConsumerReplayIndexCrossPartitionSuffixFallsBack` | Full retained-log fallback; stale state cannot hide suffix corruption or cross-partition evidence. |
| Index publication crash and failure | `TestConsumerReplayIndexCrashPublication`, `TestConsumerReplayIndexPublicationFailurePreservesPrevious` | Reopen after durable publication stages; retain the prior valid file or fall back safely. |
| Deleted groups and re-registration | `TestConsumerReplayIndexTombstoneAndReRegistration`, `TestConsumerHistoryLifecycleLadder` | Four reused live names and one deleted-name fence; 1,202 / 12,002 / 120,002 input records all reduce to nine retained and replayed records. This is an in-memory semantic fixture, not physical I/O timing. |
| Pending transactions, decisions and provenance | `TestConsumerHistoryPlanPreservesPendingIntentAndDecisionControl`, `TestConsumerHistoryPlanPreservesEpochReservationsAndSources`, `TestConsumerReplayIndexTransactionEvidenceAndDecisionChange` | Pending reservation, epoch, terminal decision and compatibility provenance remain exact. Visibility uses the current committed view. |
| Transaction retirement and intermediate logs | `TestConsumerHistoryRetiredTransactionHistoryBound`, `TestConsumerHistoryTransactionMarkerWaitsForOtherSegments`, `TestConsumerHistorySweepEveryIntermediateReplay`, `TestConsumerHistoryCurrentOpenEpochRemainsProtected` | Superseded v7 evidence requires a committed higher-epoch witness. Controls outlive their intents; open/current epochs remain protected. |
| Ordinary and transactional rewrite crashes | `TestConsumerHistoryDiskCrashStages`, `TestConsumerHistoryTransactionCrashStages`, `TestConsumerHistoryPruneCrashStages` | Nine rewrite durability stages, old-or-new complete publication, repeated reopen without a cache, preserved uncommitted tail and original offsets. |
| Concurrent commits and invalidated certification | `TestConsumerHistoryDiskConcurrentAppend`, `TestConsumerHistorySweepCannotSkipIntentOrAcceptExternalMutation`, `TestConsumerHistoryWorkDetachedAdmissionAndSuffix` | Appended suffixes survive; external replacement invalidates the proof. |
| Bootstrap and snapshot replacement | `TestConsumerHistoryBootstrapPreservesCompactedCommittedState`, `TestConsumerHistoryControllerPublishesAndRestores`, `TestConsumerHistoryRemoteProofRefreshAndReplacement` | Transfer only committed retained state, invalidate old generations, restore exact offsets. The disk bootstrap fixture measures logical local transfer, not network throughput. |
| Unrelated membership and ownership clocks | `TestConsumerHistoryPlanPreservesProducerAndLiveClocks`, `TestConsumerReplayIndexCooperativeOwnershipAndClocks`, `TestConsumerReplayPreservesOnlyUnchangedOwnerSession` | Heartbeat, activity, ownership and cooperative grant/revocation state remain intact for unchanged sessions; recreated registrations cannot inherit old sessions. |
| Repeated replay interruption | `TestConsumerReplayRepeatedProcessCrashDuringScan`, `TestConsumerHistoryReplayCrashResources` | Actual source reads before each interruption, no incomplete index publication, exact final recovery and distinct per-process accounting. |
| Activation and downgrade | `TestConsumerTransactionHistoryActivationAndRestoreGate`, `TestConsumerTransactionHistoryAndCooperativeGatesRemainDistinct` | All-broker protocol gate, snapshot persistence, independent cooperative activation and fail-closed incompatible formats. |

## Real-cluster evidence

The current [history recovery CI](https://github.com/cursus-io/cursus/actions/runs/40092762285/job/120868661507)
passed in 181.78 s. Four fixed groups acknowledged 400 commits in 34.263 s.
Three sequential cache-absent starts reached ready/full ISR in
7.694 / 7.647 / 7.644 s. OS caches were not dropped, and shutdown/cache removal
were outside those restart timers. The fixture verifies physical removal,
exact offsets, ownership movement, DELETE/TRUNCATE, re-registration and the
unrelated live member. This is correctness evidence, not a baseline/fix cold
startup comparison.

The [isolated transactional run](benchmarks/consumer-history-transactions-2026-10-24.txt)
at `7c85be9` passed in 599.06 s. One hundred transaction epochs, including owner
stop/movement/return, took 448.575 s with 207 admission retries. Every replica
met the retained-intent bound, and three cache-absent restarts recovered exact
offset 100. An earlier run overlapping heavy local package I/O failed the
two-minute reclamation deadline with ten intents where eight were allowed;
it is not counted as passing evidence and the assertion was not weakened.

The [RF2 nonreplica run](benchmarks/consumer-history-rf2-2026-10-24.txt) at
`d86c2c1` passed in 261.85 s before the final base refresh. Four hundred commits
took 83.182 s with 1,549 explicit availability retries. Node 3, an actual
nonreplica, reported 5,597 logical metadata transfer bytes after cache-absent
restart. DELETE/TRUNCATE and re-registration preserved the unrelated group.
This historical source demonstrates the remote path; it is not presented as a
current-head network performance comparison. Current package tests separately
exercise remote proof refresh/replacement and controller installation.

The [replay-crash resource report](consumer-history-replay-crash-results.md)
retains every interrupted process's CPU, I/O, records, allocation and sync
costs, survivor interval deltas, final readiness and shutdown-inclusive exit
accounting. Its final restart is warm after partial replay.

## Acceptance limits

Whole-broker 100 / 1,000 / 10,000 baseline/fix results remain a separate required
measurement; passing unit/race tests or a valid index alone cannot substitute
for that evidence. The final report must include retained physical history,
cache-miss recovery, setup and maintenance, all index modes, failover, request
latency and resource costs before claiming the measured workload is complete.

The supported bound is for fixed logical identities and supported formats,
not unlimited creation of distinct deleted group names or producer/transaction
identities. Their authoritative fences remain retained. Legacy protected
prefixes and non-compositional histories have explicitly higher recovery and
certification costs. The one-second tiny-segment stress failure remains part
of the operational evidence; no universal reclamation-latency guarantee or
hardware-cold speedup is established by the current tests.
