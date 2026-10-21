# Generation-fenced recovery acceptance audit

Audit baseline: `cursus-io/cursus` main `6b92842311150c5a8e5a991c3fc8beed5fa19d82`,
2026-10-21. Issue [242](https://github.com/cursus-io/cursus/issues/242) remains open.
GitHub reports PRs 239, 240, 243, 244, 245, 251, 252 and 253 merged; their merge
commits and actual changes were compared with this baseline. Issue
[246](https://github.com/cursus-io/cursus/issues/246) was closed by merged PR 267.
The open-PR audit found dependency updates only, with no competing recovery PR.

## Existing implementation and evidence

| Prior PR | Merge commit | Actual implementation / existing evidence |
| --- | --- | --- |
| 239 | `03f2b27` | Transaction revision barrier before replica append; transaction-state lag regressions. This is not proof of generation-fenced divergent-tail acceptance. |
| 240 | `94ca2ac` | Authenticated replica rollback and restart-persistent recovery HWM; protects the general committed-HWM truncation API. |
| 243 | `37697ae` | Moving source HWM, synchronous ISR proof submission and continuation when the target advances; rolling restart test includes live consumer commits. |
| 244 | `be0aac3` | Repeated late-snapshot recovery finalization throughout manager lifetime. |
| 245 | `24e0703` | Digest-checked one-record rollback for epoch-zero legacy tails; rejects offset-zero underflow. |
| 251 | `0c83f73` | Extends fallback to nonzero epochs when no later boundary exists; real three-broker divergent append, equality, readiness, follower restart and resumed writes already tested. |
| 252 | `f137354` | Durable cumulative lineage sidecars and corruption/publication tests. |
| 253 | `de1cd8a` | Negotiated indexed common-prefix recovery, authenticated proof validation and transfer-work tests. |
| 255–267 | through `bd8e9e4` | Merged 246 implementation: bounded storage/metadata bootstrap, request witnesses, runtime capability activation, retained-prefix exchange, metrics and fault tests. Reuse these mechanisms; do not implement another bootstrap or lineage subsystem. |

| 242 acceptance criterion | Existing code / test evidence | Remaining integrated assertion at audit time |
| --- | --- | --- |
| 1. Offline follower while appends and offset commits continue | `rolling_restart_recovery_test.go`, `indexed_tail_recovery_e2e_test.go` | Exercise live group offsets in the same divergent-tail/leader-change run. |
| 2. Leadership change and delayed old response | Existing indexed-tail test holds authenticated response before metadata/partition locks, elects another leader and releases it. | Retain this exact injection; strengthen no-mutation evidence beyond LEO alone. |
| 3. Restart different uncommitted tail | Existing stopped-broker fixture adds 10,000 legacy records; 251 test produces nonzero-epoch divergence via failed replication. | No new fixture or recovery implementation needed. |
| 4. Fence before mutation; repair only after proof | `WithReplicaRecoveryFence`, receiver witness checks, checked rollback; indexed-tail test observes old-fence rejection. | Compare persisted log bytes before/after rejection; assert excluded ISR and actual repaired boundary. |
| 5. No committed loss, offset skip, duplicate materialization | Exact raw equality in indexed/251 tests; consumer/transaction/bootstrap tests and event replay unit tests. | Include group-offset and producer deduplication checks around this combined recovery and restart. Distinguish ordinary log materialization from aggregate-event indexes. |
| 6. Latest HWM and verified prefixes before ISR | `ValidateISRCatchupProof`, exact LEO/HWM/current-authority tests; existing E2E waits for full ISR. | Advance HWM while the old response is held; verify exclusion before release and exact convergence after recovery. |
| 7. Restart all brokers and compare recovered state | Rolling/quorum recovery and individual follower/crash recovery tests exist. | Restart all three durable volumes after the combined run; compare raw records, offsets and duplicate suppression. |

Implementation presence and historical passing PR reports are not a new run of
the complete acceptance scenario. The table above records gaps before changes;
execution results must be recorded separately below.

## Kafka comparison before implementation

Primary sources:

- [Accepted KIP-101](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=177052956), accepted design, not a claim that every modern implementation detail came from this KIP.
- [Kafka 4.1 replication design](https://kafka.apache.org/41/design/design/#replication), official recovery/durability documentation.
- Kafka **4.1.0** [AbstractFetcherThread](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/AbstractFetcherThread.scala), [LeaderEpochFileCache](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java) and [Partition](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/cluster/Partition.scala), version-pinned implemented behavior.

| Concern | Adopt / retain | Cursus difference and consequence |
| --- | --- | --- |
| Leader epoch | KIP-101 uses epoch history to locate a safe divergence boundary. Kafka's fetcher checks current fetch offset, epoch and fetchable state before processing a response. Retain equivalent pre-mutation fencing. | Cursus also fences topic lifecycle, source ISR membership and receiver storage/materialization witnesses under its Raft metadata authority. Invalid authority fails closed, temporarily reducing availability. |
| Common prefix | Use proven ancestry, not a stale follower HWM, to select the rollback boundary. Kafka's epoch cache returns the next epoch start or LEO for the latest epoch. | Cursus supports same-epoch divergent records and legacy epoch zero; it requires digest/cumulative proofs. Existing 251 fallback handles absent later boundaries. Hashing and replay cost bytes/records even when exchanges are bounded. |
| HWM protection | Preserve authoritative committed history and copy only source-committed ranges. Never infer a safe deletion from follower HWM alone. | Cursus can lower an excluded replica's *local recovery* HWM during authenticated reconciliation (240); that is not permission to delete authoritative committed data. Validate raw committed records and group offsets, not just a scalar HWM. |
| ISR reentry | Kafka 4.1.0 requires an eligible follower at least at leader HWM and current epoch start. | Cursus requires a current-generation verified prefix and exact local LEO/HWM equal to current authoritative HWM plus recent proof. A moving target can require further fetches; availability waits for proof rather than weakening admission. |
| Recovery cost | Avoid replaying a known common prefix unnecessarily; count actual work. | Indexed candidates can conservatively select an earlier *proven* common prefix, not necessarily the maximal mathematical common prefix. Existing 246 metrics measure logical file reads, canonical hashing and transfer bytes; these are not physical device I/O. Cold index reconstruction and retries remain real costs. |

## Regression and fault checks selected before code changes

Extend existing three-broker tests, retaining their fault-only hooks. Record the
old response rejection while subsequent fetches remain paused; compare local
segment digests and offsets and verify ISR exclusion. Continue appends/commits
during downtime and advance HWM under the held response. Require full raw record
equality, monotonically expected consumer offsets and idempotent producer replay.
Restart every broker using the same durable volumes and repeat the comparisons.
Measure existing source/receiver counters before and after recovery, before
verification reads contaminate them; report transferred bytes, logical reads,
decoded records/hash work and replay observations separately from RPC counts.
Reuse the nonzero-epoch test and add only missing integrated/restart assertions.

## Execution results

### Contract gap found by the combined scenario

The initial strengthened run rejected the old-generation response before local
mutation and performed 46 live offset commits while recovering, but failed an
assertion that rollback would stop at end offset 1. The indexed candidate set
for tail 10001 is the tail, distances 1, 3, 7, ..., and anchor 0. End offset 1 is
not offered. The existing protocol can therefore authenticate anchor 0 and
reconstruct the excluded replica, including its previously committed seed.
`ApplyReplicaRollbackChecked` permits this authenticated local reconciliation.

This preserves authoritative committed data and converges to identical records,
but does **not** establish the literal 242 requirement that committed records
are never locally removed/rewound. The scenario now records the actual selected
boundary instead of mistaking the mathematical common prefix for an offered
proof. Closure under that literal requirement remains unsupported. Changing
prefix-selection/committed-boundary semantics would need a separate design
compatible with the already merged 246 protocol; this audit does not silently
change that protocol or treat reconstruction as physical prefix preservation.

The disk package passed a complete Linux race run (84.260 seconds). Focused
Linux race tests for source fetch fences, ISR proof validation, receiver apply
and rollback passed in FSM, command controller and topic packages. The cluster
controller package compiled; that focused regex selected no tests there.
The first lint run reported zero issues. Windows sandbox regression attempts
failed on existing atomic checkpoint/metadata replacement with `Access is denied`;
the Linux runs avoid attributing that environment failure to replication logic.

Local Docker reruns encountered occupied ports from a concurrent cluster test.
The remaining local integration validation uses an isolated copy with only test
ports, Compose project/network and container names changed. Production sources
are compared byte-for-byte with the PR checkout. Workflow branch filters are
unchanged. Full integration results will be appended after that run.

Metrics are process-wide deltas, including background proof work, failed work,
the other recovered partitions and live consumer commits. `lineage/decoded_frames`
counts canonical record hashes, including repeated proof validation;
`retained_scan/decoded_frames` counts decoded scan records (including replay and
validation), not unique application events. A zero scan delta after rollback to
the empty anchor is meaningful: there is no retained prefix to rebuild at that
boundary. Incremental records are still transferred and their final raw range
is checked. These measurements are not physical disk I/O or a claim of minimal
recovery work.
