# Generation-fenced recovery acceptance audit

Audit baseline: `cursus-io/cursus` main `6b92842311150c5a8e5a991c3fc8beed5fa19d82`,
2026-10-21. Issue [242](https://github.com/cursus-io/cursus/issues/242) remains open.
GitHub reports PRs 239, 240, 243, 244, 245, 251, 252 and 253 merged; their merge
commits and actual changes were compared with this baseline. Issue
[246](https://github.com/cursus-io/cursus/issues/246) was closed by merged PR 267.
The refreshed open-PR audit found this work in PR 279 and dependency updates
only, with no competing recovery PR. The main SHA is unchanged.

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
| Recovery cost | Avoid replaying a known common prefix unnecessarily; count actual work. | Main's sparse candidates can select an earlier proven prefix. This correction refines that interval before mutation, preserving every common record at the cost of logarithmic proof exchanges. Existing 246 metrics measure logical file reads, canonical hashing and transfer bytes; these are not physical device I/O. Cold index reconstruction and retries remain real costs. |

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

### Follow-up design before the safety correction

The follow-up keeps the existing mode-1 wire format, cumulative lineage index,
source matcher and receiver witnesses. Before applying an indexed rollback,
the receiver refines the interval between the greatest matching candidate and
the next nonmatching candidate. Each read-only exchange offers the original
tail, interval endpoints and midpoint. Only an adjacent mismatch authorizes
the final rollback, so no common record (including committed records) is erased
merely because sparse candidates skipped its boundary. Metadata and local
storage witnesses are rechecked while preparing probes and before mutation.

This follows KIP-101's ancestry-before-truncation rule. Unlike Kafka 4.1.0's
epoch search, Cursus must distinguish same-epoch records with cumulative hashes.
It adds at most 64 read-only binary probes per turn for any uint64 interval,
separate from the existing 64 mutation steps and under the same 30-second turn
deadline. Each probe uses the existing bounded index reader; it removes replay of the skipped common
prefix. Cancellation or changed authority leaves the original tail untouched.
This is a boundary-selection correction, not a second bootstrap/lineage system.

Selected regression checks: preserve a common boundary absent from the sparse
set; never invoke apply during refinement; reject changed leader/storage and
cancellation before mutation; retain exact-tail behavior and protocol limits.
The three-broker scenario must now observe `10001 -> 1`, preserve the original
seed, retain stale-response rejection and ISR exclusion, and pass crash/full
restart checks. Re-measure bytes, hashes, replay and exchanges after the change.

### Baseline contract gap found by the combined scenario

The initial strengthened run rejected the old-generation response before local
mutation and performed 46 live offset commits while recovering, but failed an
assertion that rollback would stop at end offset 1. The indexed candidate set
for tail 10001 is the tail, distances 1, 3, 7, ..., and anchor 0. End offset 1 is
not offered. The existing protocol can therefore authenticate anchor 0 and
reconstruct the excluded replica, including its previously committed seed.
`ApplyReplicaRollbackChecked` permits this authenticated local reconciliation.

That baseline preserved authoritative committed data and converged to identical
records, but did **not** establish the literal requirement that committed records
are never locally removed/rewound. The follow-up correction described above
refines the sparse interval before applying the rollback. The integrated test
now requires boundary 1 rather than accepting reconstruction from boundary 0.

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
unchanged. The isolated copy verified 796 production/support files as identical.

### Initial local integration run, before the correction

Both selected three-node scenarios passed (417.369 seconds total):

- `TestIndexedRecoveryBoundsLargeLegacyTailOnThreeNodes`: 306.77 seconds,
  including 10 interleaved offline append/offset-commit batches, HWM advancement
  from 10001 to 10002 under a delayed response, unchanged local segment hashes
  and LEO/HWM after rejection, ISR exclusion, 53 further offset commits during
  recovery, truncate/incremental process crashes, and a restart of all brokers.
  Every raw history record, offset 10002, producer retry and aggregate-event
  retry/history survived. Fresh aggregate version 4 also succeeded after restart.
- `TestReplicaRecoveryWithoutLaterEpochBoundary`: 103.35 seconds, including its
  original nonzero source-epoch assertions, follower restart, the added complete
  cluster restart, exact raw equality and producer retry with no extra offset.

The actual indexed repair log was `start=10001 current=0 target=10002` under
leader epoch 2. It took 17 exchanges including failed turns. The incremental
authoritative range reconstructed was `[0,10002)` (10002 unique records), including
the seed at offset 0. This is the concrete counterexample to claiming that this
fixture physically preserves the local committed prefix during repair.

Work deltas sampled before comparison reads and subsequent process crashes:

| Broker | Lineage logical read bytes | Canonically hashed records | Canonical hash bytes | Catch-up response bytes | Retained replay/validation scan records |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 7,571 | 1 | 183 | 0 | 0 |
| 2 (receiver) | 4,706,657 | 23,935 | 4,257,937 | 4,776,823 | 0 |
| 3 (new leader) | 1,772,829 | 10,768 | 1,915,694 | 0 | 0 |
| Total | 6,487,057 | 34,704 | 6,173,814 | 4,776,823 | 0 |

The repeated counter sample later in the test includes another 1,024 receiver
read bytes from background work; the table uses the single delimited measurement
window. Source and receiver cold/warm index state differs, so these figures are
an observed workload, not a general complexity bound.

Repeat through the existing CI jobs, or locally with Docker:

```sh
RUN_E2E_RETAINED_BOOTSTRAP=1 RUN_E2E_CHAOS=1 go test -v -count=1 -timeout 30m ./test/e2e-cluster -run '^Test(IndexedRecoveryBoundsLargeLegacyTailOnThreeNodes|ReplicaRecoveryWithoutLaterEpochBoundary)$'
```

The initial CodeRabbit review found one actionable problem: the new full-cluster
restart helper needed to honor `CURSUS_E2E_CONTAINER_PREFIX`. The helper now does
so. The isolated run used the same prefixed targets via its endpoint-only test
copy; final CI exercises the repository's default configuration. No additional
bot review was requested.

Metrics are process-wide deltas, including background proof work, failed work,
the other recovered partitions and live consumer commits. `lineage/decoded_frames`
counts canonical record hashes, including repeated proof validation;
`retained_scan/decoded_frames` counts decoded scan records (including replay and
validation), not unique application events. A zero scan delta after rollback to
the empty anchor is meaningful: there is no retained prefix to rebuild at that
boundary. Incremental records are still transferred and their final raw range
is checked. These measurements are not physical disk I/O or a claim of minimal
recovery work.

### Follow-up correction and integrated acceptance

The corrected three-broker indexed scenario passed in **187.17 seconds**.
The authenticated repair was `start=10001 current=1 target=10002` at leader
epoch 2: 12 exchanges in the successful repair turn, 46 exchanges including
failed/held turns and subsequent data catch-up. The original committed seed
remained below the truncation boundary; only `[1,10001)` was discarded, then
`[1,10002)` was fetched (10001 unique authoritative records). The retained-state
rebuild decoded one record / 140 logical bytes, rather than rebuilding from an
empty log. These are distinct from the transfer and hashing counts below.

| Broker | Lineage logical read bytes | Canonically hashed records | Canonical hash bytes | Catch-up response bytes | Retained scan records / bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 (previous leader) | 2,158,993 | 11,533 | 2,050,612 | 0 | 0 / 0 |
| 2 (receiver) | 4,798,312 | 22,505 | 4,009,119 | 4,796,874 | 1 / 140 |
| 3 (new leader) | 2,495,241 | 11,563 | 2,056,340 | 0 | 0 / 0 |
| Total | 9,452,546 | 45,601 | 8,116,071 | 4,796,874 | 1 / 140 |

The one-record common prefix makes saved data replay small in this fixture;
the extra proof reads and exchanges are a measured safety cost, not an efficiency
improvement claimed from request count alone. Index warmth, background work and
live commit counts differ from the baseline run, so these are observations,
not a controlled latency benchmark. Regression cases also locate noncandidate
common prefixes at 17 and 64 and preserve every common record.

| Acceptance criterion | Corrected integrated evidence |
| --- | --- |
| Offline append and offset traffic | 10 interleaved append/commit batches while the follower was stopped; one commit while the old response was held and 49 more during recovery. |
| Previous generation rejected before mutation | Old response released after leader/HWM change; rejection observed with identical persisted segment hashes, LEO and HWM while fresh application remained paused. |
| Divergent suffix only | 10000 divergent records; exact common boundary 1 found before the single rollback. Unit/controller tests prohibit applying a coarse sparse rollback. |
| Committed data, offsets and materialization | Original seed matches in every persisted field; all source records converge; group offset 10002 and producer/aggregate retries remain idempotent. |
| Latest HWM and ISR admission | Receiver absent from both survivors' ISR under target 10002; eventual readiness observation is 503 while held; full ISR only after catch-up to 10002 and raw-prefix equality. |
| Crash and durable restart | Crashes after durable truncation and incremental append recover; stopping all brokers before restarting any preserves all 10004 raw records, group offset 10002 and duplicate suppression. Fresh aggregate version 4 succeeds. |

Readiness is sampled by the existing observation worker. The first follow-up
attempt exposed a test timing error: an immediate HTTP assertion read the prior
successful sample after HWM advanced. The test now waits up to 10 seconds for
503 while recovery remains held, without changing the product readiness rules
or weakening the direct ISR and no-mutation assertions.

Final focused Linux race regressions passed in replication FSM (14.478s),
cluster controller (2.728s), command controller (4.099s), topic (7.929s) and disk
(15.446s). Lint reported zero issues. The isolated final integration copy had
799 production/support files byte-identical to the checkout; only test endpoints
and Compose names differed. No workflow branch exception was introduced.

`TestReplicaRecoveryWithoutLaterEpochBoundary` also passed in the final run
(81.64s), including nonzero-epoch/no-later-boundary recovery, complete cluster
restart, exact raw equality and an idempotent producer retry. Both scenarios
completed together in 274.083s. The local implementation and integrated fault
acceptance evidence now support completion of the seven criteria; upstream
integration remains subject to final CI and user-owned review/merge. This work
does not merge the PR or close the issue automatically.
