# Consumer cleanup fault-recovery checks

## Comparison and checks defined before implementation

[Kafka 3.9.0 GroupMetadataManager](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala)
fences coordinator loading by epoch and rebuilds group/offset state from its
partition log. [Accepted KIP-496](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/122915859/KIP-496+Administrative+API+to+delete+consumer+offsets)
preserves subscription protection during offset deletion. Adopt recovery before
serving a new owner and durable deletion evidence, preserving active-group fences.

Cursus additionally restores coordinator state from its metadata Raft snapshot,
then reconciles the replicated consumer log. A successful restart or a nonzero
snapshot index alone does not demonstrate that a snapshot was restored. Require
an on-disk snapshot covering the completed cleanup, a new restore-completed log
entry on every restarted broker, and successful materialization after recovery.
Do not erase storage to obtain a clean result.

[Kafka's delivery semantics](https://kafka.apache.org/38/design/design/)
deduplicate producer retries by producer identity and sequence.
[Kafka 3.9.0 ProducerStateManager](https://github.com/apache/kafka/blob/3.9.0/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateManager.java)
maintains producer epoch/sequence/offset state and snapshot recovery. Cursus also
has event IDs and aggregate versions: check both exact event retry identity and
ordinary producer sequence deduplication. Matching payloads alone cannot prove
that duplicate records were not appended; verify exact history and replica LEO/HWM.

Regression sequence:

1. Replicate a live target group and unrelated stream/producer records. Restart
   the two nonowners while the group is live, then leave through its owner.
2. Stop the owner. Require all surviving brokers to discover the same different
   coordinator and recover the committed offset before truncating the target.
3. Require cleanup on the surviving brokers, then restart the old owner and
   require convergence on all three. The deleted group must remain absent.
4. Capture snapshots covering cleanup, restart all brokers, prove actual restores,
   and verify deletion. Re-register the target and require its old offset to be
   absent; commit a new offset, leave and delete the topic.
5. Snapshot and restart again. The deleted topic/group must not resurrect.
6. At each fault boundary, read exact unrelated stream history, retry identical
   event IDs and producer sequences, and assert unchanged replica offsets. Fresh
   writes must still work, ruling out an unavailable system as a false pass.

Safety: missing proof remains pending; new tests cannot relax ownership, epoch,
subscription or reservation checks. Availability: a stopped broker temporarily
reduces ISR, and requests wait for a durable replacement owner and usable quorum.
Recovery cost: record cleanup decoded records/serialized bytes/page reads and
replica recovery work counters, plus stored snapshot bytes. Process counters reset
on restart; report stage observations, never subtract counters across restarts or
equate snapshot file size with measured physical I/O. This does not claim to bound
the existing full startup/ownership recovery path.

## Follow-up injection plan (2026-10-21)

Before implementation, rechecked the [official Admin API](https://kafka.apache.org/38/javadoc/org/apache/kafka/clients/admin/Admin.html), adopted KIP-496 and
the version-pinned Kafka 3.9.0 source linked above. `loadGroupsAndOffsets`
rejects stale coordinator epochs; keyed replay distinguishes group and offset
tombstones. `cleanupGroupMetadata` selects partition offsets and deletes group
metadata only for a dead group. Cursus retains its stricter whole-group scope
guard and versioned lifecycle epochs. Kafka's owner-side expiry retry is not
authority for a Cursus follower to expire stale membership. An ambiguous append
reply must be resolved from committed evidence, without another deletion decision.

The remaining integration check will use the dedicated `e2e_faults` build and
runtime gate, with no production fault controls or storage edits:

1. Run the original create/join/follower-restart/owner-leave/truncate sequence
   on current main and the exact original source, recording failures separately.
2. For both truncate and delete, lose the owning coordinator's tombstone writer
   acknowledgement only after the normal replicated append succeeds. Require
   proof that the injection fired and that cleanup does not append a second
   tombstone to resolve the ambiguous result.
3. On a stale follower, fail a bounded metadata page read after at least one
   successful page. Require visible pending reason and retained cursor/work;
   repeated failures must not consume or replay the completed prefix. Remove
   the transient fault and require all three brokers to materialize.
4. Preserve unrelated live membership/generation/offset, heartbeat acceptance,
   exact event history and producer deduplication, and allow fresh writes.
5. Record decoded records, serialized bytes, page attempts and retry work within
   each process lifetime. Do not present these local cleanup counters as a bound
   on startup, ownership recovery or administrative admission's full replay.

Safety keeps ownership, epoch, shared/pattern and reservation fences unchanged.
Availability is intentionally pending while the read fault is held. Recovery
cost should retain completed page progress, adding failed page attempts but no
decoded prefix bytes until the fault is released. These are proposed checks,
not completed results. Live internal metadata compaction remains unsupported;
rollback/compacted-gap/snapshot-replacement unit cases are separate evidence.

The acceptance audit also found that `CLUSTER_STATUS` only serialized a pending
count, despite the cleanup documentation promising per-topic progress. Expose
the existing detached FSM issues as `topic_materialization_issues`, using the
same snapshot for the count. The injection test must inspect this actual wire
field, including its cleanup reason/cursor/work; an internal FSM assertion alone
does not prove operator visibility. No payloads or credentials are added.

## Admission clock preservation

Before this correction, rechecked the official Admin API and adopted KIP-496
above, and [Kafka 3.9.0 GroupCoordinator](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/coordinator/group/GroupCoordinator.scala).
Kafka separates coordinator loading/session initialization from administrative
offset deletion; loading is epoch-fenced in GroupMetadataManager. Adopt that
separation: administrative refresh must not manufacture a new session timeout
or ownership grace period for an unchanged live group. Cursus still performs
its existing full replay for admission, so its scan bytes/work remain unbounded
by the cleanup page budget; this correction changes publication, not that cost.

The admission-only replay policy now carries, under the existing publication lock,
the current heartbeat, last-activity and ownership timestamps only when the
replayed registration epoch, generation, subscription, rebalance and complete
member/assignment identity match. Keep committed offsets/reservations from normal
replay. New lifecycle identities and ordinary startup/ownership/repair recovery
must not inherit the old session clocks. Preserve a heartbeat received during
the scan by copying from the live state at publication, not from a pre-scan copy.
Partition and assignment identity is independent of slice order: validated
snapshots may permute those slices while committed replay canonicalizes them.
An import-and-refresh regression covers legacy and multi-topic groups. The
comparison keeps duplicate multiplicities and never sorts the live state.

The checks defined before implementation cover repeated refresh, real timeout after
refresh, concurrent heartbeat, newer offsets with unchanged session, changed
epoch/generation/assignment, tombstones, read failure and normal ownership
recovery, plus exact three-broker heartbeat equality across admission and
administrative retries while retaining shared/pattern and reservation guards.
Safety comes from identity and existing import-generation fences; availability
does not gain synthetic grace from unrelated admin requests. The added matching
work is linear in already-replayed group membership and performs no extra I/O.

The regression failed before the correction: repeated refresh reset the member
heartbeat, and a heartbeat accepted during a paused scan lost its last-activity
timestamp at publication. `prepareTopicDependencies` now uses
`RefreshConsumerMetadataForAdmission`; ownership/startup recovery retains the
ordinary reload policy. Regression checks also exercise acknowledged reservation
prepare/release progress against a stale scan. The three-broker injection fixture
uses `ELECT_LEADER` on an offset partition different from the cleanup target's
partition, then verifies every broker routes the unrelated group to the Raft
leader. Its exact timestamp check must cross admission replay, including retries
of the administrative command, without moving the cleanup target's owner.

## Snapshot/write exclusion found during integration

The repeat run exposed a commit timeout after snapshot recovery and group
re-registration. Before changing code, the regression is defined as: hold a
durable offset writer open while it owns its group mutex, call the real FSM
Snapshot method, and require prompt refusal; then finish the write and prove a
subsequent persisted/restored snapshot contains the committed offset.

Kafka 3.9.0's `appendForGroup` and `createPutCacheCallback` in the source above
use append completion callbacks to publish group offsets. Cursus keeps its
existing synchronous writer contract. Its metadata snapshot must therefore
avoid waiting for a writer whose partition commit needs the metadata FSM.
[HashiCorp Raft 1.7.3](https://github.com/hashicorp/raft/blob/v1.7.3/fsm.go)
calls Apply and Snapshot on the same goroutine. A waiting snapshot can prevent
that writer from ever completing.

The implemented fix is nonblocking consumer snapshot export: refuse a
contended authority/map/group/provenance lock and let normal snapshot scheduling
retry. Never publish a partial state/proof pair or unlock the offset write fence
early. Check each contention point, detached snapshot equality, the deterministic
writer regression, and the three-broker recovery sequence. Safety keeps the
existing recovery envelope and file format; availability favors Raft progress.
Sustained contention can delay snapshots and increase retained Raft-log/replay
work; this is not a new bound on full recovery cost. Snapshot copying still
scales with all consumer groups and source entries; only lock waiting is avoided.

## Implemented checks and observed results

Validated on 2026-10-21 against main `bd8e9e4` plus this change, with three Linux
brokers built by the repository Dockerfile (Go 1.26.6). Both the original follower
convergence test and the new fault-recovery test passed in one sequential run.
The deterministic snapshot/writer regression failed before the fix and passed
afterward; six lock-contention cases verify no partial export and a detached,
unchanged state/proof pair on retry. Controller, coordinator and FSM race tests,
repository lint/build, and coordinator/FSM GoSec passed after the snapshot fix.
Disk and topic race tests also passed after the main rebase.

The fault test observed ownership move from `broker-3-9000` to `broker-1-9000`
and recovered offset 9 before cleanup. It checked every broker's new restore log
after each full restart, then required complete materialization and stable full
ISR. Re-registration started at offset 0, and the subsequent deletion survived a
second snapshot restart. Final unrelated histories contained exactly four events
and four ordinary messages, with LEO/HWM 4, identical event retry responses and
no extra producer-sequence records. Group absence is checked through each client
seed's owning coordinator; local topic deletion/materialization is checked on
every broker.

Representative observations from that successful run (not performance bounds):

| Phase | Observed work/storage |
| --- | --- |
| Failover cleanup, broker 1 | 4 decoded records, 2,054 serialized bytes, 1 page read |
| Failover cleanup, broker 2 | 5 decoded records, 2,440 serialized bytes, 2 page reads |
| Truncate snapshot restart | 6,536 retained-scan read bytes per new broker process |
| Delete snapshot restart | 12,262 retained-scan read bytes per new broker process |
| Stored snapshot files before truncate restart | 21,523 / 22,071 / 24,850 bytes across retained files on brokers 1/2/3 |
| Stored snapshot files before delete restart | 35,407 / 39,332 / 38,633 bytes across retained files on brokers 1/2/3 |

No counters are subtracted across process restarts, and stored snapshot bytes
are not presented as physical restore I/O. The test also logs the available
lineage, retained-page and recovery work counters at the fault boundaries.

Run the two isolated scenarios sequentially:

```sh
RUN_E2E_CHAOS=1 RUN_E2E_CLEANUP_RECOVERY=1 go test -v -count=1 -timeout=30m ./test/e2e-cluster -run '^TestCommittedConsumerCleanup(ConvergesOnFollowers|SurvivesFailoverAndSnapshot)$'
```

The ordinary E2E workflow includes a separate cleanup fault-recovery job. These
checks alone do not cover lost replies, transient reads or delayed registration
conflicts. The follow-up below adds three-broker acknowledgement/read injection;
delayed registration conflicts still have only unit-level injection. Distributed internal metadata
compaction remains disabled; compacted-gap unit tests do not prove a live
distributed compaction scenario. Oversized cleanup records stay pending, and
startup/ownership recovery keeps its existing full-replay cost.

Review follow-up checks retain the Kafka comparison above: derived replay
cursors are expendable, while committed deletion evidence remains authoritative.
Before changing cache admission, test that a superseded unfinished intent frees
a slot without evicting active unfinished scans or applying its old evidence.
The bounded cache checks at most 64 intent predicates; discarded work may need
re-reading if the intent returns, but does not authorize metadata deletion.
Predicates run in reconciliation, never by acquiring the cleanup lock in Apply.

For snapshot contention, test repeated busy failures and reset after a successful
capture. Export a failure counter and consecutive-busy gauge with an operator
alert; do not back off inside the shared Raft Apply/Snapshot goroutine. This
preserves write availability while making delayed snapshots and growing recovery
work visible. It does not guarantee a snapshot under sustained contention.

## Follow-up results and acceptance audit

The follow-up starts at main `6b92842311150c5a8e5a991c3fc8beed5fa19d82`,
whose tree is identical to the previous branch head `f2e11be`. The original two
three-broker scenarios passed again before changing production code. The new
`TestCommittedConsumerCleanupLostAcknowledgementAndReadFailure` passed both
TRUNCATE and DELETE cases on three separate fault-image brokers. The dedicated
build requires both `e2e_faults` and `CURSUS_E2E_FAULTS=1`; the ordinary build's
hooks are no-ops and have no sentinel-file access.

The acknowledgement injection is at the **coordinator's tombstone writer** after
the normal replicated append returned success. It is not an administrative TCP
response drop or a simulated uncommitted append. A consumed marker proves the
fault fired; a second successful tombstone append would create a duplicate marker
and fail the test. The test holds a page-read error on a nonowner/non-Raft-leader
follower after a successful first page. Both administrative responses report
committed intent with `cleanup_pending=true`. The actual `CLUSTER_STATUS` wire
response retains the cursor and pending reason through repeated failed reads.
Removing the read fault permits every broker to materialize, without restarting
the stuck cluster or modifying durable storage.

Measured follower-2 cleanup work in the successful isolated run:

| Operation | While read fault held | After convergence |
| --- | --- | --- |
| TRUNCATE | 128 records, 57,540 serialized bytes; page attempts 2 to 4; cursor 128, byte position 58,052 unchanged | 146 records, 65,681 bytes, 5 page attempts |
| DELETE | 128 records, 57,548 serialized bytes; page attempts 2 to 4; cursor 128, byte position 58,060 unchanged | 146 records, 65,690 bytes, 5 page attempts |

Thus two additional failed reads consumed no decoded records/serialized bytes
and did not rescan the completed prefix. These are observations, not latency,
physical I/O, transfer-byte or whole-recovery bounds. Record timestamps cause
small byte differences between runs. The test logs the other brokers' cleanup
and available lineage/retained-page/retained-scan counters separately.

Each case checks unrelated generation, membership, assignments, last rebalance
and exact heartbeat timestamps across admission, bounded retries, completion and
administrative retry, then accepts another heartbeat. The live group is owned by
the admission broker to exercise the corrected replay path deterministically.
Offset 140 survives. Exact event history and ordinary messages,
event-ID retries and producer-sequence retries remain unchanged; fresh writes
advance each unrelated stream from one to exactly two records. The truncate
retry does not advance revision 2, and `DELETE if_exists=true` reports
`deleted=false` after convergence. Both cases prove absence of the removed
group through every seed and local completion on every broker.

The status regression fails with the response-field wiring removed (pending
count 1 but no issues on the wire), and passes with the fix. Related controller,
coordinator, topic, FSM, disk and metrics race suites passed; controller,
coordinator and topic tests with `e2e_faults` and the ordinary build passed.
Full-repository lint, tagged fault-package/E2E lint and GoSec also passed.

The cleanup fixture uses host client ports 49001-49003, readiness 49081-49083,
and metrics 49101-49103, with no host replication/discovery exposure. Its network,
containers and volumes remain separate from other E2E work. Earlier bind failures
were environment failures, not cleanup results. The measured injection above
used the earlier 29000 range; a concurrent generation-audit cluster subsequently
occupied it, so the fixture moved to 49000 without stopping that other work.
Seeding valid metadata now sends
heartbeats so an expired test member cannot substitute for the required owner
leave. Every scenario creates a fresh cluster before executing the sequence.

```sh
RUN_E2E_CLEANUP_INJECTION=1 go test -v -count=1 -timeout=25m ./test/e2e-cluster \
  -run '^TestCommittedConsumerCleanupLostAcknowledgementAndReadFailure$'
```

Acceptance evidence remains deliberately split by test level:

| Issue acceptance | Evidence and remaining boundary |
| --- | --- |
| 1: same baseline/fix sequence | Current main passed the fresh create/join/follower-restart/owner-leave/truncate sequence; exact original-source comparison is recorded separately below. |
| 2: active group protection | Coordinator/controller/FSM regressions. The new injection never authorizes deletion through a follower clock. A dedicated three-broker active-group rejection matrix is still outstanding. |
| 3: invalid/uncommitted/old evidence | Unit and disk-backed FSM cases cover missing, malformed, conflicting, wrong key/scope, uncommitted and obsolete records. No claim of three-broker corruption injection. |
| 4: concurrent registration/generation | Unit replay/snapshot replacement and fencing tests; delayed deletion versus concurrent registration has not been injected across three brokers here. |
| 5: lost reply/transient read | Three-broker committed tombstone acknowledgement loss plus retained-cursor read failure pass for both operations. Administrative transport response loss is a separate, untested boundary. |
| 6: recovery boundaries | Three-broker coordinator failover, old-owner return and actual full snapshot restores after truncate/delete pass. Rollback and compacted gaps remain unit evidence; distributed internal metadata compaction is disabled, so there is no supported live compaction scenario to declare passed. |
| 7: unrelated group/scope safety | Admission refresh now preserves heartbeat/activity/ownership clocks for unchanged lifecycle identity; regressions cover real expiration, concurrent heartbeat and ordinary ownership recovery. The three-broker test requires exact heartbeat time across admission and retries, plus membership, generation and offset preservation. Shared/pattern groups and unfinished reservations retain the prerequisite safety regressions. Topic-scoped cleanup of shared groups remains future work, not a whole-group tombstone bypass. |
| 8: unrelated records/dedup | Exact event/message history, duplicate retry identity/offsets and fresh writes pass, including the separate restart/snapshot scenario. |
| 9: bounded progress/status | Page/cursor regressions and the measured injection above; the wire now exposes existing FSM details. A record over 1 MiB remains pending. Cache limits and invalidation tests do not bound total work under endless failures or invalidations. |
| 10: operator/protocol contract | Committed intent, local completion, retry reasons and oversized-record limitation are documented. Full startup, ownership recovery and administrative admission replay remain outside bounded cleanup. |

Internal metadata compaction would require retention-aware lifecycle fencing,
cursor invalidation/exclusion and three-broker crash/recovery proof before it
could be enabled or counted toward acceptance. Unit tests are not that proof.
This follow-up therefore uses **Refs #272**, not an issue-closing reference.

### Exact baseline comparison

The baseline broker image was built from `git archive` of
`35cf766a6e97e0549bbb589cc0b4d4dc40f6595b`, without modifying another checkout.
The same current harness and isolated three-broker topology ran against both
sources: create topic, join, restart each nonowner while membership is live,
leave through the owning coordinator, then truncate. No stopped-cluster restart
was substituted for that sequence.

The baseline reached truncate, then its administrative command timed out.
Broker 2 repeatedly reported `truncate local topic "cleanup-orders": delete
consumer groups for topic "cleanup-orders": topic "cleanup-orders" has active
consumer group "cleanup-workers" with 1 member(s)` and replica materialization
remained pending. This is the stale-follower failure, not a startup failure.
The test failed after 91.47 seconds. Main's identical sequence passed before
this follow-up (89.77 seconds); the existing main cleanup implementation, not
the new status field, supplies the convergence fix.

To repeat with the same harness, extract the baseline to an external directory
and set `CURSUS_CLEANUP_BUILD_CONTEXT` to its absolute path:

```sh
CURSUS_CLEANUP_BUILD_CONTEXT=/absolute/path/to/baseline RUN_E2E_CHAOS=1 \
  go test -v -count=1 -timeout=15m ./test/e2e-cluster \
  -run '^TestCommittedConsumerCleanupConvergesOnFollowers$'
# Unset CURSUS_CLEANUP_BUILD_CONTEXT for the current working source.
```

One subsequent current-source attempt failed before creating the test topic:
broker 2 exited with `bootstrap only works on new clusters` during initial Raft
bootstrap, leaving readiness at `awaiting_cluster_recovery`. Preserve this
separate startup failure in the record; it is neither cleanup success nor a
recurrence of the baseline stale-member failure. It is outside the scoped
cleanup change. A subsequent attempt collided with the concurrently running
generation-audit cluster's host ports before any scenario ran. Neither failed
attempt is counted as a pass.

After moving only the cleanup fixture's host ports, the final fresh-cluster
current-source convergence run passed (191.99 seconds including image build and
teardown). The current-source failover/old-owner/full-snapshot scenario also
passed (121.27 seconds); that subtest passed in the earlier combined invocation
whose overall exit was nonzero because of the separate bootstrap failure.
The injected acknowledgement/read cases and unrelated preservation checks above
are separate successful runs. These durations are test wall times, not recovery
latency benchmarks.

### Existing review and CI evidence

The follow-up PR's first CI attempt found a fixture regression: the shared ISR
metric reader still queried the default exporter port after cleanup's host-port
isolation. On the development host another cluster answered that request; the
isolated CI runner correctly failed with connection refused. Earlier local
results therefore do not prove the target cluster's zero-under-replicated metric,
even though the direct target-broker data/materialization assertions passed.
The reader now applies the same port shift, and an HTTP-server regression checks
the actual request destination. All three cleanup scenarios must be rerun with
this correction before accepting the isolated-cluster evidence.

The follow-up's first CodeRabbit review completed on `a3733c8` with one actionable
comment: bound the two new `docker exec` calls. They now use context deadlines
(30 seconds for marker polling and one minute for sentinel changes), allowing
normal failure reporting and cleanup instead of waiting for the suite timeout.
No additional review was requested.

An unsolicited automatic review of the admission follow-up identified the
snapshot-ordering case above. The legacy import regression reproduced the clock
reset; both legacy and multi-topic import cases pass with the corrected identity
comparison. This was an additional automatic finding, not another requested review.

The corrected-port CI attempt also exposed a production defect: TRUNCATE
admission advanced an unrelated member's heartbeat timestamp by about 118 ms
without a heartbeat from that member. `prepareTopicDependencies` calls full
`ReloadDistributedConsumerMetadata`; replayed lifecycle snapshots reconstruct
member heartbeat times and full replay replaces the ownership-clock map. The
initial evidence-only change at `e5dc8af` distinguished that boundary but did not
fix it. The admission-specific policy above fixes the cause; the test again
requires exact timestamps across the entire operation, including administrative
retries. Actual coordinator ownership recovery still grants its intended grace.

The first admission-fix CI run (`9e039ce`) passed the other 27 checks but failed
the injection fixture before its administrative operation: choosing group names
could not place the live group on the Raft leader because bootstrap had assigned
all internal offset partitions to another broker. The fixture now explicitly
elects the separate offset partition's leader and verifies routing as described
above; it does not weaken timestamp or stale-follower assertions.

The issue had no comments when checked. Prerequisite PR #275 is merged; its
shared/pattern subscription and pending reservation guards are already in main
and are not reimplemented. PRs #276/#277 were also merged before this follow-up.
For #276, CodeRabbit's first attempt was rate-limited, not a completed review;
the three GoSec conversion findings were resolved by `8ee4f83` and their threads
are resolved/outdated. For #277, the first CodeRabbit review covered `f8af608`,
posted two actionable findings, and both were addressed by `f2e11be`; both
threads are resolved. No additional bot review was requested.

The recorded checks at PR heads `8ee4f83` and `f2e11be` passed, as did main
`6b92842`'s Unit Tests, E2E Tests, GoSec and CodeQL workflows. A green CodeRabbit
check labelled "Review rate limited" is not evidence of a new review of
`f2e11be`. These historical remote checks are separate from this follow-up's
local validation; no new PR or remote-head CI result is implied.
