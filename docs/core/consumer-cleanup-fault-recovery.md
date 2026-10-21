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
checks do not complete all broader issue acceptance: lost replies/transient read
failures and delayed registration conflicts still have unit-level injection,
not equivalent three-broker injection coverage. Distributed internal metadata
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
