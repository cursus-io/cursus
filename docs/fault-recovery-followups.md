# Deterministic fault-recovery follow-ups

The acknowledged-write oracle is the foundation. These follow-ups exercise
commit-application boundaries, transaction decisions, storage failures and
recovery under foreground load. Changes stay local until publication is requested.

## Comparisons and regression contracts, defined before implementation

### Replicated data before local commit application

[Accepted KIP-101](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=177052956)
and [Kafka 4.0.0 ReplicaFetcherThread](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala)
show why a follower's old HWM cannot justify discarding acknowledged records.
Cursus uses push replication and a separate Raft commit command. Pause a selected
follower immediately before applying the exact partition/HWM commit, prove that
its durable log has the record while its local HWM is behind, then kill the
leader and that follower. Restart the follower from its unchanged volume and
require the surviving majority to recover the acknowledged prefix. Finally
restore all replicas and run the client-history/content oracle. The temporary
loss of quorum is expected; success without recovering the prefix is forbidden.

### Transaction coordinator boundaries

[Accepted KIP-98](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
and [Kafka 4.0.0 TransactionCoordinator](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala)
persist prepare decisions before sending markers and fence stale coordinators.
Adopt durable-decision recovery and idempotent retry checks. Cursus persists
transaction state through Raft and supports one fenced consumer offset scope;
Kafka's coordinator log/marker implementation is not copied. Pause after durable
prepare, after participant markers but before the final decision, and after a
durable final decision but before its reply. Kill the coordinator and require
the chosen decision, read-committed visibility and input offsets to converge.
An aborted transaction never becomes visible. Intermediate observations across
partitions are not a cross-partition snapshot-isolation assertion.

### Storage errors

[Completed KIP-112](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/67638402/KIP-112+Handle+disk+failure+for+JBOD)
and [Kafka 4.0.0 LogManager](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/log/LogManager.scala)
take failed storage out of service instead of pretending writes succeeded.
Cursus does not implement Kafka's JBOD directory migration. Its terminal disk
handler and reconciliation fences deliberately sacrifice affected-partition
availability. Inject ENOSPC/EIO at real storage operation boundaries; a failed
write cannot acknowledge success, a subsequent write cannot bypass the failure,
and restarting from retained files must preserve the previously acknowledged
prefix. Operation-boundary injection is not physical power-loss certification.
Publication/rename failures need their own recovery check, not an inferred
guarantee from append or fsync tests.

### Recovery under load

[Kafka replication throttling](https://kafka.apache.org/38/operations/basic-kafka-operations/)
separates bandwidth control from foreground requests. Cursus already has source
and receiver budgets. [Kafka 4.0.0 ReplicaFetcherThread](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala)
records fetched bytes against the replication quota, including traffic from
in-sync replicas; request counts alone are not its bandwidth accounting unit.
Cursus retains its existing fair scheduling, bounded source/receiver workers,
and mixed-load fixture instead of adopting Kafka's fetch-thread implementation.
Extend the existing test rather
than replacing scheduling: interrupt a large recovery, preserve healthy-topic
produce/consume/offset progress and require small-replica recovery. Record data
identities, latency/errors, filesystem read/write bytes, transferred bytes, hashes
and decoded frames. Compare counters only within the same process lifetime.
Small fixtures do not establish constant recovery cost or a production SLO.

## Injection and execution constraints

New deterministic hooks are compiled only with `e2e_faults` and additionally
require `CURSUS_E2E_FAULTS=1`. Production builds use no-op implementations. Hooks
are armed by local test-container files, never public commands. A reached marker
is mandatory evidence. A timed-out hook is an error, not a successful scenario.
Keep the existing production-binary acknowledgement tests unchanged in meaning.
Use dedicated containers/volumes and retain evidence in the external JSONL journal.

## Implementation status

The four scenarios are implemented. **The interrupted mixed-load regression is
failing; this suite is not evidence of Kafka-equivalent reliability.** No assertion
was removed, skipped or converted into an expected failure to obtain a green run.

| Check | Local result on 2026-11-07 |
| --- | --- |
| Follower commit application crash | Passed. Observed LEO=9/HWM=8, killed leader and follower, restarted only the follower first, recovered HWM=9, then verified 10 records on all replicas and through the public consumer after full rejoin. |
| Transaction coordinator crashes | All six commit/abort × prepare/markers/final-decision cases passed. Two output topics, idempotent decision retry, opposite-decision rejection, input offset 1 versus 0, and committed-only SDK reads through survivors and the restarted node. |
| Leader storage errors | ENOSPC before append and EIO before fsync passed. Both rejected the failed write and immediate retry; after leader replacement and retained-volume restart the history oracle verified all 9 records on three replicas and the public consumer. |
| Storage operation/reopen tests | Direct write, synchronous batch and queued synchronous append × ENOSPC/EIO passed with the Linux race detector. An EIO at index rename after compacted-log replacement also passed two successive reopen/repair checks. |
| Interrupted recovery with foreground traffic | Fails the unchanged zero-error/progress requirements. Large receiver crash and source change are observed, small recovery progresses, but consumer offset commits can remain unavailable. See the finding below. |

Normal-build disk, partition-commit and transaction regressions passed with the
Linux race detector, as did tagged faultpoint/storage/recovery-counter tests.
The changed-package static check reported zero issues. Workflow YAML parsed.
Hosted CI has not run: changes and commits remain local. The new CI job requires
explicit passes for all four scenarios and all eight transaction/storage
subtests, so the outstanding mixed-load failure will block it.

## Outstanding finding: consumer metadata during interrupted recovery

The fixture seeds 4,097 large-topic records (4,096 uniquely identified payloads
of more than 8 KiB plus a seed), alongside slow, stalled and small topics.
It pauses broker 3 immediately after a durable catch-up append, saves that
process's work counters, kills it, switches the large topic from broker 2 to
broker 1, and restarts broker 3. Healthy-topic produce, consume and offset commits
run in a 50-second window. The test counts **successful** offset commits separately
from attempts and checks complete record identities after healing source stalls.

Repeated runs exposed availability and bounded-recovery failures:

- One run consumed 147 records, with 11 failed offset commits; small-replica ISR
  recovery was observed at 11,682 ms.
- A subsequent run consumed 213 records, with 202 `coordinator_not_available`
  offset responses and one failed produce request. Offset errors continued from
  4,304 ms through 50,221 ms; small-replica ISR recovery was observed at 15,783 ms.
  Broker logs showed `__consumer_offsets-2` committed HWM 14 ahead of the current
  leader's local LEO 13. These are fixture observations, not production SLOs.
- The final run consumed 127 records, with 110 successful and 17 failed offset
  commits (`coordinator_not_available`, `replication_unavailable` and
  `insufficient_in_sync_replicas`). Produce and consume had no errors. Small ISR
  recovery completed at 17,791 ms. The current leader's internal offset log had
  converged to LEO=HWM=112 by the post-window snapshot, so persistent coordinator
  unavailability is not observed on every run. After removing source stalls,
  `c-large` still failed to regain three-member ISR within the recovery deadline.
  Full post-heal record-identity verification was therefore not reached.

The second run's interrupted receiver snapshot retained 547,846 catch-up transfer
bytes, 1,119 lineage read bytes, 744 lineage write bytes, 459 canonical hash bytes
and 3 lineage decoded frames. These are that process's counters at interruption,
not total recovery cost. JSONL also retains its container start time and `/proc`
I/O; replacement-process totals must not be subtracted from the old process.
Sender/receiver transfer counters must not be summed as a one-way network total.

This does **not** prove that an acknowledged offset was lost. The offset response
ambiguity, the committed-HWM/local-tail mismatch and eventual convergence need
to be resolved together. A suspect path is replication-task error completion
reconciling against a local HWM while a Raft commit outcome is still uncertain;
this is a hypothesis, not an established root cause or an implemented fix.
A follow-up fix needs a deterministic uncertain-commit regression before changing
tail rollback or coordinator admission. Preserve fencing and committed history;
do not solve the availability symptom by accepting writes on an unrecovered log.

## Running and evidence

Do not run this fixture concurrently with the original acknowledged-write tests:
both exclusively own the `cursus-ack-faults` Compose project and shifted ports.

```sh
go test -race -tags=e2e_faults -count=1 ./internal/faultpoint ./pkg/disk ./pkg/metrics \
  -run '^Test(PauseRequiresGateAndExplicitRelease|StorageOperationFailurePreservesAcknowledgedPrefix|CompactionRenameFailureRecoversPublishedLog|RecoveryWorkSnapshotForFault)$'

RUN_E2E_RECOVERY_BOUNDARIES=1 CURSUS_ACK_FAULT_RESULTS="$PWD/recovery-boundary-results" \
  go test -v -count=1 -timeout 28m ./test/e2e-cluster \
  -run '^Test(AcknowledgedWritesSurviveCommitApplicationCrash|TransactionDecisionsSurviveCoordinatorBoundaries|AcknowledgedWritesSurviveStorageErrors|RecoveryBudgetInterruptedMixedLoad)$'
```

The driver builds hook-enabled brokers itself. JSONL preserves each reached
boundary, write results, transaction decision/readback, process counters and
structured foreground errors. The mixed-load JSON holds latency percentiles,
successful commits, errors, small-recovery timing and process resources.
Post-window cluster metadata and internal offsets are captured even when
foreground assertions fail; the test then attempts post-heal integrity checks.
Failure remains a failure throughout evidence collection. Broker logs are
captured before the dedicated containers and volumes are removed.
