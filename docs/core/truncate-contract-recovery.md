# Truncate contract and recovery verification

## Baseline and scope

The audit starts at main `6b92842311150c5a8e5a991c3fc8beed5fa19d82`.
Shared-group protection (#275) and committed follower cleanup (#277) are
merged. Issue #272 remains open: its three-broker lost-reply, transient-read
and delayed-registration fault cases are not all covered. Neither those PRs
nor this audit alone establish completion of #164.

## Primary-source comparison (before implementation)

- [Kafka Admin DeleteRecords](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/Admin.html)
  deletes records below a partition offset.
- [Accepted KIP-107](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/67636826/KIP-107+Add+deleteRecordsBefore+API+in+AdminClient)
  defines leader routing, log-start checkpoints and replica low watermarks.
- [Adopted KIP-98](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
  separates producer identity/sequence fencing from transaction decisions.
- [Adopted KIP-496](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/122915859/KIP-496+Administrative+API+to+delete+consumer+offsets)
  defines scoped offset deletion with subscription protection.
- Version-pinned [Kafka 3.9.0 Partition.scala](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/cluster/Partition.scala),
  `deleteRecordsOnLeader`, advances the start offset and returns the low watermark.
- Version-pinned [Kafka 3.9.0 ProducerStateManager.java](https://github.com/apache/kafka/blob/3.9.0/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateManager.java)
  has distinct `onLogStartOffsetIncremented` and `truncateFullyAndStartAt`
  paths. Prefix deletion is not a full producer/transaction reset.

Adopt durable intent, authoritative routing, generation fences and protection
of subscribed groups. Cursus differs: it preserves the definition, advances
revision/epoch, resets every partition to LEO/HWM zero, prunes target-topic
transaction references, removes eligible group offsets and producer sequences,
and resets event indexes/snapshots. Kafka DeleteRecords does not provide this
combined reset contract. Cursus whole-group tombstones still require rejection
of multi-topic/pattern groups and pending reservations; unrelated state survives.

Safety requires the local epoch publication marker to follow **all** successful
storage and dependency cleanup. An error leaves the topic fenced and readiness
unavailable; it must not merely label an already-published topic pending.
This sacrifices availability until durable cleanup can succeed. Retrying a
pending reset can repeat storage work and dependency replay. Count bytes and
records/pages processed, not just requests; snapshot file size is storage size,
not measured physical restore I/O. Never subtract process counters across restart.

## Checks defined before implementation

1. Fail group tombstone and transaction-journal rewrite after the manifest
   transition. Require pending readiness, no public topic access, and no synced
   completion marker. A retry must complete cleanup before publishing.
2. Crash before/after manifest temp sync, rename and directory sync; storage
   removal/recreation and watermark persistence; group tombstones; transaction
   rewrite; marker temp sync, rename and directory sync. Restart must expose
   either the untouched pre-commit generation or a fenced/new generation, never
   old records, offsets, sequences, transaction references, indexes or snapshots
   after a completed reset.
3. A committed result receipt must survive standalone restart, Raft replay and
   snapshot restore. An exact retry must preserve fresh post-reset writes and
   new memberships; unrelated updates or an unproven old revision must not be
   mistaken for that receipt. Same-name recreation must not accept an old
   instance's retry. Legacy records without proof must fail closed.
4. Three brokers: follower routing, concurrent traffic, leader loss, lost reply,
   replay and actual snapshot restoration. Verify committed intent separately
   from each broker's materialization and capture recovery records/bytes/pages.
5. Preserve admin-only/internal-topic guards, shared/pattern subscription and
   reservation protection, terminal transaction state for unrelated topics and
   event/producer retry identity. Desired-list omission performs no mutation.

## Baseline observations

The focused topic/controller/FSM/admin SDK tests pass outside the Windows
sandbox. The sandbox run failed on file replacement and loopback socket access;
those failures are not crash-recovery evidence. Existing tests explicitly expect
conflict after a successful old-revision retry; the admin SDK deliberately does
not replay an ambiguous truncate. This differs from #164's idempotent retry
requirement. The approved follow-up design uses durable result evidence and a
topic-instance boundary, with an explicit protocol/storage compatibility gate.
That design is not implemented by the publication-fence fix.

The group-writer regression fails on the baseline: cleanup returns an error
but `IsTruncationPending` is false. The publication fix retains the fence on
storage, group or journal errors, including startup recovery. Group-write and
journal-replacement failure checks now pass, including recovery retry and
reloading the rewritten transaction journal without the target references.
Focused controller/topic/coordinator/FSM/SDK lifecycle tests pass on Windows
outside the sandbox. These are not the complete process-crash or three-broker
fault matrix described above, and introduce no storage or wire format change.

## Implemented retry contract

The follow-up implementation adds `TRUNCATE_INSTANCE` (Wire ID 61), requiring
the broker-generated topic instance and expected revision. The durable topic
definition retains the latest successful truncate revision and lifecycle epoch.
An exact retry returns that historical result, including `replayed=true`, without
touching records or dependencies created since the reset. A later definition
revision invalidates the receipt; an unproven old revision and a different topic
instance conflict. Delete/recreate cannot reuse the old identity even when its
revision counter starts at one again. Receipt storage is constant per topic.

The original issue's unconditional old-revision retry promise is therefore
**not** retroactively applied to legacy `TRUNCATE`. That command still conflicts
after success. Existing Go callers keep that behavior; setting `ExpectedInstance`
opts into the new command and bounded ambiguous transport retries. No fallback
to the old command is allowed. Legacy unbound definitions acquire identity only
on an explicit CREATE/update, advancing the revision once. An omitted desired
resource is never interpreted as delete, truncate or identity binding.

All active brokers must advertise lifecycle protocol 7 before identities are
bound. Older brokers cannot register while bound definitions exist. Manifest
writer 4 reads version 3, and Raft snapshot writer 11 reads 9/10 for a coordinated
one-way upgrade. Those legacy formats contain no invented receipt. Broker and
Helm deployment contracts reflect the new versions; rollback of written volumes
to old binaries is unsupported. The existing consumer-routing gate stays at
protocol 6, independently of this new capability.

`cleanup_pending` describes local materialization on the responding broker.
Raft commitment and replay acknowledgment do not certify every replica. For
standalone, recovery repeats dependency cleanup before publishing the epoch
marker. A completed receipt replay skips that cleanup and therefore preserves
new active/shared memberships and terminal transaction state.

## Executed checks and measured cost

Extending the live scenario to concurrent writes exposed a recovery stall on
the returning broker: `snapshot_index=42`, `commit_index=42`,
`last_log_index=28`, `applied_index=42`. The replay gate required the physical
log end to cover the committed index even though the installed snapshot already
covered it. Hashicorp Raft v1.7.3's `Stats` reports log and snapshot indexes
separately, and `installSnapshot` restores the FSM before setting the snapshot
index ([pinned source](https://github.com/hashicorp/raft/blob/v1.7.3/raft.go)).
The fix accepts coverage through `max(snapshot_index, last_log_index)` while
still requiring committed tail records, known Raft authority and local-log
validation before clearing the fence. The regression reproduces the stall on
the previous code; a missing committed index 43 remains fenced. A snapshot-only
completion reads zero compacted log entries rather than replaying old history.

Seeding the target with real data exposed a second ordering defect before the
truncate: a live CREATE/UPDATE application used leader-promotion tail repair.
That repair waited for a publish's replication ownership while its HWM command
was queued behind the same FSM application. After the wait failed, stale repair
could remove the pending record. Live definition materialization now applies
only the committed visibility watermark; leadership recovery retains ownership
of destructive tail reconciliation. The disk-backed regression holds a real
replication mutation, reapplies the definition and checks both nonblocking FSM
progress and preservation of the hidden tail before committing it. It fails
against the previous implementation.

The process-crash tests exit the subprocess with code 86, bypassing all deferred
closes, and reopen the actual files in a fresh process. They are process-failure
tests, not simulated power-loss or filesystem write-reordering tests.

| Check | Evidence |
|---|---|
| Standalone lifecycle, 11 boundaries | Manifest temp sync, rename, directory sync; storage removal/recreation; watermark persistence; both cleanup metadata appends; marker temp sync, rename, directory sync. Restart preserves the original generation before manifest commit, or finishes reset without old records/offsets/producer sequences/terminal target references/event index/snapshot resurrection. A fresh same-sequence record and version-one event survive an exact retry. |
| Journal rewrite, 6 boundaries | Temp sync, rename, directory sync, manifest write, manifest sync, manifest directory sync. Reopen and repeat pruning preserves unrelated terminal references and producer epoch floor. These internal boundaries are tested at journal level; controller integration separately tests replacement failure and pending recovery. |
| Receipt regression | Real standalone disk restart; FSM command replay and snapshot restore; same-name recreation and later-definition conflicts; shared-group preservation; internal-topic/admin guards; exact SDK retry after an actual dropped reply and mismatched-receipt rejection. |
| Three brokers, initial recovery run | Actual coordinator movement, stopped old-owner return, dropped successful truncate reply, receipt retry, two all-broker snapshot restores, removed group/offset non-resurrection and preserved unrelated event/producer dedup history. Each restart verifies `FSM restore completed`. |
| Three brokers, extended recovery run | Concurrent acknowledged writes survive the reset and restore. Explicit Raft leader loss elects a different leader; a surviving follower routes the retained-receipt retry successfully. The previously stalled old-owner return completes after the snapshot-coverage fix. This run passed in 379.56 seconds including image build and restarts. |
| Three brokers, strengthened target-data run | Seed an old idempotent target record, truncate, then reuse the same producer ID/epoch/sequence for a fresh record. After actual leader loss, receipt replay and snapshot restore, exactly the fresh record remains. Both the snapshot-coverage and live-materialization fixes are included. This run passed in 313.59 seconds including image build and restarts. |
| Local validation | Linux package tests for `pkg`, `sdk`, `cmd`, `util` pass. Focused topic/transaction/controller/FSM/SDK lifecycle tests pass with `-race`. Native build and repository lint pass. Windows full-suite disk failures involve existing open mapped-file deletion semantics; the Linux run passes those checks. |

The replay-gate regression also passes with the entire replication package under
Linux `-race`. Full GoSec reports zero issues without rule exclusions (the
repository's generated-code directory exclusion is retained). Helm 3.19.0 renders
the charts and passes their positive/negative value tests; the tool archive was
verified against the published SHA-256 checksum.

Measured on the small test fixture, not a scalability benchmark:

- Standalone consumer replay: four partition reads, three to five reconstructed
  metadata messages, 2,173-3,553 JSON message bytes depending on the crash phase.
  This counts serialized messages, not physical device reads.
- Journal restart input: 1,233 bytes before replacement, 802 bytes after it;
  the completed rewrite contains one retained transaction plus its epoch-floor
  record. Restart repeats the target-reference prune without lowering that floor.
- Initial three-broker failover cleanup: surviving nodes replayed four/five
  metadata records (2,054/2,440 bytes) in one/two reads. Their retained scans
  accounted for 3,796 bytes each. After truncate snapshot restart, each broker
  reported 6,536 retained-scan bytes even though cleanup replay records were zero.
  Zero cleanup replay is therefore not zero recovery work.
- Stored snapshot files ranged from 6,868 to 11,818 bytes in that run. These
  are stored file sizes, not measured snapshot restore I/O. Counters are logged
  independently per process; no subtraction across restarts is used.
- With target data and concurrent traffic in the strengthened run, retained
  scans were 4,098 bytes per surviving broker after cleanup, 7,406 bytes per
  broker after truncate snapshot restart, and 12,828 bytes per broker after
  the later delete snapshot restart. Cleanup replay was zero after each
  snapshot restart; the retained scan work was still nonzero.

Reproduce the process faults with
`go test -v ./pkg/topic ./pkg/transaction -run 'Truncate.*ProcessCrash'`.
For the isolated Docker scenario set `RUN_E2E_CLEANUP_RECOVERY=1` and
`CURSUS_CLEANUP_PORT_OFFSET=20000`, then run
`go test -v -count=1 -timeout=35m ./test/e2e-cluster -run '^TestCommittedConsumerCleanupSurvivesFailoverAndSnapshot$'`.
The offset isolates ports, container names, network, image and volumes from an
existing developer cluster; leave it unset for the existing CI fixture.

## Remaining acceptance follow-ups

- Keep #272 open for live three-broker transient metadata-read failures,
  delayed registration during cleanup, and the complete convergence fault
  matrix. Distributed consumer-metadata compaction is still disabled.
- Extend the journal's six internal crash points to a complete broker startup
  fixture and exercise actual power-cut/torn-write behavior on supported
  filesystems. Windows directory-sync behavior is not Linux fsync evidence.
- Run scaled recovery histories and report decoded/read/written bytes and
  replay work alongside latency. The small measurements above establish
  accounting boundaries, not constant-time recovery.
- The retained-receipt contract is bounded by a later definition mutation.
  Legacy calls and retries without instance proof deliberately remain conflicts;
  callers requiring a longer retry horizon need a separately designed operation
  history rather than refreshing the expected revision.

These follow-ups prevent treating #164 as fully complete merely because the
publication fence and opt-in retry contract are implemented.
