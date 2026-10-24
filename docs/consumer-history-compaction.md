# Consumer history compaction

## Remaining-work design and regression contract

The follow-up compares against Apache Kafka release commit
`13f70256db3c994c590e5d262a7cc50b9e973204` (4.1.0), specifically
`LogCleaner.java` and `OffsetMetadataManager.java`, and accepted KIP-848.
Kafka protects producer continuity and retains a transaction marker until its
associated batches are gone. Cursus must additionally prove that old consumer
intents cannot still be materialized and preserve registration epochs,
reservations, decisions and physical provenance. Neither Kafka's retained-log
replay nor KIP-848 provides a persistent Cursus current-state snapshot.

The extension uses version-7 intents and the stable `txn-history:<id>:<decision>`
control producer, enabled only by a second durable protocol gate. A committed
higher producer epoch for the same transaction ID and producer is the retirement
witness; **mutable transaction-manager state is never deletion authority**. The
new epoch must have a physical intent inside the certified committed cut. Both
the retired and witness intents must be version 7. Version-3 history predates
some materialization guards and is deliberately retained as a finite upgrade
prefix, not retroactively certified by newer code.

Initialization holds the per-ID lock through old reservation release, committed
offset materialization and Raft acknowledgement. SEND_OFFSETS uses the same lock.
A later version-7 physical intent therefore proves an earlier version-7 epoch
cannot still materialize offsets. Valid, consistent terminal controls are also
required. Pending/current epochs, conflicting controls and producer recovery
witnesses survive. Controls are removed only after all their intents are absent
or removed by the same atomic segment publication. Control sequence numbers are
the positive coordinator epoch; ownership movement advances sequence without
creating another producer ID. Old control identities remain protected.

A whole closed-prefix certificate can be reused for an ordered segment sweep
only when every target group's ordinary state has one physical source, each semantic key advances
epoch/revision, and lifecycle formats stay constant within an epoch. The retained
final witnesses then preserve **every intermediate state**, not just final replay
equivalence. Other histories use the conservative single-segment path. Each
publication reacquires admission/FSM/storage fences and validates all identities;
only its own successful rewrite can advance the certificate generation. Scanning
uses the monitor's cancellation context, avoiding a fixed one-minute deadline
that could restart a large scan forever. Each individual admission acquisition
still has a 250 ms bound. When the FSM fence was never entered, publication can
retry the existing certificate up to 16 times, releasing admission and waiting
100 ms between attempts. A failure after entry discards the certificate. Metrics
include successful earlier segments even when a later segment defers. No live membership or ownership clock is imported.

Version-7 transactional intents may reside on another physical partition. They
do not supply materialized offset state and use the separate retirement proof;
the final intent preserves that source's provenance and producer continuity.
Cross-partition transaction tests replay after every single-frame publication,
including each gap between intent and control removal. Ordinary multi-source
histories still require conservative single-segment certification.

Nonreplica partitions use the existing budgeted detached-transfer cache, with
raw reads and immutable adopted-prefix generation. Before publication, refresh
source certificates outside local admission/FSM locks and revalidate topology,
source/requester incarnations and the fixed committed vector. Replacement or
certificate loss changes the adopted identity and discards the proof. This does
not turn a remote transfer cache into a normal-startup index.

Before enabling deletion, regression checks cover committed/aborted epochs,
pending/current epochs, legacy records, missing/conflicting/late controls,
producer changes, exact cuts, restore, cross-segment controls, repeated recovery,
and abrupt process exit at all nine replacement durability stages. Compare full
state, physical provenance and raw producer/control recovery. Whole-broker
measurements remain separate from these correctness tests.

## Implementation and rollout contract

Official main `9c43d398eecc387ae0011bb7900b54a74a0b0d8d` contains the large-record, durable append-time,
cooperative ownership/recovery and recovery scheduling changes. They are incorporated through the
existing PRs. Production metadata
uses producer ID `consumer-offset-coordinator`; fixtures must use that ID too.
Keep both the last physical producer record and the maximum positive
epoch/sequence/offset record used by partition recovery. Kafka 4.1.0 LogCleaner
likewise retains producer continuity, but Cursus must preserve its own recovery
ordering rather than assume physical order implies sequence order.

The maintenance path captures a detached compatibility baseline without closing
consumer admission, certifies ordinary fixed read views outside live admission,
then acquires a new lease and FSM fence to install a generation-checked closed
segment. Ordinary suffix appends may advance the committed end; prefix changes,
restore, lifecycle or compatibility changes invalidate the work. No live state
is installed and membership clocks are never rewritten. Empty interior segments
are removed using the durable deletion protocol; the first logical-start anchor
and active tail remain. Sweep no-op ranges still validate the certificate before advancing to the next range.

Before activation, test real producer-bearing records, non-monotonic producer
epochs/sequences, changed source generations, canceled work, concurrent suffix
appends, storage replacement failures, and recovery without the derived index.
Measure certification outside admission separately from publication's request
pause. Transactional frames must remain protected until their visibility and
producer/control state have an equivalent certified representation.

## Scope and comparison before implementation

The implementation base is official main
`9c43d398eecc387ae0011bb7900b54a74a0b0d8d`. Its distributed internal-topic
compaction guard remains necessary. The recovery index is disposable and must
never authorize deleting authoritative records.

The primary-source comparison in [the recovery index design](consumer-recovery-index.md)
also applies here: Kafka 4.1.0 LogCleaner protects transaction and producer
state and rewrites closed segments. KIP-848 does not supply a Cursus snapshot
format. Cursus additionally validates epoch/revision conflicts and preserves
physical metadata provenance across partition routing changes.

Certification therefore starts from the complete committed log and the proven
Raft compatibility baseline, independently of the index and live coordinator.
For one candidate range, retain the latest record per semantic key **per physical
partition**, plus the maximum positive epoch/sequence/offset and last physical
record for every producer. Registration and tombstone share
a key; offset snapshots use a key per topic; lifecycle and reservation snapshots
have separate keys. Keeping a representative in every physical partition retains
provenance without inventing a new on-disk record. Tombstones have no time-based
expiry. Superseded registration epochs may be removed only when replay of the
actual proposed remainder proves the same deleted-group fence and live state.

Full replay must succeed before selection. Replay the candidate remainder and
compare the entire exported state and observed source sets. An exact, detached
removal list is bound to all partition identities and committed bounds, the
exclusive lease and compatibility generation. Raw and committed scans must
agree for ordinary records. Legacy batches, inferred registrations, orphan
future epochs and unrecognized physical transaction formats are rejected.
Legacy/current transaction intents and their controls are preserved. Version-7
retirement follows the additional proof above, including invisible aborted
intents. Transaction evidence never authorizes removing ordinary snapshots. Completed transactions materialize ordinary offset
snapshots separately; those snapshots may be compacted after replay equivalence.
Reservation and decision snapshots remain ordinary metadata and are compared in
full. Certification never installs replayed state or changes membership clocks.

Cooperative metadata v6 is a complete lifecycle snapshot: retain its member
sessions, current grants, target assignments and pending revocation revisions.
The certificate compares those fields as part of the entire recovered state.
Regression coverage repeatedly removes/rejoins the same member name, compacts
superseded lifecycle records, then recovers without a cache and rejects an eager
downgrade. The activation flag also survives the conditional cooperative Raft
snapshot v12; restoring that snapshot still rejects protocol-9 registration.

The unit of atomic replacement is one closed segment, not a whole log: certify
the range that will actually be replaced. Applying a whole-log deletion plan
one segment at a time would require proving every intermediate replay state.
Storage must reject a changed generation and must retain the active/uncommitted
tail, original offsets, topic/partition identity and producer continuity.

## Tests specified before implementation

- Fixed live groups and partitions with 100, 1,000 and 10,000 revisions; report
  certification cost separately from subsequent replay and real file reduction.
- Cache absent/corrupt/populated must produce the same deletion decision.
- Tombstone, re-registration, legacy multi-key batches, cross-partition old
  revisions, lifecycle, pending reservations and recorded decisions.
- Source replacement, committed-end movement, authority changes and released
  recovery leases invalidate certification; neither failed nor successful
  certification changes unrelated members, activity or ownership clocks.
- Disk replacement faults before source generation invalidation, after marker
  publication, after log replacement and after index replacement. Reopen and
  replay both possible durable outcomes; no recovery cache is required.
- Producer recovery witnesses, unresolved/current transaction records and
  uncommitted tails survive. Bootstrap and snapshot replacement invalidate plans.

## Format, upgrade and downgrade

`consumer.history.compaction.enabled` (flag `--consumer-history-compaction`,
environment `CONSUMER_HISTORY_COMPACTION_ENABLED`, default `false`) requests
durable activation. It is independent of `consumer.recovery.index.enabled`.
The leader commits `CONSUMER_HISTORY_ACTIVATE` only after every registered
broker, including offline brokers and current voters, advertises lifecycle
protocol **10**, bootstrap recovery protocol **2** and a durable incarnation.
Activation is stored in the Raft FSM snapshot. Registration rejects older
capabilities afterward; voter admission already requires registration with the
current protocol. Turning the flag off stops maintenance, not the durable gate.

The leader then commits `CONSUMER_TRANSACTION_HISTORY_ACTIVATE` only when every
registered broker and voter supports lifecycle protocol **11**. The second flag
is persisted in snapshot **13**; restoring a flag with an older snapshot version,
missing ordinary history gate or incapable broker is rejected. Writers emit
metadata v7 and stable history controls only after that gate. Before activation,
writers retain v3 and existing control identities. New binaries read both.

The local Raft recovery marker is **14**. Supported marker-9 through marker-13
volumes are replayed first, then atomically upgraded and synced before capability
advertisement. Older binaries reject marker 14; active snapshot 13 also fails
closed. Unactivated snapshots keep the existing version 11/12 contract. Turning
configuration off stops maintenance but does not reverse either durable gate or
new writer format. In-place downgrade is unsupported. Restore a complete
pre-upgrade backup into isolated volumes; never edit a marker or activation flag.

Replay index v2 is **derived**, not authoritative. It stores selected ordinary
records, compatibility/source provenance and all certified raw v7 transaction
evidence. Reuse resolves each cached intent against the current committed read
view, so a formerly pending decision cannot be cached as permanently invisible.
Reservations and decisions remain complete ordinary snapshots. V1 ordinary-only
indexes remain readable; a legacy or incompatible transactional range falls back
to full replay. A lost/corrupt/unsupported index changes recovery work, not state.
The 32 MiB cache limit remains an explicit fallback boundary. Transaction evidence
cost is proportional to retained transactions, not discarded ordinary revisions.

No retention or large-record functionality is duplicated. The large-record issue
is closed; retention issue #292 remains open. Only the already merged durable
broker-time implementation (PR #300, commit `2260df1`) is a prerequisite. No
unmerged retention follow-up or its remaining acceptance evidence is assumed.

Removing historical revisions necessarily removes the ability to diagnose
conflicts with those discarded revisions later. Activation accepts that
validation boundary and rejects pre-activation writers. The ordinary distributed
internal-topic compactor remains blocked. Only the separate certified path may
rewrite a closed, fully committed range with all replicas caught up and eligible.
All metadata partitions must have fixed certified coverage, either local views
or existing detached transfers. Missing or stale coverage defers publication.

## Recovery and cost contract of this layer

The certificate is process-local and deliberately not serializable. Restart,
restore, bootstrap replacement, a regressed committed end or changed authority
generation requires fresh certification. Ordinary committed suffix growth is
allowed after detached scanning. It is never authoritative state.
The proposed retained records are original log frames, with their existing
version and physical offset. Authoritative recovery uses retained records plus
the existing Raft compatibility baseline. Neither the certificate nor the
disposable recovery index is required after a crash or for replica bootstrap.
Compaction invalidates the index's durable generation before publication, so a
stale cache falls back to retained-log replay. Corrupt authoritative records
remain an error, never a reason to trust a cache.

The checked disk installer stages and syncs log, index and gap marker, then
invalidates the durable source generation, publishes/syncs the marker, replaces
log then index, and syncs the directory. It honors cancellation while the
replacement files remain private. Once durable generation
invalidation begins, it completes publication even if the maintenance deadline
expires; real storage failures still fence writes until recovery. Cancellation
at each of the nine durable stages is checked for immediate write availability,
unchanged committed offsets and cache-free reopen. Reopen accepts either the original log
or its fully replaced log; existing startup index reconstruction handles the
window with a replaced log and old index. The tests kill a subprocess at each
boundary. They exercise process crashes, not host power loss or every filesystem
reordering. Existing distributed internal-topic and application compaction gates
are unchanged.

Pruning an empty interior segment first validates the preceding unmarked
segment's contiguous end, publishes and syncs its gap marker, then uses the
durable segment deletion protocol. Deleting the empty base extends that
predecessor's logical range. Without the earlier marker, strict startup and
bootstrap scans would correctly reject an unexplained trailing gap. Crash tests
cover both the predecessor-marker boundary and completed empty-segment deletion.

The recovery scheduler from the updated main continues to own bootstrap/transfer
admission. This maintenance path uses ordinary local read views and its own
interval/deadline; it does not add a remote transfer or another scheduler.
Linux race regressions cover the consumer bootstrap staging and installation
slot ordering together with replay and compaction. Native Windows bootstrap
fixtures can reject a low-space host before exercising recovery (the default
5% reserve required 24.6 GB on the validation host). The same checks pass in the
isolated Linux filesystem; the production reserve is not lowered to run them.

The current certification deliberately replays all partitions twice and scans
the raw committed records once **per candidate segment**. A filtered page can
end before its consumed source page: the sequential candidate reader retains
the consumed position separately from the last returned offset, so removed
trailing records are not read again within that pass. This cursor lives only
inside one certification; an error or retry starts from the original bounds.
It is not a durable checkpoint or an authority to skip validation. Scanning does
not hold a live recovery lease; publication does and includes storage reads and
fsync. Maintenance processes one segment per configured compaction interval with
a one-minute attempt deadline. This bounds each attempt but does not prove that
maintenance keeps up with an arbitrary append rate. Transaction intent/control
frames remain protected. Distinct deleted-group fences and reservation-decision
state can still grow: this is not a bound on every consumer metadata workload.

Observe `cursus_consumer_history_runs_total{result}` for completed/deferred/skipped
attempts, `cursus_consumer_history_phase_seconds{phase}` for capture/certify/publish
wall time, `cursus_consumer_history_source_records_total` and
`cursus_consumer_history_source_payload_bytes_total` for repeated scan work,
and `cursus_consumer_history_removed_records_total` and
`cursus_consumer_history_removed_frame_bytes_total` for removal. Payload/frame
bytes are logical counters, not device I/O or replica transfer. Deferred work is
logged with its reason; continuous deferral means history is not being bounded.

The diagnostic counters in the isolated measurements distinguish source
key/payload bytes, rewrite frame bytes, temporary file sizes and kernel physical
I/O. Source/rewrite sync counters count instrumented successful boundaries; they
are not a complete syscall trace (identity creation and reopen work are outside
those counters). Transfer is zero because the fixture is local. Whole broker
startup/failover and cold OS-cache recovery remain unmeasured acceptance
conditions. The cluster correctness and request-latency sample below is separate.

## Earlier certificate-only measurements (2026-10-23)

These historical measurements used producerless fixtures and the private rewrite
helper before production integration. They are not measurements of the current
controller or evidence of production startup improvement. The updated fixture
uses the real metadata producer identity and must be measured independently.

Go 1.25.0, Linux Docker, four CPUs and 3 GiB memory, network disabled; four
groups, four partitions. Each partition has one large closed segment and an
unchanged active uncommitted tail. Initial construction/appends are excluded.
`full` starts with already-open disk handlers; `reopen_full` includes opening
new handlers and full recovery, with no cache file. Neither drops the OS cache.
These are single samples on a shared host, not a cold-start speedup claim.

| Revisions/group | Full replay ms | Certification + rewrite ms | Reopen + replay ms | Retained log bytes before → after | Full → retained replay records |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 256.10 | 1,011.59 | 82.46 | 151,844 → 3,416 | 404 → 8 |
| 1,000 | 250.51 | 4,046.83 | 60.94 | 1,509,052 → 3,424 | 4,004 → 8 |
| 10,000 | 2,649.57 | 38,776.34 | 680.16 | 15,153,060 → 3,432 | 40,004 → 8 |

At 10,000 revisions, certification/rewrite consumed 36,025.37 ms CPU,
2,229,646,408 allocated bytes and 77,599,360 sampled peak heap bytes. It read
304,158 source records / 79,308,330 key+payload bytes plus 15,152,524 rewrite
frame bytes; removed 39,996 records; wrote 3,172 instrumented file bytes and
81,920 kernel-accounted physical bytes. The temporary `.compacting` files peaked
at 761 bytes. Sixteen source sync and 24 rewrite sync boundaries were observed;
identity creation/reopen syncs are not included in these counters.

All samples reported zero physical read bytes because OS caches were warm;
transfer bytes were zero because there was no network recovery. Full/reopen CPU
at 10,000 was 3,482.42/229.17 ms and sampled peak heap was
49,076,768/5,437,536 bytes. Kernel counters include process work during the
measurement but do not measure underlying device/controller write amplification.
Per-mode allocations and all raw values are in
[the JSONL artifact](benchmarks/consumer-history-linux-2026-10-23.jsonl).

Windows tests also exercised 100/1,000/10,000 histories, cache invalidation and
cache removal, nine subprocess crash boundaries and concurrent physical append
past the committed cut. The core coordinator/disk Linux race run passed. A
Windows attempt to run existing ordinary-compaction tests hit the host's free
space guard (about 9.8 GB available versus a 24.6 GB threshold); the same
ordinary-compaction regressions passed in the isolated Linux race run. No
production guard or existing test was weakened for that host limitation.

## Updated producer-bearing measurements and integration

The current fixture uses `consumer-offset-coordinator`, four fixed groups and
four partitions, with the same unchanged active/uncommitted tail. Linux
Go 1.25.0, four CPUs/3 GiB, no network, one sample per size on a shared host:

| Revisions/group | Full replay ms | Certification + rewrite ms | Reopen + replay ms | Log bytes before → after | Replay records before → after |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 61.43 | 1,153.15 | 36.95 | 162,752 → 3,632 | 404 → 8 |
| 1,000 | 89.51 | 2,525.67 | 49.05 | 1,617,160 → 3,640 | 4,004 → 8 |
| 10,000 | 814.75 | 11,291.80 | 41.59 | 16,233,168 → 3,648 | 40,004 → 8 |

These are warm OS-cache storage measurements, not whole broker startup. Setup
and closing old handlers are excluded; opening handlers is included in the
reopen mode. At 10,000 revisions, certification/rewrite costs 13,971.26 ms CPU,
2,857,210,224 allocated bytes, 79,915,544 sampled peak heap bytes, 304,158 source
records/79,308,330 key+payload bytes plus 16,232,632 rewrite frame bytes. It
observes 16 source-sync and 24 rewrite-sync boundaries, 3,388 temporary/output
file bytes, 81,920 kernel write bytes and 815 peak temporary bytes. These sync
counts are partial instrumentation, not a syscall trace; this one-segment
fixture does not exercise the extra predecessor-marker publication. Physical
read bytes and network transfer are zero. See [raw measurements](benchmarks/consumer-history-producer-linux-2026-10-23.jsonl).

A separate run requests `FADV_DONTNEED` on **only the fixture's synced log/index
files**, before timed reopen. It never drops host-wide caches. Two CPUs/2 GiB:

| Revisions/group | Baseline reopen ms | Compacted reopen ms | Kernel read bytes before → after |
| ---: | ---: | ---: | ---: |
| 100 | 953.21 | 157.28 | 200,704 → 49,152 |
| 1,000 | 109.52 | 43.30 | 1,703,936 → 49,152 |
| 10,000 | 1,358.10 | 93.65 | 16,908,288 → 49,152 |

Eviction is advisory; nonzero kernel read counters establish that these runs
actually read storage pages. They do not establish a completely cold broker,
filesystem/device cache, Raft replay or network recovery. The non-monotonic small
sample wall times show shared-host noise. Full CPU/heap/write/repeated-work data
is in [the separate artifact](benchmarks/consumer-history-evicted-linux-2026-10-23.jsonl).
Reproduce with `CURSUS_HISTORY_MEASURE=1 CURSUS_HISTORY_EVICT_FILES=1 go test
./pkg/disk -run '^TestConsumerHistoryDiskMeasurement$' -count=1 -v`.

The checked controller path additionally validates activation, producer-bearing
compaction, bounded empty-segment count, cache removal and real FSM snapshot
restore. Retained-bootstrap tests install compacted committed ranges into
different storage, invalidate the receiver generation and reopen without a
recovery cache. Publication faults cover nine segment-rewrite and two
empty-segment-pruning process-crash boundaries. A queued recovery during JOIN
must return the generation captured from the durable lifecycle candidate;
a second admitted getter must not fabricate a successful generation-zero reply.

An isolated three-broker integration run passed at source `f301a0a` in 428.901 s
(package elapsed time, including Docker image build, cluster setup and cleanup;
Go compilation before the package run is excluded). Four groups committed 400
offset updates in 71.786 s. Commit completion p50/p95/max were
164.218/273.896/439.695 ms, including retries; 388 availability retries were
counted across commit and heartbeat commands. Each broker was limited to one CPU
and 1 GiB. Every replica removed at least 20 records. TRUNCATE and DELETE followed
by recreation reset the target offsets while the unrelated member retained its
generation, heartbeat admission and offset 17.

That run removed the derived cache immediately before each running-container
restart; restart-to-ready/full-ISR was 12.279/9.779/8.760 s. Because the process
could theoretically republish the cache before stopping, these timings alone do
not certify a cache miss. The test now stops the broker first, removes and checks
the cache through an isolated helper using the exact existing image/volume, then
times start-to-ready/full-ISR. Shutdown and cache removal are excluded from that
new timing. Neither sequence drops OS caches. Broker CPU/peak heap/device I/O,
transfer bytes and total fsync counts were not measured in this cluster sample;
the local-storage measurements above must not be substituted for them.

That integration revision required all metadata partitions to have ordinary
local committed coverage. The current extension adds separately certified
nonreplica sources as described above. Remote transfer caches still do not
become current-state startup snapshots.

Those results did not validate whole cold broker startup/failover across the
history ladder, transactional intent/control history reclamation, maintenance
throughput or compaction without complete local metadata coverage. The current
follow-up must validate these separately. Retaining necessary deleted-name fences
or decisions is authoritative state growth, and is not bounded by live-group
count alone. Do not close the history-growth issue based only on these results.

## Sequential certification reads (2026-10-24)

The follow-up baseline is official main `9c43d39`, which includes both recovery
index and ordinary history compaction. Kafka comparison was checked again
against the **4.1.0 release commit**
[`13f70256db3c994c590e5d262a7cc50b9e973204`](https://github.com/apache/kafka/tree/13f70256db3c994c590e5d262a7cc50b9e973204).
Its [LogCleaner](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaner.java)
builds an offset map before rewriting segments, excludes the unstable tail,
and retains producer continuity and transaction markers while needed. Adopt
sequential progress through a fixed source view; retain Cursus's full state and
physical-provenance comparison instead of replacing it with key-only cleaning.
The accepted KIP remains a protocol design, not this implementation's persistent
snapshot or deletion proof.

Before changing the reader, a four-partition coordinator fixture (one populated
group) with 100/1,000/10,000 offset revisions read 303/3,003/31,026 source records.
The last case reread 1,023 filtered records. After separating source consumption
from returned offsets, the counts are 303/3,003/30,003: one full committed scan,
one raw scan and one filtered scan. These are decoded logical record counts,
not physical I/O or startup timings. The fixture uses in-memory source pages;
initial history construction is outside this comparison.

Regression checks cover short pages of 1/3/17/1,024 records, existing offset
gaps, retained producer witnesses between removed runs, an unchanged suffix,
and a deadline failure in the candidate pass followed by a fresh retry. Exact
offset recovery and the existing tombstone/epoch, reservation, cooperative
ownership and source-generation rejection checks still apply. No on-disk format,
activation gate, upgrade/downgrade rule or fallback behavior changes.

This removes redundant page work, **not** the complete-history-per-segment
algorithm. It does not establish a maintenance throughput bound, enable remote
coverage, reclaim transaction history or measure cold broker startup/failover.
In particular, transaction deletion must additionally preserve partition raw
recovery's producer fences and unresolved boundaries; equality of coordinator
state alone is insufficient. The distributed internal compaction guard remains
in place.

The real-disk comparison uses baseline `9c43d39` and source `7e91c69`, Linux
Go 1.25.0, two CPUs/3 GiB and no container network. Four groups/four partitions
remain fixed. Each source is measured once on a shared host; another race run
overlapped the beginning of the baseline. OS caches are warm, initial history
construction and closing old handlers are excluded, and reopening handlers is
included in `reopen_full`. The [raw JSONL](benchmarks/consumer-history-scan-linux-2026-10-24.jsonl)
records all modes and both revisions, including the slower samples.

| Revisions/group | Certification source records baseline → fix | Source key/payload bytes baseline → fix | Retained log bytes before → after (both) |
| ---: | ---: | ---: | ---: |
| 100 | 3,066 → 3,066 | 785,982 → 785,982 | 162,752 → 3,632 |
| 1,000 | 30,066 → 30,066 | 7,779,078 → 7,779,078 | 1,617,160 → 3,640 |
| 10,000 | 304,158 → 300,066 | 79,308,330 → 78,249,174 | 16,233,168 → 3,648 |

At 10,000 revisions, certification/rewrite wall time was 12,469.89 → 33,695.86 ms,
CPU 16,464.93 → 19,395.06 ms, sampled peak heap 78,351,872 → 79,792,144 bytes,
and allocations 2,857,618,464 → 2,853,238,048 bytes. Cache-free warm reopen was
67.71 → 650.87 ms. These noisy single samples **do not demonstrate a wall-time,
CPU or cold recovery speedup**; the verified improvement is fewer duplicate
decoded records/bytes. The certification cost still dominates this workload.

Both 10,000-revision runs removed 39,996 records and recovered eight retained
records. They read 16,232,632 rewrite frame bytes, wrote 3,388 instrumented file
bytes and 81,920 kernel-accounted bytes, and used at most 815 temporary bytes
in the measured per-segment rewrite. Both observed 16 source sync and 24 rewrite
sync boundaries (partial instrumentation, not all fsync syscalls). Physical
read bytes and transfer bytes were zero. These local figures exclude network,
Raft replay, full broker startup and request latency; the existing cluster E2E
and its CI result remain separate correctness evidence.

## Verified cache-absent cluster restart

The strengthened fixture passed in [CI run 39917076635](https://github.com/cursus-io/cursus/actions/runs/39917076635)
at `e4f2c280c9328c51ef9d30b661994b5632e5e596`, before the cooperative-base refresh.
Every broker was stopped before removing and checking the absence of its derived
index. Start-to-ready/full-ISR was 7.511/7.516/7.505 s. Four groups performed 400
commits in 33.128 s, with completion p50/p95/max 81.609/106.885/142.713 ms and
29 availability retries across commit/heartbeat. The E2E test elapsed 160.94 s,
including image setup and cleanup but excluding pre-test Go compilation.
Exact offsets, replica history removal and unrelated member state passed.

This is a cache-absent correctness sample with warm OS caches, not a baseline/fix
comparison of cold startup or a measurement of the newly rebased head. Shutdown
and cache removal are excluded from the restart timer. Broker CPU, peak heap,
physical I/O, network transfer and complete fsync counts were not collected.
The full history ladder and resource measurements remain required separately.

## Whole-broker experiment

Publication temporarily closes consumer admission. If it starts between command
preflight and coordinator routing, routing preserves the same retryable
`consumer_metadata_recovering` response. The tests count these retries in request
completion latency; they do not rejoin members or retry epoch/ownership failures.

Ordered sweeps leave an admission-free interval between segment publications:
at least 25 ms and at least the elapsed preceding publication time. The next
publication revalidates the certificate after that interval. Background passes
wait the configured interval after completion, rather than immediately consuming
an overdue ticker event. This limits consecutive maintenance exclusion, including
slow fsyncs, without weakening committed-cut or generation checks. It does not
promise a maximum syscall latency or eliminate cross-broker contention.

Publication must also acquire the partition reconciliation and visibility gates
nonblockingly while holding the FSM fence. Replication owns a shared mutation
gate until its Raft decision applies; waiting for that gate under the FSM fence
deadlocks the decision that would release it. A busy partition has not entered
the installer: release admission/FSM, back off, and retry the same certificate
with every identity and committed-cut check repeated. Validation/storage errors
after entry still discard the plan. A real three-node transaction test exposed
this lock cycle; spacing publications alone did not fix it.

Cleanup on a nonreplica stages the missing committed raw prefixes through the
same bounded, certified transfer cache as recovery, before taking coordinator
lifecycle/global locks. Local read callbacks fence FSM transitions and validate
topic incarnation, topology, committed cuts and source/requester incarnations.
The final materialization check performs only nonblocking in-memory validation.
These remote proofs are ephemeral: they never become a durable cleanup
checkpoint or startup index. Replacement, restore, rollback or process/topology
changes require fresh evidence. No cleanup view imports the complete group state
or renews unrelated membership clocks. If membership changes between bounded
passes, the deletion target is refreshed so its generation fence can converge.

`cursus_recovery_work_total{stage="consumer_replay"}` counts every source-adapter
page and returned record/key-payload byte, including partial errors and repeated
attempts. It excludes in-memory projection reads and maintenance certification,
which has separate counters. Raw reads count returned uncommitted tails as work,
never as certified state. `stage="consumer_index"` counts actual cache file
read/write bytes even when parsing or publication fails. These cumulative counters
also distinguish completed preparations that hit or miss the index; whole-broker
experiments assert the requested path actually occurred on each broker. The
maintenance experiment probes ordinary offset reads without changing the seeded
history and reports completion latency (including SDK retries), successes and
returned errors separately. These cumulative counters
must accompany the last-recovery gauges; a later hit does not erase earlier work.
Underlying detached/file counters can overlap these logical layers and must not
be summed as physical I/O. Process counters reset on restart; interrupted runs
must retain each process sample separately to claim cumulative resource totals.

`stage="consumer_frames"` measures the bounded internal-metadata range parser's
actual mmap read return bytes and successfully decoded frames, including length
headers rejected by the byte budget, records preceding the requested offset,
partial failures and repeated reads. It includes recovery, ordinary metadata
fetches and maintenance using that parser; it does not cover every disk read
path. These are logical frame bytes, separate from replay key/payload bytes and
Linux guest physical I/O. Counters are aggregated once per parser call.

`RUN_E2E_CONSUMER_HISTORY_MEASURE=1 CURSUS_CLEANUP_PORT_OFFSET=34000`
with `CURSUS_HISTORY_MEASURE_OUTPUT` set to a new JSONL file runs
`TestConsumerHistoryWholeBrokerLadder`. It requires Docker and uses only that
fixture's project, network, shifted ports and volumes. The default ladder is
100/1,000/10,000 acknowledged revisions per each of four groups. The baseline
is the same binary with maintenance disabled; initial setup, seeding, maintenance
including restart, individual startup modes and coordinator failover are separate
rows. Existing historical commit comparisons above remain a different experiment.

The test-only image traces fsync/fdatasync with strace's seccomp BPF filter and samples the broker's `/proc` CPU,
RSS and I/O counters and exported Go heap. Physical `read_bytes`/`write_bytes`
are distinct from logical rchar/wchar, replay payload bytes and transfer metrics.
Go's cumulative allocated bytes/allocation counts and non-loopback container
interface bytes are recorded too. Interface totals include control/replication
and monitoring traffic; they are not recovery payload bytes.
Heap and temporary/store space peaks are sampled at 1 s, not exact maxima.
Tracing/sampling overhead remains in wall time; tracer/helper CPU and writes are
not counted as broker-process I/O. Per-file DONTNEED is advisory, restricted to
stopped fixture files; valid-index runs retain warm OS caches. Shutdown and
explicit cache preparation are outside startup timers. Failover rows include
survivor before/after counters. Interrupted-start rows currently expose the last
process only, so their resource totals cannot represent all interrupted work.
The experiment header records the source head, working-diff digest and tracing
configuration. Earlier failed pilots did not use the seccomp filter and are not
comparable timing samples. Both baseline and fix use the same instrumentation.

Retained bytes are metadata `.log` bytes; total store bytes also include Raft,
indexes and checkpoints and may include sparse logical file sizes. Unique group
IDs, tombstone epoch fences and producer/transaction identities are not expired
by this change: fixed live-group count alone is not a bound on unbounded creation
of distinct identities. Legacy protected prefixes and workloads that fail the
subsequence proof retain their explicitly documented extra recovery/scan costs.
