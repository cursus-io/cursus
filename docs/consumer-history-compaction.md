# Consumer history compaction

## Implementation and rollout contract

Official main `2260df1` now contains the large-record and durable append-time
changes. They are incorporated through the existing PRs. Production metadata
uses producer ID `consumer-offset-coordinator`; fixtures must use that ID too.
Keep both the last physical producer record and the maximum positive
epoch/sequence/offset record used by partition recovery. Kafka 4.1.0 LogCleaner
likewise retains producer continuity, but Cursus must preserve its own recovery
ordering rather than assume physical order implies sequence order.

The maintenance path captures a detached compatibility baseline without closing
consumer admission, certifies ordinary fixed read views outside live admission,
then acquire a new lease and FSM fence to install a generation-checked closed
segment. Ordinary suffix appends may advance the committed end; prefix changes,
restore, lifecycle or compatibility changes invalidate the work. No live state
is installed and membership clocks are never rewritten. Empty interior segments
are removed using the durable deletion protocol; the first logical-start anchor
and active tail remain. No-op candidates do not acquire a publication lease.

Before activation, test real producer-bearing records, non-monotonic producer
epochs/sequences, changed source generations, canceled work, concurrent suffix
appends, storage replacement failures, and recovery without the derived index.
Measure certification outside admission separately from publication's request
pause. Transactional frames must remain protected until their visibility and
producer/control state have an equivalent certified representation.

## Scope and comparison before implementation

The implementation base is official main
`2260df1`. Its distributed internal-topic
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
Versioned transaction intents and validated control frames are preserved in
full, including invisible pending/aborted transactions. They never authorize
removing ordinary snapshots. Completed transactions materialize ordinary offset
snapshots separately; those snapshots may be compacted after replay equivalence.
Reservation and decision snapshots remain ordinary metadata and are compared in
full. Certification never installs replayed state or changes membership clocks.

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
- Producer/control/transaction records and uncommitted tails are never selected
  for deletion. Bootstrap and snapshot replacement must invalidate old plans.

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

The local Raft recovery marker is **13**. A supported marker-12 volume is first
replayed successfully, then atomically upgraded and synced before this binary
can advertise its new capability. Previous binaries reject marker 13. Snapshot
version 11 gains optional activation evidence; older strict decoders reject that
field after activation. Metadata record versions and original frame bytes are
unchanged. Perform a coordinated upgrade with complete immutable backups; mixed
release operation and in-place downgrade are unsupported. Restore a complete
pre-upgrade backup into isolated volumes to return to an older binary. Never
edit/remove the marker or activation field to bypass the boundary.

No retention or large-record functionality is duplicated. The merged storage
changes on the stated official main are prerequisites, not assumed open work.

Removing historical revisions necessarily removes the ability to diagnose
conflicts with those discarded revisions later. Activation accepts that
validation boundary and rejects pre-activation writers. The ordinary distributed
internal-topic compactor remains blocked. Only the separate certified path may
rewrite a closed, fully committed range with all replicas caught up and eligible.
All metadata partitions must be available as ordinary local read views; missing
coverage defers work instead of transferring or certifying a partial state.

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

The checked disk installer stages and syncs log, index and gap marker,
invalidates the durable source generation, publishes/syncs the marker, replaces
log then index, and syncs the directory. Reopen accepts either the original log
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

The current certification deliberately replays all partitions twice and scans
the raw committed records once **per candidate segment**. A filtered page can
also cause repeated reads of records removed within that page. Scanning does
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
startup/failover, cold OS-cache recovery, normal request latency and cluster
activation/replica compatibility remain unmeasured acceptance conditions.

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

Whole cold broker startup/failover across the history ladder, transactional
intent/control history reclamation, and a proven maintenance-throughput bound
remain separate acceptance conditions. Retaining necessary deleted-name fences
or decisions is authoritative state growth, and is not bounded by live-group
count alone. Do not close the history-growth issue based only on these results.
