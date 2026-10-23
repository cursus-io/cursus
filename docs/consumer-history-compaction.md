# Consumer history compaction certification

## Scope and comparison before implementation

The implementation base is official main
`bb7dc59383c2e77c886f3b7c05bd79c9d70e21eb`. Its distributed internal-topic
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
partition**, and every producer-bearing record. Registration and tombstone share
a key; offset snapshots use a key per topic; lifecycle and reservation snapshots
have separate keys. Keeping a representative in every physical partition retains
provenance without inventing a new on-disk record. Tombstones have no time-based
expiry. Superseded registration epochs may be removed only when replay of the
actual proposed remainder proves the same deleted-group fence and live state.

Full replay must succeed before selection. Replay the candidate remainder and
compare the entire exported state and observed source sets. An exact, detached
removal list is bound to all partition identities and committed bounds, the
exclusive lease and compatibility generation. Raw and committed scans must
agree. Legacy batches, inferred registrations, orphan future epochs and physical
transactions (including control records) are rejected, rather than guessed at.
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

## Rollout boundary

This layer certifies deletion candidates; it does not activate distributed
cleaning. Activation still needs a durable cluster capability agreement,
registration/voter admission fences, downgrade rejection, and replica/bootstrap
tests. No new metadata record version is assigned here, and no retention or large
record functionality from the concurrent storage work is duplicated.

Removing historical revisions necessarily removes the ability to diagnose
conflicts with those discarded revisions later. The future activation contract
must explicitly accept that validation boundary and reject pre-activation
writers. A local configuration flag alone is not that contract. Until this is
implemented and tested, the production internal-topic compaction guard stays
closed. Actual reduction in an isolated storage fixture is not a production
rollout or proof that the issue is complete.

## Recovery and cost contract of this layer

The certificate is process-local, lease-bound and deliberately not serializable.
Restart, restore, bootstrap replacement, a changed committed end or authority
generation requires a fresh certification. It is never authoritative state.
The proposed retained records are original log frames, with their existing
version and physical offset. No new storage format is introduced or activated.
Downgrading this preparatory code requires no new format conversion; that does
not grant permission to downgrade a future activated cleaner.

The private disk replacement helper stages and syncs log, index and gap marker,
invalidates the durable source generation, publishes/syncs the marker, replaces
log then index, and syncs the directory. Reopen accepts either the original log
or its fully replaced log; existing startup index reconstruction handles the
window with a replaced log and old index. The tests kill a subprocess at each
boundary. They exercise process crashes, not host power loss or every filesystem
reordering. Existing distributed internal-topic and application compaction gates
are unchanged.

The current certification deliberately replays all partitions twice and scans
the raw committed records once **per candidate segment**. A filtered page can
also cause repeated reads of records removed within that page. It requires an
exclusive consumer recovery lease throughout. Thus it is not an acceptable
unbounded background job: request admission and repeated full-log work must be
addressed before online activation. All producer-bearing records and all
transaction-bearing histories are conservatively retained/refused. Empty
segments are not yet pruned; byte reduction alone does not bound file count.

The diagnostic counters in the isolated measurements distinguish source
key/payload bytes, rewrite frame bytes, temporary file sizes and kernel physical
I/O. Source/rewrite sync counters count instrumented successful boundaries; they
are not a complete syscall trace (identity creation and reopen work are outside
those counters). Transfer is zero because the fixture is local. Whole broker
startup/failover, cold OS-cache recovery, normal request latency and cluster
activation/replica compatibility remain unmeasured acceptance conditions.

## Isolated measurements (2026-10-23)

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

The fixture proves actual retained byte reduction and cache-miss recovery for
the certified ordinary format. It does not yet prove bounded segment-file count,
transaction-bearing log reduction, replica bootstrap, failover, restore or
DELETE/TRUNCATE during an activated cleaner. Those remain required before
production activation or closure of the history-growth issue.
