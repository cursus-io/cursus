# Consumer metadata recovery index

## Design and evidence

The official main inspected on 2026-10-23 is
`a49138e7922aa66b14cb4482a8b084e5b9c1b8cd`. Distributed internal-topic
compaction is disabled in `pkg/disk/compaction.go`. Strict startup reads every
committed metadata partition. Cleanup checkpoints and detached transfer staging
do not replace this scan. No end-to-end cold-start speedup has been measured.

[Accepted KIP-848](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
proposes partitioned coordinator state and incremental reconciliation; it is
not a persistent Cursus recovery snapshot specification.
[Kafka 4.1.0 OffsetMetadataManager](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java)
implements replay, separate pending transactional offsets, tombstones, and
record-position ordering when transaction decisions materialize offsets.
[LogCleaner](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaner.java)
and the [official compaction design](https://kafka.apache.org/41/design/design/)
retain keyed state, protect producer continuity and transaction markers, and
exclude the unstable tail. Kafka loads retained records; these sources do not
establish constant-time startup.

Cursus should adopt committed-prefix certification, explicit deletion fences,
separation of pending transaction state, and measured cleaner costs. It must
preserve its registration epochs, full offset revisions, legacy multi-key
batches, compatibility Raft baseline and physical source provenance. Ordinary
last-key-wins cleaning cannot prove these contracts. In particular, strict
replay currently validates historical conflicting revisions and offset
regressions. Deleting old revisions changes what corruption it can detect.

## Layers and safety boundary

1. A disposable, versioned local replay projection plus suffix replay. Build it
   only from a successfully validated fixed committed cut, never live state.
   A cache miss always uses the authoritative full log. Older or ambiguous
   suffix revisions fall back to full replay, preserving existing validation.
2. Separately gated authoritative history compaction. This requires a durable
   protocol/format agreement, replica/bootstrap support, transaction retention
   rules and crash-safe replacement. Layer 1 does not authorize deletion and
   does not close the history-growth issue.

The first format deliberately falls back for legacy records, inferred groups,
unknown compatibility authority, remote staged partitions, and physical
transaction records. Reservation and decision snapshots stored as ordinary
metadata remain in the projection. Transaction-bearing partitions must keep
using full replay until a projection can certify both raw pending records and
the receiver's current transaction decisions. This is an availability/cost
limitation, not permission to omit those records.

Identity includes topic instance/lifecycle, partition count, durable local
content generation, retained start and exact exclusive committed ends, and a
digest of the compatibility baseline. Prefix mutations invalidate the durable
generation before changing data. Appends leave it stable. Restore must match
the compatibility digest and disk identity; bootstrap and truncation cannot
reuse another generation. Local projections are not replication authority and
must not be copied independently onto a different log.

Publication order: validate replay and reduced projection equivalence; sync
the source log; validate identity again; write a checksummed bounded temporary
file; sync and close it; atomically replace the published cache. A missing,
torn, oversized, unknown-version or mismatched cache is ignored. Directory
publication loss may expose an older cache: it must independently pass the same
identity and suffix checks. No source data is deleted at any stage. Cache write
failure must not prevent a correct full replay from installing its result.

## Regression and fault checks defined before implementation

- Fixed live groups/partitions with 100, 1,000 and 10,000 offset revisions;
  compare full replay, valid index with/without suffix and rebuilding costs.
- Missing, corrupt, old-version and stale-generation indexes; exact bounds,
  rollback, changed compatibility baseline, topic replacement, and restore.
- Repeated replay interruption and failures before source sync, file sync and
  publication; concurrent append beyond the cut must remain outside the index.
- Deleted group epoch fences, re-registration, reservations and decisions;
  suffix conflicts and regressions must match full replay's outcome.
- Runtime recovery must preserve unrelated member heartbeat/ownership clocks.
- Report decoded records and logical payload/frame bytes separately from OS
  physical I/O, CPU and wall time. Include index writes, fsyncs and temporary
  space. A local replay benchmark excludes broker startup, Raft, transfer and
  request latency; it cannot establish an end-to-end cold recovery speedup.
- Demonstrate cache-miss recoverability separately from actual retained-history
  reduction. Until layer 2 is implemented and verified, retained history remains
  unbounded and the issue remains open.

Concurrent work: the record-size progress issue and durable time-retention issue
are active in separate workspaces. This layer does not modify replay-page size
admission, record serialization, retention timestamps or their capability gates.

## Operation and format contract

The feature defaults to **off**. Enable `consumer_recovery_index_enabled: true`
in YAML, `CONSUMER_RECOVERY_INDEX_ENABLED=true`, or `--consumer-recovery-index`.
It is supported only for known-authority distributed full recovery with ordinary
local views for **every** offsets partition. A remote staged partition disables
the index for that recovery. Scoped bootstrap replay and legacy startup retain
their existing paths. There is no background refresh: each successful eligible
recovery publishes the new cut; later appends are replayed as a suffix.

`<log_dir>/.consumer-replay-index.json` is a **derived cache**, never authoritative
state. Its version-1 checksum envelope contains original selected records with
their original partition/offset positions, source provenance, identity fences,
and a compatibility-baseline digest. Registration/tombstone selection preserves
deleted-group epoch fences. Ordinary reservation/decision snapshots are retained;
physical transactional records (including invisible pending records) prohibit
publication. All supported records must reconstruct exactly the full replay's
exported state before publication. The cache does not serialize live heartbeat or
ownership clocks. Runtime replay preserves them only for an unchanged owner and
unchanged recovered member session; startup, restore and a newly acquired owner
must establish their own session windows.

The published file and its single `.tmp` file are each limited to 32 MiB.
The limit is an on-disk serialization limit, **not** a peak-heap limit. Building
an index still performs full replay and its historical validation, plus a raw
transaction-certification pass and a reduced-state equivalence replay. Large
current state can exceed the limit and safely remain on full replay.

Source fsync precedes publication. On Unix, replacement also fsyncs the directory;
Windows uses the existing write-through atomic replacement helper. The process
crash tests exit after write, after file fsync, and after publication, twice per
stage. They establish process-crash recovery, not power-loss behavior on every
filesystem. A failed write retains either the old independently valid cache or
no usable cache; full logs remain authoritative. One fixed temporary filename
bounds repeated-crash leftovers. The broker log-directory lock and exclusive
recovery lease must serialize writers.

Upgrade requires no wire or authoritative log-format change. Disable the option
to roll back its use. Unknown cache versions are discarded and rebuilt. Before
downgrading to a broker version that predates durable cleanup generations, stop
the broker and remove this derived cache (and its `.tmp`) so older prefix
mutations cannot leave a reusable token. Do not copy a cache independently of its
logs. Restore, retention, compaction, DELETE/TRUNCATE and bootstrap replacement
must continue to invalidate the existing disk generation/lifecycle fences; this
change does not relax those requirements or the internal-compaction prohibition.

`RecoverySnapshot()` exposes `index_hit`, `index_read_bytes`, `index_write_bytes`,
`index_source_records`, `index_source_payload_bytes`, and `index_failure`.
The source counters include raw certification and committed reads, including
retry work when a cache-backed replay falls back. Payload bytes mean decoded key
plus payload lengths, **not frame bytes or physical I/O**. Cache byte counts are
serialized file bytes; write counts describe successful publication. Errors and
partial failed writes require OS-level observation for complete I/O accounting.
With the feature disabled, source counters are zero (not instrumented), while
`replayed_records` still counts the ordinary full replay. A valid unchanged cut
does not rewrite or fsync the index. Monitor rebuild failures and repeated misses;
enabling this option can increase cache-miss recovery time and allocations.

## Measurements (2026-10-23)

The [Windows replay benchmark](benchmarks/consumer-replay-index-windows-2026-10-23.txt)
uses one fixed live group and four partitions, in-memory source records and real
cache-file I/O. Three samples cover full, build, valid, one-record suffix and
corrupt-cache recovery at 100/1,000/10,000 historical revisions. Fixture creation
is excluded. The baseline hides the optional index interface in the same binary,
using the unchanged strict replay algorithm; it is not a separately built audit
commit. At 10,000 revisions, full replay reads 10,001 source records, a build
reads 20,002, a valid unchanged cache reads zero source records, and a one-record
suffix reads two. Cache sizes are 1,962/1,966/1,970 bytes. Host contention produced
large outliers, including a 255 ms valid-cache sample: these data do not support a
stable end-to-end startup speedup claim.

The [Linux disk measurements](benchmarks/consumer-replay-index-linux-2026-10-23.jsonl)
use Go 1.25.0 in an isolated Docker container (2 CPUs, 3 GiB memory, no network),
four fixed live groups and four partitions. Logs are real disk files; the measured
operation is local controller recovery. Initial appends, broker construction and
topology setup are excluded; the OS page cache is **not dropped**, and package
tests were running concurrently. All modes use the same fixture and verify exact
offsets after recovery. The JSON includes each measured CPU time, sampled heap,
allocated bytes, OS I/O counters, logical source work and cache bytes.

| Revisions/group | Full wall ms | Build wall ms | Valid wall ms | Missing wall ms | Corrupt wall ms | Retained log bytes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 120.11 | 256.34 | 15.30 | 159.21 | 140.25 | 151,308 |
| 1,000 | 361.50 | 846.73 | 24.47 | 1,111.76 | 487.86 | 1,508,516 |
| 10,000 | 3,087.55 | 7,814.35 | 3.14 | 5,396.86 | 4,871.06 | 15,152,524 |

At 10,000 revisions/group, full/build/valid CPU time was
3,991.31/9,214.95/4.65 ms; sampled absolute peak heap was
46,752,456/46,547,416/2,163,576 bytes; total allocation was
223,108,912/499,297,480/159,024 bytes. The valid cache reads 6,767 bytes and
replays eight selected records. Build reads 80,008 source records and
20,864,104 key-plus-payload bytes. There is zero network transfer in this local
fixture. `/proc/self/io` reports zero physical read bytes in every case, reflecting
the warm/mapped pages; this is **not** evidence of zero disk work on cold start.
Build physical writes are 24,576 bytes, missing/corrupt rebuilds 8,192 bytes and
valid-cache writes zero. OS `rchar` does not account for memory-mapped reads.
Heap is sampled every millisecond and may miss a transient peak.

Reproduce the disk fixture with:

```sh
CURSUS_INDEX_MEASURE=1 go test ./pkg/controller -run '^TestConsumerReplayIndexDiskMeasurement$' -count=1 -v
go test ./pkg/coordinator -run '^$' -bench '^BenchmarkConsumerReplayIndex$' -benchmem -benchtime=3x -count=3
```

Fsync syscall counts, filesystem temporary-space high-water marks, normal request
latency, whole broker cold startup, remote staging/bootstrap transfer and
multi-broker ownership failover remain unmeasured. Publication executes source
syncs plus a temporary-file sync and durable replacement; these are code-path
descriptions, not measured syscall counts. The crash test bounds temporary-file
count, not peak filesystem space. No log bytes are reclaimed by this layer.

## Remaining acceptance conditions

This is a partial foundation and must reference, not close, the history-growth
issue. It validates exact cuts, suffix replay, checksum/version/generation and
compatibility fallback, tombstones and re-registration, reservation/provenance
preservation, physical-transaction/legacy refusal, repeated publication crashes,
unchanged-owner clock preservation, real disk recovery and FSM snapshot restore.
It does **not** implement authoritative history reduction, accelerate all
transaction-bearing or remote-stage recovery, or prove distributed failover cost.

Before a separate compaction layer can roll out, define and test a versioned
cluster capability and authoritative retained-state format that every eligible
replica/bootstrap recipient accepts. It must retain tombstone epoch fences after
group deletion/recreation, resolve legacy multi-key batches, preserve producer
and transaction control/reservation/decision visibility, and distinguish a
validated compacted prefix from missing data. Crash-safe data replacement must
survive every durable step and replay correctly without this cache. Measure
compaction's own read/write/transfer bytes, CPU, fsync and temporary-space costs,
and demonstrate bounded retained history under repeated normal operation. The
existing distributed internal metadata compaction guard remains in place until
that work is verified. No task-branch CI exceptions are introduced.
