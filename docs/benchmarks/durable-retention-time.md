# Durable retention validation and costs

Measured on 2026-10-23 against the broker-time implementation, based on official
main `a49138e7922aa66b14cb4482a8b084e5b9c1b8cd`. The comparison and contract are
in [design](../durable-retention-time-design.md) and
[operations](../durable-retention-time-operations.md).

## Baseline versus durable time

The baseline fixture writes one identical 110-byte legacy frame, rolls the
source, waits for actual filesystem creation time to advance, and creates an
incremental replica and retained-bootstrap replica. No `Chtimes` is used.
At source mtime + one hour + one nanosecond, the audited predicate expires the
original but neither replica. The observed mtimes were:

| Copy | UTC mtime | Baseline expired |
| --- | --- | --- |
| Original | 02:40:54.366539758 | yes |
| Incremental | 02:40:54.486541339 | no |
| Bootstrap | 02:40:54.556542262 | no |

With durable time, the two-record fixture preserves identical append times in
all three copies. At age 90 minutes, source and incremental copies delete the
first individually closed record; a combined bootstrap segment retains both
until its youngest record expires. At exactly two hours, the youngest is kept;
one millisecond later all copies reclaim it. This is segment-granularity delay,
not a changed record age. Each copy reads 236 bytes and decodes two records to
rebuild time evidence; repeated warm checks read no additional log bytes.
Compaction retains both the original mtime and the survivor's append time.

## Measured work

Linux amd64 Docker, Go 1.25.0, Intel i5-1340P host, two-CPU quota,
`GOMAXPROCS=2`, 3 GiB memory limit, network disabled and no published ports.
Files are private container fixtures. This is a shared development host and a
single small sample, not production sizing or a statistically significant
latency comparison. CPU quota and other host work affect wall time.

The fixture contains 4,096 records with 1,024-byte payloads in one closed
segment, plus an empty active segment. File sizes are logical lengths reported
by `Stat`, including sparse/index reservations, not allocated physical blocks.

| Quantity | Measured value |
| --- | ---: |
| Legacy log bytes | 4,628,480 |
| Timed log bytes | 4,661,248 |
| Added record metadata | 32,768 (8 per record) |
| All retained file lengths before export | 6,758,400 |
| Cold summary read bytes / decoded records | 4,661,248 / 4,096 |
| Summary write bytes / temporary disk bytes | 0 / 0 |
| Warm extra log read bytes | 0 |
| Subsequent retained-export page reads / decoded records | 4,665,816 / 4,100 |
| Canonical export writes | 4,661,248 |
| Export checksum reads | 4,661,248 |
| Chunk bytes actually returned by `ReadChunk` | 4,661,248 |
| Export records/catalog temporary file lengths | 4,661,248 |
| Export pages | 5 |

The four additional decodes are repeated page-boundary work. The export
measurement includes source paging, canonicalization, checksum and chunk reads.
It does not represent network packet bytes, receiver installation, startup,
Raft snapshot transfer, filesystem metadata writes or physical device I/O.
Those are not claimed as zero. Cold backup/install correctness is tested
separately. Time-summary reconstruction itself creates no disk sidecar.

| Benchmark | Wall/op | Allocated bytes/op | Allocations/op |
| --- | ---: | ---: | ---: |
| Cold time summary (165 iterations) | 8.399 ms | 8,994,258 | 16,426 |
| Warm retention tick (57,249 iterations) | 18.085 us | 2,103 | 27 |
| Durable append, feature disabled (200 requests) | 9.434 ms | 3,613 | 19 |
| Durable append, feature enabled (200 requests) | 9.854 ms | 3,499 | 19 |

Append p50/p95 were 8.073/15.860 ms disabled and 8.599/15.710 ms enabled.
These are real local `EnqueueSync` requests including durable writes, not mock
clock durations or network latency. Startup, fixture creation, teardown and
sorting samples are excluded from benchmark timers. Concurrent summary rebuild
latency and production-size segments are not measured by these numbers.

The disk measurement process used 3.176 s user CPU + 1.553 s system CPU, with
33,920 KiB maximum RSS; the append process used 0.047 s + 0.284 s, with
17,332 KiB maximum RSS. These process measurements use `ProcessState`/Linux
`Rusage` and **include** startup, fixtures, test work and teardown. They are
not per-operation CPU costs. CPU profiles sampled 3.710 s and 0.120 s,
respectively; sampling totals differ from OS process accounting.

Raw benchmark output is in [disk](durable-retention-disk.txt) and
[append](durable-retention-append.txt). Run:

```sh
go test ./pkg/disk -run 'TestRetentionTimeRecoveryWork|TestRetentionRecoveryBaselineMtimeDrift|TestDurableRetentionRecoveryGranularity' -v -bench BenchmarkRetentionTimeSummary -benchtime=1s
go test ./pkg/topic -run '^$' -bench BenchmarkRetentionTimeAppend -benchtime=200x
```

## Verification and limits

Focused contract tests pass for legacy/new mixtures, future timestamps,
committed and transaction boundaries, event-history protection, capability
activation, Raft snapshot restore, standalone downgrade rejection, compaction,
interrupted bootstrap replacement, torn CDM6 checksum repair, restart, cold backup restore, absent/corrupt
offset indexes and corrupt authoritative records. The controller integration
test transports sealed JSON through verified catch-up and rejects timestamp
tampering before append. Tail repair reseeds the leader clock and retention
defers during the exclusive recovery view.

Linux race checks pass for retention, clock, bootstrap, compaction and tail
repair across util/wire/disk/topic/replication/FSM/controller packages. Broader
local unit coverage passed the changed storage/topic/replication/server and
other packages, except `TestTopicCleanupReplaysBrokerControlsFromDiskAfterSnapshot`
in the controller suite. That test also fails on the unmodified official main
in the same two-CPU isolated environment: cleanup exhausts its metadata scan
budget. Its four baseline subtests failed in 138.603 s total. This work does
not weaken that budget or duplicate the independent cleanup change.

The first broad local command also included external E2E packages without their
broker fixture; that portion was stopped and is not counted as a passing E2E
run. GitHub's configured E2E jobs provide separate cluster/fault coverage.
Windows broad tests encountered the host's existing disk-headroom threshold;
the Linux measurements retain default disk-pressure protection. Small semantic
fixtures use their explicit fixture configuration. No operating-volume mtime,
shared-volume cleanup or production deployment was performed.
