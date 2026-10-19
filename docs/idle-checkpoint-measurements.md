# Idle checkpoint measurement, 2026-10-20

## Final results

Final-code measurements ran **06:07:17–06:12:21 KST** (21:07:17–21:12:21 UTC
on October 19). All eight untraced runs completed in 30.000–30.002 seconds;
all four trace runs completed in 10.000–10.002 seconds. These results replace
the exploratory runs discussed below. [Raw counters, timestamps and binary
hashes](benchmarks/idle-checkpoints-2026-10-20.json) are retained.

Untraced means of two runs per revision/condition:

| Metric | Idle baseline | Idle fixed | Load baseline | Load fixed |
| --- | ---: | ---: | ---: | ---: |
| CPU cores (user + system) | 0.0664 | 0.0423 | 0.0644 | 0.0461 |
| User CPU seconds / run | 0.588 | 0.369 | 0.549 | 0.480 |
| System CPU seconds / run | 1.403 | 0.899 | 1.382 | 0.902 |
| Storage-accounted write KiB/s | 192.0 | **0** | 323.2 | 83.2 |
| Whole-VM iowait % | 3.34 | 0.59 | 4.48 | 1.69 |

CPU execution decreased by **36.3% idle / 28.4% under load** in these samples.
Storage-accounted writes decreased by **100% idle / 74.3% under load**.
The idle baseline CPU samples ranged from 0.0546 to 0.0782 cores, so the CPU
percentages are indicative local results, not a statistical or production claim.

File operations during each independent ten-scan trace:

| Operation | Idle baseline | Idle fixed | Load baseline | Load fixed |
| --- | ---: | ---: | ---: | ---: |
| Checkpoint opens / writes / closes / renames (each) | 480 | **0** | 720 | 120 |
| Checkpoint file fsyncs | 480 | **0** | 720 | 120 |
| Checkpoint directory fsyncs | 480 | **0** | 720 | 120 |
| Checkpoint bytes written | 24,960 | **0** | 37,440 | 6,240 |
| All data-directory file writes | 480 | **0** | 780 | 180 |
| All fsync calls | 2,160 | 1,200 | 2,760 | 1,560 |
| Non-checkpoint fsync calls | **1,200** | **1,200** | **1,320** | **1,320** |

Thus unchanged checkpoint write operations fall from 48/s to zero. Changed
checkpoints still publish under load (12 writes/s), with their file and directory
syncs intact. The existing non-checkpoint data/index sync work is unchanged.

Latency is the range of the two untraced per-run percentiles (milliseconds):

| Metric | Baseline | Fixed |
| --- | ---: | ---: |
| Append p50 | 12.10–13.07 | 12.72–13.76 |
| Append p95 | 15.01–26.79 | 16.42–18.48 |
| Append p99 | 16.26–27.93 | 18.69–38.17 |
| Local commit p50 | 48.86–52.86 | 54.71–55.93 |
| Local commit p95 | 78.22–80.33 | 59.89–63.02 |
| Local commit p99 | 81.16–104.73 | 63.81–63.95 |

Local commit tails improved in this sample, while append distributions overlap
and one fixed append p99 is higher. There is **no claim of improved append
latency or a latency non-regression guarantee**. Network/production latency and
physical block-device IOPS require a separate suitable-host experiment before
using these numbers for deployment sizing.

## Method and interpretation

Baseline: deployed/main commit `35cf766a6e97e0549bbb589cc0b4d4dc40f6595b`.
Both binaries use the identical opt-in `TestCheckpointWorkload` source.
This is an isolated storage/FSM experiment on Linux, not a production rollout
or a three-host network benchmark.

- Go 1.26.6, linux/amd64; Docker 24.0.2 on WSL2 kernel
  `5.15.167.4-microsoft-standard-WSL2`; overlay filesystem backed by the same
  local Linux volume; `GOMAXPROCS=4` for both binaries.
- Three FSM instances, eight partitions each; one seeded idempotent producer
  per partition. Catch-up scan once per second. The fixed-load case sends two
  logical 1-KiB messages/s, each appended to all three replicas, then applies
  the fenced partition commit to all replicas. Each load run offers 60 logical
  messages / 180 replica appends and 30 catch-up scans per FSM over 30 seconds.
  Actual completion may exceed that window if the baseline cannot keep up.
- Each condition uses baseline/fixed/fixed/baseline order, 30 seconds per run.
  The requested duration and actual completion time are reported separately.
  Fresh directories and a one-second settling period exclude initialization.
  No builds or other test suites run during these measurement windows.
- Independent strace runs request ten seconds per condition. Trace overhead can
  extend elapsed time, so syscall counts are reported per ten scans, separately
  from untraced CPU and latency results. Twenty logical messages are appended in
  each traced load case. The parser restricts counts to the recorded time window
  and handles unfinished/resumed syscalls by thread.
- CPU cores mean `(process user time + process system time) / elapsed time`.
  Host iowait is reported separately from `/proc/stat`; it is a whole-VM signal
  and cannot be attributed exclusively to this process.
- Process `write_bytes` is Linux storage-write accounting, not serialized JSON
  bytes. Checkpoint payload bytes and file-operation counts come from strace.
  WSL2 exposes neither cgroup v2 `io.stat` nor v1 blkio I/O counters here, so
  **physical-device/cgroup write IOPS are unavailable**. File write operations
  must not be described as physical IOPS. Total `syscw` also includes Go runtime
  eventfd writes; those can increase while checkpoint writes disappear.
- Append/commit timings include sequential work on three local replicas.
  Commit means FSM commit application plus disk barriers, without Raft consensus
  round trips or client network latency. Two runs of 60 messages support a local
  regression check, not a production tail-latency guarantee.

## Activity controls and limitations

Read-only Kubernetes inspection during measurement found only the ARC controller
and listener, with no ARC runner workload pods. No `Runner.Listener` or
`Runner.Worker` process was found on the local Windows host. The Cursus repository
runner API returned `total_count: 0`. Organization-wide runner inventory returned
HTTP 403, so activity on a separate host serving other repositories is unknown.
The supplied earlier production observation explicitly had a separate active
host runner; it is not treated as a fully idle node measurement.

Only `cursus-checkpoint-lab` was running in this Docker engine during the final
measurement. An unrelated MySQL test container seen earlier had already stopped;
this task did not start, stop or reconfigure it. No production resources were
changed. Other Windows/VM activity remains a possible source of noise, which is
why both CPU components and host iowait, repeated runs, raw counters and exact
timestamps are retained.

A preliminary 120-partition run was discarded from the matched-time comparison:
the baseline took 109 seconds to process the requested 30 scans on this local
filesystem. The final experiment uses 24 partitions to reduce saturation, but
any remaining schedule overrun is retained in the results. Compare identical
work using total CPU/storage bytes as well as rates. Do not extrapolate these numbers to the supplied 243-file
production snapshot or claim that its 0.71-core CPU usage will fall by the same
absolute amount.

## Reproduction

Install Go 1.26.6, Python 3 and strace on Linux. Build one test executable from
each revision, copying `pkg/cluster/replication/fsm/checkpoint_workload_test.go`
unchanged into the baseline checkout first:

```sh
# In the baseline checkout:
go test -c -o /results/baseline.test ./pkg/cluster/replication/fsm
# In the modified checkout:
go test -c -o /results/fixed.test ./pkg/cluster/replication/fsm
python3 scripts/measure-checkpoints.py /results
```

The script records binary SHA-256, UTC timestamps, process user/system CPU,
storage-accounted bytes, host CPU/iowait counters, latency percentiles and syscall
counts. Raw traces/logs remain local measurement artifacts; summarized raw runs
are retained with this report for auditing.
