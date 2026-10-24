# Whole-broker consumer history recovery measurements

Source: `bdbe7fcfc6b75f385a1a1a368ee106df9dbe963f` (CI tested merge for PR source `533afcb82011cc232b88724892426218e0693a5b`), clean tree: True. [Linux CI scaling job](https://github.com/cursus-io/cursus/actions/runs/40096288876/job/120879133800).

The complete test package elapsed 1,534.821 s, including fixture setup and cleanup, offline preparation, shutdown and all scenarios; Go test compilation is outside that package timer. The 100 / 1,000 / 10,000 subtests took 332.26 / 295.88 / 906.60 s. Individual startup timers below intentionally exclude shutdown and offline preparation, while this full-test total retains those costs.

The 2026-10-24 hosted Linux Docker experiment runs the complete fixed-identity ladder. Four consumer groups, four metadata partitions and replication factor three remain fixed while revisions per group increase 100 → 1,000 → 10,000. Both baseline and fix use the same binary, layout and instrumentation; only history maintenance is disabled for baseline and enabled for fix. This measures the incremental history-reclamation effect, not a comparison with a historical release binary. Valid-index results separately demonstrate index reuse on each retained layout.

[Raw JSONL](benchmarks/consumer-history-whole-broker-linux-2026-10-24.jsonl) and [complete resource CSV](benchmarks/consumer-history-whole-broker-linux-2026-10-24.summary.csv) retain every scenario. Logical frame bytes, replay key/payload bytes, index bytes and transfer counters are separate overlapping quantities; they must not be summed into physical I/O. All timings are single observations, not confidence intervals. Baseline scenarios precede compacted scenarios in the same fixture; Raft snapshot, leadership and background progress are not frozen between them. Whole-start wall-time differences are observations, not an isolated causal multiplier for metadata compaction. Retained log bytes and metadata replay work provide separate direct evidence.

At 10,000 revisions per group, each replica retained 139,824 metadata log bytes instead of 17,593,264, while cache-absent replay decoded 1,902 rather than 240,072 records across three brokers. Whole startup was 15.217 → 12.941 s and failover 6.396 → 4.997 s. Maintenance including recreation cost 33.677 s and 42.890 broker CPU seconds. The foreground probe returned one `NOT_COORDINATOR` error in 1,341 calls, and sampled startup heap increased from 290,204,368 to 307,984,000 bytes. The benefit is bounded authoritative history and less recovery work; this sample does not establish zero disruption, lower memory use, or a universal startup multiplier.

## Setup and maintenance costs

Three Linux containers run on an Ubuntu 24.04 GitHub runner with Go 1.26.6 brokers and a Go 1.26.2 test driver, two CPUs and 1 GiB per broker. Metadata segments are 64 KiB, indexes 1 MiB, segment roll time 60 s and the maintenance interval 1 s. The sampler uses `strace -f --seccomp-bpf -e trace=fsync,fdatasync`; tracing overhead remains in both variants. The complete setup, seeding and maintenance costs are reported separately, not removed from an end-to-end improvement claim. Members explicitly leave after seeding to prevent session expiry from changing the frozen history; four registered groups and their offsets remain. Live-member preservation is tested separately in the lifecycle and ownership regressions.

| Revisions/group | Initial setup s | Seeding s | Acknowledged commits | Seed retries | Maintenance including recreation s | Per-replica metadata bytes before → after | Retained segments/node |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 100 | 60.279 | 10.120 | 400 | 0 | 66.431 | 180,704 → 5,636 | 8 |
| 1,000 | 5.427 | 66.753 | 4,000 | 0 | 11.535 | 1,757,117 → 193,059 | 8 |
| 10,000 | 5.173 | 620.079 | 40,000 | 0 | 33.677 | 17,593,264 → 139,824 | 8 |

The 100-revision maintenance row also includes waiting for the 60 s active-tail roll; maintenance time need not grow monotonically with history count.

Every replica must satisfy the same 327,680-byte metadata-log bound (four 64 KiB active tails plus 64 KiB of fixed-state witnesses), independently of history level, and the five-minute reclamation observation bound after readiness. The reported maintenance wall time additionally includes recreation and readiness waiting. Exact offsets are checked after every recovery and ownership movement. Removing the index does not undo physical reclamation. These bounds apply to this fixed-identity, supported-format fixture; unique identity creation and protected legacy evidence have separate limits below.

| Revisions/group | Seed commit p50 / p95 / max ms | Maintenance fetch p50 / p95 / max ms | Fetch successes / calls | Returned errors |
| --- | --- | --- | --- | --- |
| 100 | 22.495 / 164.603 / 210.171 | 0.263 / 0.340 / 603.873 | 5758 / 5758 | {} |
| 1,000 | 20.687 / 148.949 / 237.958 | 0.262 / 0.510 / 603.734 | 386 / 386 | {} |
| 10,000 | 19.705 / 150.699 / 286.960 | 0.250 / 2.822 / 1407.733 | 1340 / 1341 | {"NOT_COORDINATOR": 1} |

The read probe starts after recreated brokers become ready. It measures ordinary offset reads, including client retries, during maintenance; it is not a concurrent write-throughput benchmark or a measurement of unavailable time during recreation. Seed commit latency is a different phase. No error is counted as a successful read.

## Whole-cluster startup and failover

Startup runs from the compose start command through readiness, full ISR and exact offset verification. Shutdown and explicit offline index/cache preparation are excluded. Absent, corrupt and old-index cases use per-file POSIX advisory eviction; this does not establish hardware-cold storage. Valid-index cases retain warm OS pages and assert an actual index hit on all three brokers. Failed-index cases assert misses. The old-index fixture sets unsupported version 0 while recomputing a valid digest; it exercises version rejection rather than checksum rejection. Supported v1 ordinary indexes remain readable and have separate compatibility regressions.

| Revisions/group | Index case | Baseline startup s | Compacted startup s |
| --- | --- | ---: | ---: |
| 100 | absent | 9.438 | 8.940 |
| 100 | valid | 9.415 | 8.919 |
| 100 | corrupt | 9.418 | 8.930 |
| 100 | old | 9.429 | 8.924 |
| 1,000 | absent | 10.440 | 8.942 |
| 1,000 | valid | 9.413 | 8.955 |
| 1,000 | corrupt | 10.436 | 9.957 |
| 1,000 | old | 9.446 | 9.689 |
| 10,000 | absent | 15.217 | 12.941 |
| 10,000 | valid | 12.499 | 10.934 |
| 10,000 | corrupt | 14.217 | 12.943 |
| 10,000 | old | 15.208 | 12.962 |

| Revisions/group | Scenario | Baseline s | Compacted s |
| --- | --- | ---: | ---: |
| 100 | interrupted_start_then_restart | 4.216 | 3.692 |
| 100 | coordinator_failover | 5.698 | 5.907 |
| 1,000 | interrupted_start_then_restart | 5.005 | 4.465 |
| 1,000 | coordinator_failover | 5.742 | 5.916 |
| 10,000 | interrupted_start_then_restart | 8.822 | 7.742 |
| 10,000 | coordinator_failover | 6.396 | 4.997 |

The interrupted-start wall timer spans start, SIGKILL, restart and exact recovery, but its resource row covers only the final process and does not establish the kill occurred during replay. The separate [deterministic replay-crash experiment](consumer-history-replay-crash-results.md) records three actual partial-replay processes and all their work. Failover includes stopping the old owner, observing the new owner on survivors and verifying exact offsets. Returning the old node to full ISR is checked afterward, outside that failover timer.

## Resource accounting

CPU, guest I/O and sync columns below sum reported broker counters across the three nodes; memory/temp columns take the largest reported node peak, not a simultaneous cluster peak. Heap maxima include the periodic peak and the explicit final heap sample; a later larger observation is not discarded. Missing heap observations stay blank rather than being treated as zero. Startup counters are process-lifetime values captured near verification, so monitoring can extend slightly beyond the wall timer. Maintenance includes newly recreated processes. The full CSV provides all index modes and survivor failover deltas.

| Revisions/group | Variant / phase | Broker CPU s | Guest read / write bytes | Successful sync | Largest sampled heap bytes | Largest kernel RSS bytes | Largest sampled temp bytes |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |
| 100 | baseline / whole_cluster_start | 1.890 | 2,162,688 / 626,688 | 1,926 | 205,166,016 | 70,520,832 | 0 |
| 100 | compacted / maintenance_including_recreate | 11.570 | 0 / 831,488 | 5,356 | 300,861,472 | 180,703,232 | 84 |
| 100 | compacted / whole_cluster_start | 1.730 | 1,732,608 / 524,288 | 1,948 | 232,763,344 | 104,312,832 | 84 |
| 1000 | baseline / whole_cluster_start | 3.080 | 18,124,800 / 688,128 | 12,776 | 242,363,344 | 140,550,144 | 0 |
| 1000 | compacted / maintenance_including_recreate | 5.680 | 0 / 1,609,728 | 13,393 | 322,683,904 | 234,819,584 | 84 |
| 1000 | compacted / whole_cluster_start | 3.360 | 13,565,952 / 638,976 | 12,739 | 311,019,584 | 200,622,080 | 84 |
| 10000 | baseline / whole_cluster_start | 11.460 | 105,459,712 / 1,257,472 | 50,652 | 290,204,368 | 270,131,200 | 0 |
| 10000 | compacted / maintenance_including_recreate | 42.890 | 0 / 11,218,944 | 58,858 | 384,563,072 | 342,679,552 | 84 |
| 10000 | compacted / whole_cluster_start | 6.330 | 52,498,432 / 1,081,344 | 50,618 | 307,984,000 | 264,593,408 | 84 |

| Revisions/group | Variant / phase | Frame bytes | Replay records | Index read / write bytes | Compaction source records | Removed records |
| --- | --- | ---: | ---: | --- | ---: | ---: |
| 100 | baseline / whole_cluster_start | 1,084,224 | 2,472 | - / 33,441 | 0 | 0 |
| 100 | compacted / maintenance_including_recreate | 4,141,953 | - | 33,441 / - | 9,432 | 1,200 |
| 100 | compacted / whole_cluster_start | 287,436 | 72 | - / 33,441 | 540 | 0 |
| 1000 | baseline / whole_cluster_start | 10,542,702 | 24,072 | - / 33,489 | 0 | 0 |
| 1000 | compacted / maintenance_including_recreate | 43,299,477 | - | 33,489 / - | 98,850 | 10,716 |
| 1000 | compacted / whole_cluster_start | 9,846,009 | 2,640 | - / 33,489 | 19,800 | 0 |
| 10000 | baseline / whole_cluster_start | 105,915,912 | 240,072 | - / 33,558 | 0 | 0 |
| 10000 | compacted / maintenance_including_recreate | 399,456,045 | - | 33,558 / - | 905,505 | 119,085 |
| 10000 | compacted / whole_cluster_start | 10,906,272 | 1,902 | - / 33,558 | 22,824 | 0 |

Frame counters include background certification work while maintenance is enabled, not just startup replay. A smaller replay count therefore need not mean fewer total frame bytes in the observation window. Repeated scans and partial work remain counted. Retained-scan and replay stages can read the same source independently. RF3 gives every broker local metadata, so this fixture does not require nonreplica metadata transfer; RF2 recovery is covered separately in the correctness evidence. Container-interface traffic includes replication, health checks and monitoring and is not metadata payload transfer.

## Limits and reproduction

- This is Linux guest accounting on a hosted Docker runner, not physical host-device tracing. Available cgroup CPU, memory and I/O counters remain separate in the full CSV; missing counters stay blank rather than zero.
- Heap, total store size and temporary space are sampled at one-second intervals. Samples can miss shorter peaks. Kernel RSS high-water marks have a different scope. Total store size includes Raft, indexes and checkpoints and may include sparse logical file sizes; retained metadata counts only `.log` bytes.
- Maintenance can raise latency and consume more read/write/sync work than replay alone. The earlier one-second segment-roll experiment failed its five-minute maintenance bound at 1,000 revisions; [its raw results](benchmarks/consumer-history-one-second-roll-2026-10-24.jsonl) and measured costs remain recorded. The hosted sixty-second layout is a separate complete experiment, not a replacement baseline for that failed layout. The local sixty-second run also failed its old per-node readiness wait at 10,000 after all ordinary startup cases; its [partial data](benchmarks/consumer-history-local-partial-2026-10-24.jsonl) remains separate. This hosted run uses a shared two-minute readiness budget, with all waiting included in wall time and the same five-minute reclamation and retained-byte bounds.
- The bound assumes fixed group, producer and transaction identities. Deleted-group epoch fences and unique producer identities remain authoritative. Unlimited creation of distinct identities or protected legacy prefixes is not bounded by live-group count alone. Non-compositional histories use conservative per-segment certification and may cost more.
- The disposable index accelerates valid-index recovery but does not authorize deletion. Cache-miss recovery still reads the authoritative retained log. Compaction preserves committed offsets, identity, generations, transaction evidence and producer continuity under the documented protocol gate.

Run with a fresh output path and unused isolated port offset:

```sh
RUN_E2E_CONSUMER_HISTORY_MEASURE=1 \
CURSUS_CLEANUP_PORT_OFFSET=38000 \
CURSUS_HISTORY_MEASURE_OUTPUT=/tmp/history-whole-broker-new.jsonl \
go test ./test/e2e-cluster -run '^TestConsumerHistoryWholeBrokerLadder$' -count=1 -timeout=120m -v
python3 test/e2e-cluster/summarize_history_measure.py /tmp/history-whole-broker-new.jsonl
```
