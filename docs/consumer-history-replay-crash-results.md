# Interrupted consumer replay: process resource evidence

Observed on 2026-10-24 in the [Linux CI fault experiment](https://github.com/cursus-io/cursus/actions/runs/40092762285/job/120868661498). PR source `88b3e7d57a292efc22328574e3b5312cb3e897d7`, tested merge `fd66ae9352e55ca4e6a4aec6f33ea54607bada90`. Ubuntu 24.04 runner, Go 1.26.2 driver, Go 1.26.6 broker; three replicas, four fixed consumer groups with 1,000 revisions each, two CPUs and 1 GiB per broker. History maintenance was disabled to retain replay work. The test passed in 146.235 s including its fixture lifecycle.

[Raw JSONL](benchmarks/consumer-history-replay-crash-linux-2026-10-24.jsonl) retains all per-process and survivor counters. This is a fault-image experiment, not a baseline/fix speed comparison. It must not be combined with local Docker measurements as if the environments were identical.

## Measured costs

Initial setup took 49.192 s and seeding took 62.763 s. Each armed process exited with code 77 after consuming actual source pages and before index publication; all three attempts verified that no index had been published. Thresholds are checked at page boundaries, so consumed records can exceed the threshold.

| Attempt | Threshold records | Consumed records | Start-to-exit s | Frame bytes | Replay key/payload bytes | Broker CPU s before diagnostic | Waited-child CPU s through exit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,024 | 2,006 | 2.540 | 882,563 | 591,693 | 0.494036 | 0.704489 |
| 2 | 2,048 | 3,009 | 1.552 | 1,323,835 | 887,530 | 0.451221 | 0.647622 |
| 3 | 1,024 | 2,006 | 1.550 | 882,563 | 591,693 | 0.460917 | 0.659357 |

| Attempt | Guest process read bytes | Guest process write bytes | Heap bytes before exit | Kernel process peak RSS bytes | Successful sync calls through exit | Sampled temp peak bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 6,049,792 | 102,400 | 227,404,832 | 95,236,096 | 4,066 | 0 |
| 2 | 6,053,888 | 65,536 | 231,321,760 | 95,731,712 | 4,052 | 0 |
| 3 | 6,053,888 | 65,536 | 229,286,296 | 97,517,568 | 4,050 | 0 |

Across the three interrupted processes, 7,021 source records were consumed, with 3,088,961 logical frame bytes and 2,070,916 replay key/payload bytes. The stages overlap and must not be added together. Guest process I/O before diagnostic publication totaled 18,157,568 read bytes and 233,472 write bytes. Waited-child CPU through exit totaled 2.011468 s, wrapper CPU before writing its report totaled 0.399140 s, and 12,168 sync calls succeeded. These costs precede the final successful restart; they are not inferred from its counters.

The final unarmed restart verified offset 1,000 for every group and full ISR in 12.716 s. The complete interrupted-attempt/recovery interval, including offline preparation and result collection, was 20.740 s. Final shutdown took another 0.405 s. The interrupted attempts used advisory file-cache eviction; the final restart retained pages from partial replay and is explicitly a warm-page-cache recovery.

| Final ready-state scope | Broker CPU s | Guest read bytes | Guest write bytes | Successful sync calls | Cumulative allocated bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Restarted broker, process lifetime | 1.390 | 8,192 | 204,800 | 4,348 | 319,934,944 |
| Both surviving brokers, interval delta | 2.910 | 0 | 1,060,864 | 934 | 52,255,208 |

The final broker decoded 8,024 source records, read 3,530,262 logical frame bytes and wrote 11,179 index bytes. Its ready-state sampled heap peak was 216,708,416 bytes and kernel RSS peak was 131,796,992 bytes. Shutdown-inclusive waited-child CPU was 1.653655 s with 4,358 successful sync calls; it is a different accounting boundary and is not added to the ready-state values.

## Interpretation and reproduction

- Each process report has a distinct token, and the wrapper removes any old exit report before starting. A missing report cannot silently reuse a previous process's counters.
- Broker pre-exit counters exclude diagnostic publication. Waited-child CPU includes the broker, tracer and exit work; wrapper CPU is separate. Child peak RSS is the largest waited child, not the sum of simultaneous process memory. Block operations in raw `getrusage` output are not converted to bytes.
- Linux guest `/proc` and cgroup I/O are not host-device measurements. Frame, replay, retained-scan and network-transfer work are separate overlapping views. Interface bytes also include Raft, health and monitoring traffic.
- Heap before an injected exit is a synchronous sample, not a proven peak. The wrapper obtained no periodic heap samples before those short crashes. Ready-state heap and temporary-space peaks are sampled at one second; a sampled zero does not prove zero transient use. RSS is a kernel high-water mark. Survivor peaks remain lifetime peaks and cannot be subtracted to obtain interval peaks.
- Full raw counters include container CPU/memory/I/O, allocation counts, interface traffic, retained `.log` bytes and total store sizes. Distinct source reads on later attempts are counted again; failed work is not hidden.

Run on an isolated Linux/Docker fixture with a fresh output path:

```sh
RUN_E2E_CONSUMER_HISTORY_REPLAY_CRASH=1 \
CURSUS_CLEANUP_PORT_OFFSET=32000 \
CURSUS_HISTORY_REPLAY_CRASH_OUTPUT=/tmp/history-replay-crash-new.jsonl \
go test ./test/e2e-cluster -run '^TestConsumerHistoryReplayCrashResources$' -count=1 -timeout=25m -v
```

The ordinary image has no active crash hook. The dedicated image requires both the `e2e_faults` build tag and its test-only environment/arm file. Production deployment and data are outside this experiment.
