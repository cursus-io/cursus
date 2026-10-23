# Consumer cleanup measurements (2026-10-23)

The identical opt-in test was run three times at incident commit
`aff3c938cb26ba374aba97d162cfa0c15a686c65` and three times with continuations and
checkpointing enabled. Linux Docker Desktop, Go 1.25.0, 2 CPU quota,
`GOMAXPROCS=2`, 2 GiB memory limit, container writable-layer storage. Fixture
construction and normal coordinator startup are outside the timed interval.
The final measurements include Unix parent-directory sync. Before/after runs
execute sequentially without overlapping local validation. Three samples are
not sufficient to claim a general improvement in concurrent probe latency;
host scheduling and run-to-run noise remain visible in the raw observations.
All individual observations are in
[`cleanup-continuations-2026-10-23.json`](../benchmarks/cleanup-continuations-2026-10-23.json).

Each run uses 56,000 historical records across four partitions, including runs
of twenty 60-KiB metadata fields every 512 offsets. Both versions read exactly
56,000 records / 158,984,674 storage-frame bytes in 441 batches and 448 reads.
This measures logical decoded frame bytes, not physical block-device traffic;
four-byte frame lengths and file metadata reads are not included in that byte
counter. It is not a count of currently live consumer metadata.

| Quantity | Incident version | Continuations + checkpoints |
| --- | ---: | ---: |
| Active scan wall time, median | 0.506 s | 1.688 s |
| Process user + system CPU, median | 0.616 CPU-s | 0.742 CPU-s |
| Total allocations, median | 415,128,368 B | 418,639,416 B |
| Peak sampled live heap, median | 7,683,088 B | 7,735,552 B |
| Concurrent operation p99, median of run p99s | 282.3 us | 46.8 us |
| Largest concurrent operation observation | 2,385.2 us | 371.0 us |
| Injected inter-batch wait, per run | 2,200 s | 0.440 s |

Active scan throughput is approximately 111k records/s before and 33k records/s
after at the median. Persisting and checking progress costs CPU, metadata I/O
and fsyncs; it does **not** make the decoder faster. The scheduling improvement
removes fixed polling between successful batches. Adding observed active time
to injected wait yields about 2,200.51 s versus 2.13 s, but these are **modelled
totals**, not measured production or real-time end-to-end runs. In particular,
the baseline was not allowed to sleep for 36 minutes and neither test includes
the initial five-second startup tick. No production speedup ratio is claimed.

The concurrent probe reads an unrelated partition offset and invokes the
coordinator's nonblocking Raft-Apply cleanup check every approximately 1 ms.
It is a local contention probe, not a network request benchmark or a full Raft
workload. Scheduler tests additionally show that a short topic finishes while
25,000 long-topic continuations run, replica checks retain their periodic
deadline, and errors/waits do not inherit the fast continuation rate.

## Restart work

The actual close/reopen test scans an 8,192-record, four-partition log. Before
restart it reads 4,096 records / 1,413,096 bytes. After reopening every disk
handler and restoring stale group state, cleanup reads 4,992 records /
1,722,240 bytes. The saved prefix covers 3,200 records; only 896 already-read
records repeat. Tombstone evidence from before that prefix still permits
deletion only after validation of the entire current committed cut.

These counters exclude the separate normal coordinator startup replay, which
still scans metadata. A lost, torn, incompatible or invalidated checkpoint
falls back to a full cleanup scan. The tests also exercise appended commits,
rollback, retention movement, new target generations and changed partition
identity/token. No persistent current-state index is introduced.

## Validation and limits

Linux Go 1.26.2 race tests passed for coordinator, disk, topic, replication and
FSM packages. Existing transaction-control and malformed-metadata regressions
are retained. Added deterministic tests cover a 54,164-record scheduling model,
byte/time boundaries, cancellation, worker contention, independent backoff,
checkpoint corruption and generation-write failure before truncation.

The 10-ms budget is cooperative: a single page decode, fsync, OS I/O operation
or existing durable-write callback may take longer. The measurements are small
synthetic local workloads with a warm operating-system cache; production disk,
network, Raft load and physical power-loss behavior remain unmeasured. Startup
timeout increases are neither required by this change nor evaluated here.
