# Recovery scheduling and resource admission

The defaults below were selected using the mixed-load experiments described
below. They are not Kafka defaults or a claim of physical disk QoS.

## Baseline and design comparison

Initial measurements used official main
`bb7dc59383c2e77c886f3b7c05bd79c9d70e21eb`, fetched on 2026-10-23.
After cleanup progress and broker-time retention landed during this work, both
implementation and the final comparison baseline advanced to
`2260df12c8ea9c241e6307c0b8aaf1b10291475e`.
The earlier audit at `a49138e7922aa66b14cb4482a8b084e5b9c1b8cd` correctly
identified a sequential pass with **30-second turns and a 64-step cap**.
Those bounds existed; ten preceding 30-second turns imply roughly five minutes
of queue delay, but this is a code-derived scenario, not an observed incident.
Bootstrap already had two shared inbound permits, six source owners, seven-minute
receiver attempts, 120-second source creation deadlines, five-minute source TTLs,
and default storage admission of 8 GiB per transfer / 32 GiB aggregate.

The comparison used these primary sources before implementation:

* [KIP-74](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/65864887/KIP-74+Add+Fetch+Response+Size+Limit+in+Bytes):
  aggregate and per-partition fetch bounds, rotating partitions, and allowing the
  first message to exceed the ordinary response budget to guarantee progress.
* [KIP-73](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/65864833/KIP-73+Replication+Quotas):
  leader/follower byte accounting and selective replication throttling; optimistic
  accounting allows overshoot bounded by the fetch sizes.
* Kafka **4.1.0**
  [ReplicationQuotaManager](https://github.com/apache/kafka/blob/4.1.0/core/src/main/java/kafka/server/ReplicationQuotaManager.java)
  uses time-windowed `SimpleRate` sensors, not these token reservations.
  [RemoteLeaderEndPoint](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/RemoteLeaderEndPoint.scala)
  and [ReplicaManager](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/ReplicaManager.scala)
  throttle selected partitions when quota is exceeded and the replica is out of
  sync. [ReplicaFetcherThread](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala)
  records selected replication bytes, including in-sync bytes: accounting and
  eligibility to throttle are distinct.

Cursus adopts bounded fetches, rotation, valid-unit progress and accounting on
both ends. Its durable lineage/witness proofs, prefix refinement, snapshot
publication and bootstrap phases require additional continuations. Kafka does
not implement the same scheduler. Healthy ISR writes and control are exempt
from Cursus recovery admission; this differs from Kafka's selected-byte accounting.

Before coding, the regression contract required mixed slow/unresponsive sources,
large/small lag, continuous foreground requests, bytes and storage work as well as
request counts, new arrivals, fake-clock quota/backoff tests, cancellation/join,
source/generation changes, oversized progress and unchanged exact ISR fences.

## Ownership and fairness

Production `StartReplicaCatchupVerified` owns a fixed worker pool. Each dispatch
performs one phase: build a request/proof, fetch and apply a page (or refine one
prefix probe), continue one snapshot page, or submit a terminal proof. The ready
cursor rotates; blocked quota/source slots and independent source/partition
retry deadlines are skipped. Backoff starts at 100 ms and caps at 30 seconds.
Retry counts saturate; identities never become metric labels.

One additional control worker may advance a healthy ISR replica's committed HWM
without recovery tokens. It rechecks ISR membership and local LEO under the
existing gates and cannot perform fetch, prefix proof, rollback or bootstrap.
Inventory observes partition watermarks without waiting behind an apply/fsync.
Turn deadlines cancel cooperatively; slots remain owned until actual I/O exits.

One partition owns at most one worker and continuation. A changed source,
incarnation, leader/lifecycle or replica set cancels its old owner. The old
concurrency lease remains charged until that owner exits. Shutdown cancels,
drains completions and joins every worker before dependencies close. Snapshot
revision/cursor and the actual terminal witness survive yields; authority is
rechecked before publication and proof submission. A moved committed HWM
requires another exact proof. No durability, dedup, divergent-tail or ISR
validation was removed.

Queue capacity must cover `max_partitions`, avoiding overflow starvation behind
permanently active continuations. Entries allocate on discovery; nonexistent
local partitions belong to the existing materialization scheduler. That
scheduler retains its own rotation, cadence and lifecycle ownership. Bootstrap
retains its process-owned attempts and partition exclusion. The synchronous
`RunReplicaCatchupOnceVerified` compatibility API retains its existing bounded
pass behavior; production does not use it.

## Cost classes and normal-traffic reservation

| Work | Admission and accounting |
| --- | --- |
| Incremental data and prefix responses | Broker and peer token buckets plus worker/source slots. Full returned bytes, including oversized first units, are charged. |
| Local proof construction/refinement and apply | Bounded scheduler workers and turn deadlines; existing read/hash/write/sync work metrics remain authoritative. Proof submission builds eligibility for just one partition. |
| Event snapshot recovery | One catalog record per page, receiver worker/byte admission and source nonblocking admission; revision/witness checks remain. |
| Bootstrap chunks | Same broker/peer byte buckets on source and receiver; reservations precede locks. Partial socket reads are charged. |
| Export, checksum, staged writes and install | Atomic-work subset of broker concurrency, at most `max(1, workers/4)` owners; acquired before storage/FSM gates. Existing logical work metrics count their I/O. Network byte limits are not a disk byte-rate guarantee. |
| Healthy ISR replication | Existing normal internal request capacity; does not wait for recovery tokens. |
| Heartbeat, exact proof, membership/Raft and release | Control path remains available when recovery tokens are empty. Discovery recovery requests use at most one quarter of connection capacity; release/control bypass that subset. Snapshot list/fetch/manual catch-up requests use at most one quarter of internal request/byte reservations when at least two internal slots exist. |

The existing client/internal request split is preserved. Recovery waits on its
subset before occupying a shared internal reservation. Tiny configurations with
only one internal slot cannot reserve concurrent control capacity; use at least
four total inflight requests for a distributed broker. FSM/storage locks
are not held while waiting for recovery byte or operation admission. Atomic
storage work retains its slot until its actual completion, including after
cancellation. Source creation and receiver lifetime/storage limits still apply.
Source transfer leases extend through response serialization and socket writes;
finishing the handler or canceling its context does not release a live writer.

Rate limiting must not make every large bootstrap expire and restart. With a
budget installed, receiver lifetime is the original seven-minute allowance plus
`max(ceil(16*T/broker_rate), ceil(4*T/peer_rate))` seconds, where `T` is
`bootstrap_max_transfer_bytes`. The factors size for six source and two inbound
owners, twofold encoded-byte overhead and competing work involving one peer.
This is a finite sizing allowance, not a guaranteed share under arbitrary load.
Source exports retain their **five-minute idle expiry**: only a successful chunk
renews it, and never beyond the computed receiver lifetime minus two minutes.
Unresponsive receivers therefore cannot retain exports for the whole allowance.
Creation still has its 120-second deadline; shutdown and authority invalidation
still cancel and join owners. Certificates retain their original finite expiry.
The default maximum receiver lifetime is 36 h 31 m 32 s; configurations requiring
more than seven days are rejected. Lower `bootstrap_max_transfer_bytes` or raise
rates to satisfy this constraint. Storage bytes and owner counts remain bounded,
but progressing exports may occupy them longer. Use compatible rate/transfer
settings across brokers; an older source still has its original fixed TTL.

Peers share debt by stable broker ID across incremental, snapshot and bootstrap
work. Incarnation changes invalidate retry/authority state, not byte debt.
There are at most 4096 peer ledgers and 32 admission waiters. An idle ledger can
be evicted only after all its debt is repaid.

## Oversized progress and compatibility

`ReplicaCatchupRequest.max_bytes` is an optional byte hint. Omission preserves the
old storage/wire maximum; old sources may ignore the hint. A first valid record
exceeding the hint is retried once with the existing maximum and one-record
limit. It is never skipped. Multiple records are reduced to a bounded response
without advancing past an unsent record, including compacted offset holes.

A known-size unit larger than burst starts from full buckets and incurs debt;
only one such known oversized reservation is active. For a response whose size
is not known in advance, admission is optimistic: at most `workers` responses
(`source_concurrency` per peer) can exceed the quantum, each bounded by the
64-MiB wire frame plus 64-KiB framing allowance. Complete cost becomes debt before
new work is admitted. This is a finite overshoot bound, not a strict instantaneous
rate cap. A very large unit can therefore increase later quota wait.

The cleanup-reader progress contract is shared with the separate cleanup work:
an ordinary quantum is not a validity maximum; retain the existing hard record
limit and allow one valid oversized first unit. This change does not implement
or duplicate coordinator cleanup paging.
The merged implementation is documented in [cleanup record progress](core/cleanup-record-progress.md);
its oversized replay reservation and cleanup continuation remain separate.

## Configuration

The scheduler and admission are enabled on distributed startup. Configure the
`recovery` YAML object or the corresponding environment variables, then restart.
Explicit zero/negative values fail validation; zero is never unlimited. A zero
programmatic `RecoveryConfig` inherits defaults for compatibility with callers.

| YAML key | Environment suffix after `RECOVERY_` | Default | Unit / accepted range |
| --- | --- | --- | --- |
| `bytes_per_second` | `BYTES_PER_SECOND` | 1048576 | bytes/s, 1..2^40 |
| `burst_bytes` | `BURST_BYTES` | 2097152 | bytes, positive, at most 67174400 |
| `source_bytes_per_second` | `SOURCE_BYTES_PER_SECOND` | 524288 | bytes/s, positive, at most broker rate |
| `source_burst_bytes` | `SOURCE_BURST_BYTES` | 1048576 | bytes, positive, at most broker burst |
| `quantum_bytes` | `QUANTUM_BYTES` | 1048576 | bytes, 4096..16777216, at most source burst |
| `workers` | `WORKERS` | 4 | simultaneous turns, 1..32 |
| `source_concurrency` | `SOURCE_CONCURRENCY` | 1 | simultaneous operations per peer, 1..workers |
| `queue_entries` | `QUEUE_ENTRIES` | 100000 | entries, workers..1000000; must cover max_partitions |
| `turn_timeout_ms` | `TURN_TIMEOUT_MS` | 5000 | milliseconds, 100..30000 |

Rates count bytes, not decimal MB or messages. Burst permits immediate work after
idle time and never accumulates beyond its configured bound. The budget is shared
by inbound/outbound recovery in one process; both brokers charge their own work.
Rate/transfer-size combinations must also pass the seven-day lifetime bound.
For rollback, restore the prior broker binary/configuration; the optional fetch
hint does not change stored records or relax the existing recovery protocol.

Lower rates/bursts reduce recovery interference but lengthen under-replicated
time, and recovery cannot converge while writes advance faster than its service
rate. Compare foreground throughput/errors/p95/p99 with queue first-service,
oldest pending age, completed residence, running turns, admission deferrals and
`cursus_recovery_work_total` read/write/hash/transfer/sync deltas. Residence alone
does not describe unfinished work; inspect pending age and actual ISR state too.
Queue length and oldest age include admitted ISR watermark maintenance; use ISR gauges to distinguish unfinished recovery. No per-topic/partition/source metric labels are added.

## Reproducing the mixed-load experiment

`scripts/recovery-mixed-load.ps1 -OutputDirectory <absolute-results-directory>`
builds a fault-instrumented broker and starts only `cursus-recovery-scheduler-*`
containers, with fresh volumes, 2 CPUs / 2 GiB per broker and `GOMAXPROCS=2`.
It requires PowerShell 7, Docker Compose and a local Go toolchain. Ports
29001..29003, 28000..28002, 29101..29103 and 29081..29083 must be free.
It refuses existing measurement containers and never operates on `broker-1/2/3`.
Use `-Offline -ModuleCache <populated-volume>` to build without network access.

For baseline, pass `-Variant baseline -SourcePath <baseline-checkout>`. At the
baseline SHA above, copy only `pkg/cluster/recovery_fault_default.go` and
`recovery_fault_e2e.go`, and insert `injectedRecoverySourceDelay(ctx, request.Topic)`
immediately before `FetchReplicaCatchupContext` in the source handler, returning
`internalClusterError(err)` on failure. The hook requires both `e2e_faults` and
`CURSUS_E2E_FAULTS=1`; ordinary builds contain a no-op. The runner uses the same
current test harness for both binaries. Do not copy the scheduler into baseline.

The test stops the isolated third broker, accumulates lag of 1 / 128 / 4096 / 2
records (8 KiB each) across unresponsive / slow / large / small topics, then
restarts it. The first source has a context-bounded 2-minute injected stall and
200-ms slow responses; the small and large topics use the other source. A
separate two-replica topic continuously produces, consumes and commits offsets
for 50 seconds. Small recovery is timed from restart to the first source RPC and
to authoritative ISR membership, not just local LEO. Initial group coordination
is outside this measurement window. JSON includes request counts, errors,
percentiles, recovery logical-work counters and per-process `/proc` observations.
`VmHWM` is process-lifetime peak; source CPU/I/O comparisons use before/after
deltas, while restarted receiver counters start with its new process. These are
Docker-host observations, not a production device-isolation guarantee.

`TestRecoveryMixedLocalWorkload` separately uses controlled transport and real
partition storage for a fixed 40-second window. Its metadata append is not an
offset-commit RPC; only the cluster test supports that claim. Queue tests add
new work continuously while earlier entries are blocked and verify rotation.
Fake-clock budget tests cover exact refill, burst, debt and independent peer
limits; controlled transport tests retain canceled owners until their I/O exits.

## Measurements used to select defaults

The initial `bb7dc59` local-storage experiment ran for 40 seconds with a silent
source, a 25-ms source, a 10,000-record large lag and a two-record small lag.
Baseline small first-service / exact ISR completion were 32,813 / 32,829 ms.
The first 16-MiB/s scheduler candidate reached them in 1 / 17 ms, but transferred
49,673,120 bytes versus 1,160,324 and worsened foreground append p99 from 14,302
to 18,386 microseconds. Faster recovery alone was not accepted as protection of
normal traffic.

Real-TCP 50-second observations on the same baseline revision were:

| Profile / run | Small first RPC / ISR (ms) | Successful commit count | Produce / consume / commit p99 (ms) | Errors |
| --- | ---: | ---: | ---: | --- |
| Sequential baseline 1 | 29232 / 29496 | 200 | 237.503 / 4.534 / 198.553 | 0 |
| Sequential baseline 2 | 30636 / 31042 | 121 | 358.280 / 15.907 / 372.697 | 1 produce failure |
| Scheduler 16 MiB/s | 8523 / 9607 | 130 | 450.081 / 18.237 / 320.541 | 0 |
| Scheduler 1 MiB/s, run 1 | 2811 / 3202 | 215 | 270.684 / 4.403 / 174.079 | 0 |
| Scheduler 1 MiB/s, run 2 | 8720 / 9240 | 107 | 424.433 / 4.451 / 358.246 | 0 |

The 16-MiB/s candidate used 26.61 aggregate process CPU seconds, 35,490,073
receiver catch-up wire bytes and 414,204,185 receiver lineage-read bytes. The
1-MiB/s second run used 17.29 CPU seconds, 21,844,706 wire bytes and 241,832,960
lineage-read bytes. Sum of process-lifetime VmHWM peaks was 1305.77 / 1252.63 MiB
respectively; this sum is not a simultaneous peak. Receiver exporter collection
failed in the first 1-MiB/s run, so its missing work counters are not treated as
zero. Foreground operations share CPU, hashing, allocation, disk writes and
flushes with recovery. The smaller network budget reduces admitted work but
does not remove cumulative durable proof regeneration after each page.

These runs show host/run variability and do **not** demonstrate that every
foreground percentile improves. They justify a conservative 1-MiB/s broker,
512-KiB/s peer default, bounded operation concurrency and explicit monitoring.
In the second budgeted run, large and slow replicas remained outside ISR at
50 seconds; baseline had restored both. This is the measured availability cost
of throttling. The permanently stalled topic remained under-replicated in both.
Do not infer a production SLO or statistical confidence from these short samples.

### Final comparison on current main

Both final binaries use base `2260df12c8ea9c241e6307c0b8aaf1b10291475e` and
Go 1.26.2, matching the repository's CI toolchain. The checked-in runner executed
them sequentially with fresh Docker volumes and the same 50-second workload.
Both tests passed; each produce/consume/commit chain had no errors or empty reads.

| Observation | Sequential baseline | Scheduler, default 1 MiB/s |
| --- | ---: | ---: |
| Small first source RPC (ms from restart) | 33715 | 6955 |
| Small exact ISR membership (ms from restart) | 34134 | 7301 |
| Successful produce / consume / commit count | 146 / 146 / 146 | 126 / 126 / 126 |
| Produce p95 / p99 (ms) | 292.336 / 454.144 | 300.820 / 403.674 |
| Consume p95 / p99 (ms) | 6.128 / 9.646 | 6.612 / 12.628 |
| Offset commit p95 / p99 (ms) | 227.292 / 492.198 | 270.916 / 331.755 |
| Receiver catch-up transfer bytes | 36646717 | 24024190 |
| Receiver lineage read / canonical hash bytes | 60227255 / 45564929 | 266563710 / 200864368 |
| Receiver lineage sync calls | 284 | 524 |
| Aggregate process CPU seconds | 21.48 | 24.02 |
| Aggregate kernel read / write bytes (MiB) | 27.69 / 59.12 | 29.95 / 44.18 |
| Sum of process-lifetime VmHWM peaks (MiB) | 1305.36 | 1173.85 |
| Large / slow replica in ISR at 50 seconds | yes / yes | no / no |

Small exact recovery was about 79% faster in this run. This is not an overall
foreground throughput improvement: fewer request chains completed, and consume
p99 increased. Smaller resumable pages repeat cumulative durable lineage proof
work; the extra reads, hashing and syncs are explicitly retained rather than
weakening verification. Network bytes fell about 34%, but CPU and logical proof
reads did not. Operation limits and reserved request capacity bound interference;
the network quota alone is not a physical-I/O or latency guarantee. Deployments
must tune against their foreground SLO and monitor the longer under-replicated
interval. Source outage and a sustained write rate above the recovery allowance
can still prevent convergence.

All initial and final raw observations, including the failed baseline sample,
are in [the measurement directory](benchmarks/recovery-2026-10-23/README.md).
Final commands used `-GoImage golang:1.26.2 -Offline` with a populated module
volume; the documented runner's default Go 1.25.0 reproduces the initial series.
