# Benchmarks

Cursus ships Docker end-to-end correctness benchmarks and Go storage microbenchmarks. Published throughput numbers are meaningful only with the workload, hardware, durability, topology, and correctness counters attached.

## Docker Workloads

| Topology | Compose file | Records | Partitions | Producer/consumer |
|---|---|---:|---:|---|
| Standalone | `test/docker-compose.yml` | 100000 | 12 | one idempotent batched publisher, one polling group consumer |
| Cluster | `test/cluster/docker-compose.yml` | 100000 | 12 | one multi-broker publisher, one coordinator/leader-routed polling group consumer |

The source settings are `test/publisher/config.yaml`, `test/consumer/config.yaml`, and the matching files under `test/cluster`. Benchmark storage uses tmpfs in the checked-in compose workloads, so results emphasize broker/client behavior and do not represent durable physical-disk latency.

## Running

```bash
RUN_E2E_BENCHMARK=1 go test -v -timeout 30m ./test/e2e-benchmark/...
```

PowerShell:

```powershell
$env:RUN_E2E_BENCHMARK = "1"
go test -v -timeout 30m ./test/e2e-benchmark/...
```

`make bench` runs the standalone compose workload, checks container exit codes
and correctness/fatal markers, prints compose logs, and always tears down the
stack. The Go suite is the authoritative way to validate both topologies.

Docker Engine with Compose, sufficient CPU/memory for the clients and three
brokers, and unused fixture container names are required. The suite builds each
stack, starts it with `--no-build --force-recreate`, checks client exit and
summaries, and removes containers and volumes. Fixed-name cleanup is limited to
containers whose Compose labels identify this repository's test stacks.
Without `RUN_E2E_BENCHMARK=1`, or with `go test -short`, the Docker tests skip.

Run one topology:

```bash
RUN_E2E_BENCHMARK=1 go test -v -timeout 15m -run '^TestStandaloneBenchmark$' ./test/e2e-benchmark/...
RUN_E2E_BENCHMARK=1 go test -v -timeout 15m -run '^TestClusterBenchmark$' ./test/e2e-benchmark/...
```

For direct log inspection, start one stack and tear it down with the matching
compose file after inspection:

```bash
docker compose -f test/docker-compose.yml up --build --force-recreate
docker compose -f test/docker-compose.yml down -v --remove-orphans

docker compose -f test/cluster/docker-compose.yml up --build --force-recreate
docker compose -f test/cluster/docker-compose.yml down -v --remove-orphans
```

The standalone clients are `bench-publisher`/`bench-consumer`; cluster clients
are `broker-publisher`/`broker-consumer`.

## Durable Storage Workload

The checked-in workloads use tmpfs and are not evidence of physical-disk
durability latency. To run the same standalone correctness workload against a
host directory on the intended filesystem, use the separate durable compose
file. The directory must already exist, be dedicated to the run, and have
enough space for broker logs and temporary compaction files.

```powershell
$env:RUN_E2E_DURABLE_BENCHMARK = "1"
$null = New-Item -ItemType Directory -Force "D:\cursus-benchmark-results"
$env:CURSUS_DURABLE_BENCHMARK_LOG_DIR = "D:\cursus-benchmark-logs"
$env:CURSUS_DURABLE_BENCHMARK_RESULT = "D:\cursus-benchmark-results\run.json"
$env:CURSUS_BENCHMARK_REVISION = (git rev-parse HEAD)
$env:CURSUS_DURABLE_BENCHMARK_STORAGE = "NVMe model, filesystem, mount options"
go test -v -timeout 30m ./test/e2e-benchmark/... -run '^TestDurableStandaloneBenchmark$'
```

The result is versioned JSON containing revision, Go/runtime metadata, the
operator-declared storage identity, post-run file/byte counts, elapsed time,
benchmark summaries, and mandatory publish/consume correctness counters. The
harness refuses to run or publish a result when that metadata or any required
counter is missing. It is still a standalone workload; transaction, retention,
compaction, and replica-recovery soak scenarios are separate operational
validation runs and must be reported with their own workload settings.

## Correctness Gate

The harness fails on non-zero container exit, timeout, incomplete/verification markers, anchored panic/fatal logs, failed publishes, missing records, duplicate message IDs, or duplicate logical offsets. Multi-digit counters are parsed numerically. A success banner without zero correctness counters is not sufficient.

Expected consumer summary fields include:

```text
Total Messages
Elapsed Time
Overall TPS
Duplicate (MessageID) : 0
Duplicate (Offset)    : 0
Message missing       : 0
```

The test output filters successful logs to benchmark summaries. Full compose service logs are collected when a process times out or verification fails.

## What It Measures

- idempotent producer throughput and retries,
- partition distribution,
- real group join/sync/heartbeat behavior,
- broker-owned offset resume and batch commit,
- polling consumer throughput,
- duplicate/missing correctness at 100000 records,
- standalone versus three-broker routing overhead.

## What It Does Not Prove

- physical disk fsync latency (the compose logs use tmpfs),
- long-duration retention or segment aging,
- streaming-mode throughput,
- event-sourcing 100000-record throughput,
- transaction commit throughput or every crash window,
- cross-zone network behavior,
- production TLS/mTLS certificate overhead.

Those require purpose-built workloads and failure tests. Do not infer them from this benchmark.

## Troubleshooting

- Commit errors or duplicates: inspect coordinator redirects, group generation
  and member state, and `BATCH_COMMIT` responses.
- Consumer timeout: compare unique published records, per-partition totals, and
  committed offsets.
- Broker not ready: inspect `/ready`, Raft leader/ISR state, and container-name
  collisions.
- Slow cold build: prebuild the images; preserve correctness counters and fatal
  checks.
- For deeper traces, temporarily use `log_level: debug` in test configs and
  restore INFO after diagnosis.

## CI Policy

Normal E2E runs on pull requests. The Docker benchmark runs only on pushes to `main`, after normal E2E succeeds, in the final step of `.github/workflows/e2e-tests.yml`. It builds its own standalone/cluster compose images; Docker layer caching may help, but the active E2E containers are not reused.

## Storage Microbenchmarks

```bash
make bench-disk
make bench-serialize
```

Equivalent commands:

```bash
go test ./pkg/disk/ -bench=. -benchmem -count=3 -timeout=120s
go test ./pkg/disk/ -bench=BenchmarkSerializeDiskMessage -benchmem -count=5 -timeout=60s
```

Microbenchmarks isolate serialization and disk-handler operations from the network and coordinators. Record Go version, OS/filesystem, CPU, disk/tmpfs, benchmark count, and configuration when comparing results.

## Reporting Results

A useful report includes:

- commit SHA and dirty status,
- standalone or cluster topology,
- record size/count and partition count,
- acks/idempotence/isolation settings,
- storage medium and mount options,
- CPU/memory/container limits,
- producer throughput and p95/p99 latency,
- consumer TPS and elapsed time,
- all retry/failure/missing/duplicate counters,
- broker errors, leader changes, and commit redirects.

Do not commit generated benchmark result files unless they are intentionally curated evidence for a release or published performance analysis.
