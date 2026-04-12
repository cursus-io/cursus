# Benchmark Verification

Verifies that produce and consume work correctly under load in both standalone and cluster modes.

## Prerequisites

- Docker and docker-compose installed
- Project builds successfully (`go build ./...`)

## Standalone Benchmark

Runs a single broker with one publisher (100k messages) and one consumer.

```bash
cd test
docker-compose up --build --force-recreate
```

### Expected Outcome

| Component | Success Indicator |
|-----------|-------------------|
| Publisher | Logs `All messages published` or exits with code 0 |
| Consumer  | Logs `All messages consumed` and `Consumer closed gracefully` |

### Verification

```bash
# Check publisher completed
docker logs bench-publisher 2>&1 | tail -5

# Check consumer completed
docker logs bench-consumer 2>&1 | tail -5

# Teardown
docker-compose -f test/docker-compose.yml down -v
```

## Cluster Benchmark (3-node)

Runs a 3-node Raft cluster with publisher and consumer.

```bash
cd test/cluster
docker-compose up --build --force-recreate
```

### Expected Outcome

| Component | Success Indicator |
|-----------|-------------------|
| Brokers   | All 3 pass healthcheck, Raft leader elected |
| Publisher  | Logs `All messages published` or exits with code 0 |
| Consumer   | Logs `All messages consumed` and `Consumer closed gracefully` |

### Verification

```bash
# Check all brokers healthy
for i in 1 2 3; do curl -s http://localhost:908$i/health; echo; done

# Check publisher
docker logs broker-publisher 2>&1 | tail -5

# Check consumer
docker logs broker-consumer 2>&1 | tail -5

# Teardown
docker-compose -f test/cluster/docker-compose.yml down -v
```

## Automated Test

Run benchmark verification as Go tests:

```bash
# Both standalone and cluster
go test -v -timeout 10m ./test/e2e-benchmark/

# Standalone only
go test -v -timeout 10m -run TestStandaloneBenchmark ./test/e2e-benchmark/

# Cluster only
go test -v -timeout 10m -run TestClusterBenchmark ./test/e2e-benchmark/

# Skip benchmark tests in CI fast mode
go test -short ./test/e2e-benchmark/
```

## Troubleshooting

**Tip**: For deeper insights, change the `log_level` config from `info` to `debug` on all components (publisher, consumer, and broker).

- **Consumer never finishes**: Check `docker logs bench-consumer` for join/sync errors
- **Publisher fails**: Check broker healthcheck and topic creation logs
- **Cluster leader not elected**: Check Raft logs on all 3 brokers for election timeout issues
- **Timeout**: Increase `benchmarkTimeout` in `benchmark_test.go` or check resource constraints

