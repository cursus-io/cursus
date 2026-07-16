# Getting Started

This guide starts one local broker and exercises it with the in-repository Go SDK examples. See [Installation](installation.md) for build/container details and [Configuration](configuration.md) before production use.

## Start With Docker

```bash
docker pull ghcr.io/cursus-io/cursus:latest
docker run --rm --name cursus \
  -p 9000:9000 -p 9080:9080 -p 9100:9100 \
  -v cursus-data:/root/broker-logs \
  ghcr.io/cursus-io/cursus:latest
```

The default ports are:

| Port | Purpose |
|---:|---|
| 9000 | length-prefixed TCP client protocol |
| 9080 | `/live`, `/ready`, `/health` |
| 9100 | Prometheus `/metrics` |

In another terminal:

```bash
curl -f http://localhost:9080/live
curl -f http://localhost:9080/ready
curl -f http://localhost:9100/metrics
```

For production, mount a configuration, enable client TLS/auth, configure a separate broker-internal mTLS listener for clusters, and use durable storage.

## Start From Source

Cursus requires Go 1.25.0 or newer.

```bash
git clone https://github.com/cursus-io/cursus.git
cd cursus
make build
./bin/cursus
```

Development mode:

```bash
make run
```

`make build` creates `bin/cursus` and `bin/cursus-cli`. Benchmarks are Docker/Go test workloads rather than a third binary.

## Publish And Consume With Go Examples

With the broker running:

```bash
cd examples
go run ./publisher
```

Then run the consumer:

```bash
cd examples
go run ./consumer
```

`examples/config.yaml` uses `localhost:9000`, topic `example-topic`, and group `example-group`. The default Go consumer uses broker-owned offset resume and `read_committed`; after successful processing it commits the next offset. Edit the example configuration or construct `sdk.ConsumerConfig` directly for `auto_offset_reset`, TLS/auth, and other settings.

## CLI Scope

```bash
./bin/cursus-cli
```

The current CLI creates local broker components and executes commands in-process. It is useful for command exploration, but it is not a remote network administration client for an already running broker. Use the Go/Java/Python SDKs or a framed protocol client for remote operations.

Raw `nc localhost 9000` text is not valid because every TCP payload requires a 4-byte big-endian length prefix.

## Standalone And Cluster Modes

Standalone mode is the default. Cluster mode adds Raft metadata, partition leaders, group/transaction coordinator routing, replication quorum, and broker-internal authentication/mTLS. Client commands remain the same and redirects identify the correct owner.

Use the checked-in E2E compose topology as a development reference:

```bash
make e2e
```

The 100000-record standalone/cluster benchmark is intentionally opt-in:

```bash
RUN_E2E_BENCHMARK=1 go test -v -timeout 30m ./test/e2e-benchmark/...
```

## Essential Semantics

- offsets are `nextOffset`; commit `lastProcessedOffset + 1`,
- process before commit for at-least-once delivery,
- `read_committed` is the default; `read_uncommitted` exposes raw control records,
- one broker transaction may commit offsets for one `(topic, group, member, generation)` scope,
- initialize a new producer epoch before each new transaction; finalization retry keeps the old epoch,
- ordering is per partition,
- retention deletion can create offset gaps,
- log compaction is not implemented.

## Next Steps

- [Wire Protocol](../protocol-spec.md)
- [SDK Overview](../sdk-overview.md)
- [Consumer Groups](../core/topic/consumer-groups.md)
- [Event Sourcing](event-sourcing.md)
- [Observability](../reference/observability.md)
- [Performance](../reference/performance.md)
