<div align="center">

<img src=".github/cursus-readme.png" alt="Cursus" width="60%" height="60%">

[![GitHub](https://img.shields.io/github/stars/cursus-io/cursus.svg?style=social)](https://github.com/cursus-io/cursus)
[![Latest Release](https://img.shields.io/github/v/release/cursus-io/cursus?include_prereleases&label=release&color=00ADD8)](https://github.com/cursus-io/cursus/releases)
[![Unit Tests](https://github.com/cursus-io/cursus/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/cursus-io/cursus/actions/workflows/unit-tests.yml)
[![E2E Tests](https://github.com/cursus-io/cursus/actions/workflows/e2e-tests.yml/badge.svg)](https://github.com/cursus-io/cursus/actions/workflows/e2e-tests.yml)
[![CodeQL](https://github.com/cursus-io/cursus/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/cursus-io/cursus/actions/workflows/codeql-analysis.yml)
[![GoSec](https://github.com/cursus-io/cursus/actions/workflows/gosec-analysis.yml/badge.svg)](https://github.com/cursus-io/cursus/actions/workflows/gosec-analysis.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/cursus-io/cursus)](https://goreportcard.com/report/github.com/cursus-io/cursus)
[![CodeCov](https://img.shields.io/codecov/c/github/cursus-io/cursus)](https://codecov.io/gh/cursus-io/cursus)
![Go Version](https://img.shields.io/github/go-mod/go-version/cursus-io/cursus)
[![Go Reference](https://pkg.go.dev/badge/github.com/cursus-io/cursus.svg)](https://pkg.go.dev/github.com/cursus-io/cursus)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

</div>

Cursus is a lightweight, partitioned-log message broker written in Go. It runs as a standalone broker or as a Raft-backed cluster and exposes a compact length-prefixed TCP protocol for producers, consumers, administration, and event sourcing.

## Capabilities

- Partitioned topics with key-hash or round-robin routing and per-partition ordering.
- Buffered, batched disk persistence, configurable segment rolling, mmap reads, retention by time or size, and recovered high-watermark checkpoints.
- Durable consumer groups with generation fencing, broker-owned monotonic `nextOffset` commits, restart resume, and group/partition isolation.
- Idempotent producers and broker-managed consume-process-produce transactions with producer fencing, durable coordinator state, transaction markers, recovery, and `read_committed`/`read_uncommitted` isolation.
- Raft-backed metadata, partition-leader routing, replication quorum checks, follower catch-up, and leader redirects.
- Event streams with optimistic concurrency, committed-tail reads, durable indexes, and quorum-replicated snapshots.
- TLS, client token authentication and authorization, per-topic ACLs, broker-to-broker mTLS/internal command boundaries, health probes, and Prometheus metrics.
- Go SDK in this repository; Java and Python SDKs are maintained in their own repositories against the same wire contract.

## Contract Boundaries

| Area | Current contract |
|---|---|
| Ordering | Ordered within one partition; no ordering across partitions. |
| Consumer delivery | At-least-once when processing finishes before committing `lastProcessedOffset + 1`. Committing first can produce at-most-once behavior. |
| Transactions | Atomic broker visibility for staged output records plus one fenced consumer offset scope `(topic, group, member, generation)`. External database or service side effects are outside the broker transaction. |
| Read isolation | `read_committed` is the default and hides open/aborted transactions. `read_uncommitted` exposes the raw committed partition log, including control records. |
| Retention | Time/size deletion is supported. Log compaction is not implemented; `log_cleanup_policy` is normalized to `delete`. |
| Protocol | Cursus-native TCP framing and commands. This project does not claim byte compatibility with another broker protocol. |

## Quick Start

```bash
docker pull ghcr.io/cursus-io/cursus:latest
docker run --rm -p 9000:9000 -p 9080:9080 -p 9100:9100 ghcr.io/cursus-io/cursus:latest
```

The client protocol listens on `9000`, health probes on `9080`, and Prometheus metrics on `9100` by default. Production deployments should mount durable storage and configure TLS/authentication explicitly.

## Documentation

Start with the [documentation index](docs/README.md), then use the [wire protocol specification](docs/protocol-spec.md), [API reference](docs/reference/api-reference.md), [configuration guide](docs/user-guide/configuration.md), and [SDK overview](docs/sdk-overview.md) as the authoritative contracts.

## Community

Cursus is developed openly. Use GitHub Issues for bug reports, design proposals, and compatibility questions. Contributions should preserve documented protocol behavior and include tests for changed guarantees.
