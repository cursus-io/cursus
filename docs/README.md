# Cursus Documentation

This index separates normative contracts from implementation notes. When prose conflicts, the wire protocol specification and API reference define client-visible behavior; configuration defaults are defined by `pkg/config`.

## Start Here

- [Getting Started](user-guide/README.md): run a broker and issue basic commands.
- [Installation](user-guide/installation.md): source builds, binaries, Docker, and GHCR images.
- [Configuration](user-guide/configuration.md): files, environment variables, CLI flags, security, storage, cluster, and SDK options.
- [Architecture](architecture.md): standalone and clustered data/control paths and their guarantees.
- [SDK Overview](sdk-overview.md): Go, Java, and Python compatibility plus high-level client behavior.

## Normative Contracts

- [Wire Protocol Specification](protocol-spec.md): framing, commands, responses, errors, groups, transactions, topic policies, and SDK requirements.
- [API Reference](reference/api-reference.md): command-by-command request and response reference.
- [Command Interface](reference/command-interface.md): compact command map and routing rules.
- [Cluster Consumer Contract](cluster-consumer.md): coordinator discovery, partition-leader routing, resume, heartbeat, and rejoin behavior.
- [Consumer Groups](core/topic/consumer-groups.md): durable `nextOffset` lifecycle and delivery guarantees.
- [Event Sourcing](user-guide/event-sourcing.md): aggregate streams, optimistic concurrency, snapshots, and distributed recovery.

## Architecture And Internals

- [Core Systems](core/README.md)
- [Message Flow](core/message-flow.md)
- [Server](core/server.md)
- [Topics And Partitions](core/topic/topics-and-partitions.md)
- [Topic Management](core/topic/topic-management.md)
- [Disk Format](core/storage/disk-format.md)
- [Disk Persistence](core/storage/disk-persistence.md)
- [Segment Management](core/storage/segment-management.md)
- [Platform Optimizations](core/storage/platform-optimizations.md)

## Operations And Evaluation

- [Observability](reference/observability.md): health, readiness, metrics, alerts, and security boundaries.
- [Performance](reference/performance.md): tuning knobs and durability/latency trade-offs.
- [Benchmarks](reference/benchmark.md): tools, workload model, and result interpretation.
- [Benchmark Verification](benchmark-verification.md): local standalone and cluster verification.
- [Comparison](reference/comparison.md): product scope, strengths, and explicit maturity gaps.
- [Release Automation](specs/release-automation-design.md): release workflow and artifacts.

## Contributing

See [Design And Contribution Principles](contributing/README.md). Changes to protocol behavior must update the specification, API reference, relevant SDK documentation, and executable tests in the same pull request.
