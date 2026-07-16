# Product Scope And Trade-offs

Cursus is a Cursus-native partitioned-log broker. Its goal is to provide durable streaming, transactions, event sourcing, and cluster operation with a smaller runtime and protocol surface than large general-purpose streaming platforms. It does not claim wire or on-disk compatibility with another broker.

## Current Capability Map

| Area | Cursus today | Contract boundary |
|---|---|---|
| Deployment | Single Go binary; standalone or Raft-backed cluster | Cluster operation still requires deliberate storage, certificate, and quorum management. |
| Storage | Append-only segments, offset index, mmap reads, configurable rolling, time/size deletion, corruption recovery | Log compaction and a general timestamp index are not implemented. |
| Ordering | Stable order within one partition | No cross-partition total order. |
| Consumer groups | Dynamic membership, generation/owner fencing, monotonic durable offsets, restart resume | Assignment strategy is intentionally simpler than mature multi-assignor ecosystems. |
| Replication | Partition leaders, ISR/quorum checks, high-watermark recovery, follower catch-up | Long-running fault injection across every topology remains an ongoing validation area. |
| Idempotency | Producer epoch/sequence fencing and restart recovery | Producer state retention must be sized for the application retry window. |
| Transactions | Durable coordinator state, epoch fencing, markers, recovery, `read_committed`, and one bulk consumer offset scope | External side effects and multiple consumer scopes are excluded; full-snapshot coordinator sync and the standalone append-only journal favor bounded transactions. |
| Event sourcing | Optimistic aggregate versions, committed-tail reads, indexes, snapshots, and distributed routing | Snapshots optimize replay; the committed event log remains authoritative. |
| Security | TLS, token authentication, coarse permissions, topic ACLs, internal token/mTLS listener boundary | Certificate/token rotation and external identity-provider integration are deployment responsibilities. |
| Operations | Health/readiness probes, Prometheus metrics, lag and cluster state | Broader admin automation, backup orchestration, and extended soak evidence can still mature. |
| SDKs | Go SDK in-tree; Java and Python SDKs maintained separately | Cross-repository compatibility must be verified for every new protocol capability. |

## Delivery Semantics

- Normal consumer groups are at-least-once when applications process first and commit `lastProcessedOffset + 1` afterward.
- Committing before processing can skip work after a crash and therefore behaves at-most-once for that client.
- Broker transactions atomically gate output visibility with a fenced consumer offset scope for consume-process-produce workflows.
- `read_committed` hides open and aborted transactional records; `read_uncommitted` exposes the raw committed partition log.
- Database writes, HTTP calls, file writes, and other external effects require their own idempotency or transaction mechanism.

## Where Cursus Fits

Cursus is a strong fit when a team wants a partitioned durable log, consumer-group resume, event streams, and transactional broker processing without operating a large runtime stack. It is especially useful for Go services, game backends, edge deployments, and focused event-processing systems where the compact text/binary protocol is an advantage.

A more mature or specialized platform may be a better fit when the deployment requires a very large connector ecosystem, multi-datacenter replication with established operational tooling, many assignment strategies, log compaction, tiered storage, or independently audited long-duration failure evidence today.

## Evaluation Rule

Choose from measured requirements rather than feature labels. Validate the exact workload with retained data, broker restart, leader failover, stale producers, consumer rejoin, transaction retry, disk pressure, and certificate/authentication settings. The benchmark documents describe throughput tests; they are not substitutes for application-specific durability and recovery tests.
