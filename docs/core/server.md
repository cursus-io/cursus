# Server System

The server package owns client/internal listeners, bounded connection handling, framing, command dispatch, stream response mode, and startup/shutdown coordination.

## Interfaces

| Interface | Default | Contract |
|---|---:|---|
| Client TCP | 9000 | Required Wire v2 handshake followed by correlated 32-byte-header `CRS2` frames; optional TLS. |
| Health HTTP | 9080 | `/live` and `/ready`. |
| Metrics HTTP | 9100 | Prometheus scrape endpoint when enabled. |
| Internal broker TCP | configured separately | Broker-only replication/Raft/snapshot commands; token and/or mTLS authenticated. |

The client port is not an HTTP API. Health and metrics endpoints do not expose broker data commands.

## Connection Handling

The client listener uses a bounded pool of 1000 workers and a buffered connection queue. Each accepted connection can carry multiple framed requests until disconnect, deadline, stream handoff, context cancellation, or protocol error.

Each connection first completes the required Wire v2 negotiation. For every later frame the server:

1. validates the fixed 32-byte `CRS2` header, protocol version, request ID, lengths, and CRC32C,
2. reads and decompresses the exact payload with the negotiated algorithm,
3. establishes or updates the authenticated `ClientContext`,
4. decodes the command ID and deterministic `CRQ2` fields or `CBV2` batch,
5. dispatches through `CommandHandler`,
6. writes a correlated success, typed error, or consume/stream frame.

Unknown commands return structured errors. The server does not expose the removed `SUBSCRIBE` text command; embedded fan-out registration is an in-process API, while network consumers use groups plus `CONSUME`/`STREAM`.

## Response Modes

- Command successes use the documented `CRQ2`, JSON, or batch payload for that command.
- Failures use the Wire v2 typed error payload with explicit class and retryability.
- Poll consume returns correlated binary record frames for the requested batch.
- Streams use correlated batches, keepalives, and control/end frames.

A broker/network crash can close TCP without a stream terminator. SDKs treat that as retryable and resume from the broker committed group offset.

## Routing And Authority

`CommandHandler` routes:

- topic/partition data to partition leaders,
- membership/offset commands to group coordinators,
- transaction commands to transactional-id coordinators,
- durable metadata through the Raft leader/FSM,
- event-stream operations to the aggregate partition leader.

`NOT_LEADER` and `NOT_COORDINATOR` carry redirect fields. Internal forwarding never turns an internal command into a public client command; the internal context/listener boundary must authenticate the peer.

## Security

Client TLS and token authentication are independent controls. When authentication is enabled, the connection must use `AUTH` or documented inline credentials before protected commands. Coarse permissions and topic ACLs are evaluated before mutation.

Distributed production deployments should configure a dedicated internal broker port and mTLS CA/cert/key. The public listener rejects internal-only commands even when a client guesses their syntax. Protect all token traffic with TLS.

## Health And Readiness

| Endpoint | Meaning |
|---|---|
| `/live` | Process and health handler are alive. |
| `/ready` | Listener initialization and required dynamic broker/cluster dependencies are ready. |

A distributed broker can be live but not ready while it has no resolvable Raft leader or required authority. Metrics startup/bind errors are surfaced during initialization rather than silently ignored.

## Lifecycle

Startup loads normalized configuration, opens storage/coordinator/cluster state, restores recoverable indexes and prepared transactions, starts listeners/workers, and only then reports readiness.

Shutdown cancels listeners and connections, closes stream activity, drains topic/storage writes, syncs files/checkpoints, and stops coordinator/cluster resources. Abrupt kill relies on active-tail, HWM, producer, group, transaction, and stream recovery.

## Errors And Logging

Connection-local malformed frames close or fail that connection without crashing the broker. Command errors remain structured for clients. INFO logging should retain lifecycle, ownership, and final benchmark summaries; DEBUG adds per-operation detail. Fatal/panic conditions are process-level failures and benchmark gates detect anchored fatal forms.

See [Wire Protocol](../protocol-spec.md), [API Reference](../reference/api-reference.md), and [Observability](../reference/observability.md).
