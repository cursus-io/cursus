# SDK Overview

Cursus broker에 연결하는 3개 SDK의 아키텍처 개요.

## SDK Ecosystem

```mermaid
flowchart LR
    subgraph "Client SDKs"
        GO[Go SDK<br/>sdk/]
        JAVA[Java SDK<br/>cursus-java]
        PY[Python SDK<br/>cursus-python]
    end

    subgraph "Cursus Broker"
        B1[Broker 1]
        B2[Broker 2]
        B3[Broker 3]
    end

    GO --> B1
    JAVA --> B2
    PY --> B3
    B1 <--> B2
    B2 <--> B3
    B1 <--> B3
```

## Wire Protocol Compatibility

All SDKs implement the same wire protocol:

```mermaid
flowchart TB
    subgraph "Wire Protocol"
        FRAME["4-byte length prefix + payload"]
        BATCH["Batch: magic 0xBA7C + header + messages"]
        CMD["Commands: key=value text format"]
    end

    subgraph "Shared"
        ENCODE[EncodeMessage / encode_message]
        DECODE[DecodeBatchMessages / decode_batch]
        COMPRESS[gzip / snappy / lz4]
    end

    GO[Go SDK] --> FRAME
    JAVA[Java SDK] --> FRAME
    PY[Python SDK] --> FRAME
    FRAME --> BATCH
    FRAME --> CMD
    BATCH --> ENCODE
    BATCH --> DECODE
    FRAME --> COMPRESS
```

## Feature Matrix

| Feature | Go SDK | Java SDK | Python SDK |
|---|---|---|---|
| Producer (sync) | ✅ | ✅ | ✅ |
| Producer (async) | — | — | ✅ AsyncProducer |
| Consumer (polling) | ✅ | ✅ | ✅ |
| Consumer (streaming) | ✅ | ✅ | ✅ |
| Consumer Groups | ✅ | ✅ | ✅ |
| EventStore | ✅ | — | ✅ |
| Compression (gzip) | ✅ | ✅ | ✅ |
| Compression (snappy) | ✅ | — | ✅ (extras) |
| Compression (lz4) | ✅ | — | ✅ (extras) |
| TLS | ✅ | ✅ | ✅ |
| FindCoordinator | ✅ | ✅ | ✅ |
| Partition Leader Routing | ✅ | ✅ | ✅ |
| Protocol capability negotiation | ✅ | pending | pending |
| Typed structured broker errors | ✅ | pending | pending |
| Broker-managed transactions | ✅ | pending | pending |
| Connection authentication | ✅ | pending | pending |
| Framework Integration | — | Spring Boot | FastAPI |
| Iterator Pattern | — | — | ✅ for/async for |

## Cluster Consumer Routing

```mermaid
sequenceDiagram
    participant SDK as Any SDK
    participant ANY as Any Broker
    participant COORD as Coordinator
    participant LEADER as Partition Leader

    SDK->>ANY: FIND_COORDINATOR group=G
    ANY-->>SDK: OK host=H port=P

    SDK->>COORD: JOIN_GROUP topic=T group=G member=M
    COORD-->>SDK: OK member=M-1234 generation=N assignments=[0,1,2]
    SDK->>COORD: SYNC_GROUP topic=T group=G member=M-1234 generation=N
    COORD-->>SDK: OK member=M-1234 generation=N assignments=[0,1,2]

    SDK->>ANY: METADATA topic=T
    ANY-->>SDK: OK leaders=L0,L1,L2

    loop Per Partition
        SDK->>LEADER: CONSUME topic=T partition=P offset=O
        LEADER-->>SDK: batch(messages)
    end

    loop Heartbeat
        SDK->>COORD: HEARTBEAT topic=T group=G member=M-1234 generation=N
        COORD-->>SDK: OK member=M-1234 generation=N
    end
```
## Protocol Capability Negotiation

The Go SDK can query broker capabilities with `sdk.FetchProtocolInfo(conn)` and negotiate a connection with `sdk.NegotiateProtocol(conn, request)`. High-level producer and consumer clients perform the same handshake automatically when protocol settings are configured:

```go
cfg := sdk.NewDefaultConsumerConfig()
cfg.ProtocolVersion = 1
cfg.ProtocolFeatures = []string{"structured_errors_v1", "offset_resume_v1"}
cfg.RequireProtocolFeatures = true
```

The default configuration leaves automatic negotiation disabled for compatibility with older brokers. Set `ProtocolVersion` or at least one `ProtocolFeatures` entry to enable it. Negotiation then runs once for every newly opened or reconnected TCP connection. A failed required negotiation closes the connection before it can be used, and `RequireProtocolFeatures=true` requires at least one configured feature. `ProtocolNegotiationTimeoutMS` bounds the handshake; values less than or equal to zero use 5000 ms.

Broker failures returned by negotiation are available as `*sdk.BrokerError`:

```go
var brokerErr *sdk.BrokerError
if errors.As(err, &brokerErr) {
    if brokerErr.Retryable {
        // Apply bounded backoff or redirect handling before retrying.
    }
}
```

`BrokerError` exposes `Code`, `Class`, `Retryable`, `Fields`, and the raw response. It also remains compatible with existing Go SDK sentinels such as `ErrTopicNotFound`, `ErrInvalidPartition`, and `ErrNotLeader` through `errors.Is`.

## Go Transactional Producer

The Go SDK exposes both the existing low-level transaction commands and a
session-preserving `TransactionalProducer`:

```go
cfg := sdk.NewDefaultConsumerConfig()
cfg.BrokerAddrs = []string{"broker-1:9000", "broker-2:9000"}

client, err := sdk.NewConsumerClient(cfg)
if err != nil {
    return err
}
producer, err := client.NewTransactionalProducer("game-server-events")
if err != nil {
    return err
}
if err := producer.Begin(); err != nil {
    return err
}
if err := producer.Publish("events", -1, sdk.Message{
    SeqNum:  1,
    Key:     "match-42",
    Payload: payload,
}); err != nil {
    _ = producer.Abort()
    return err
}
if err := producer.SendOffsets(
    "events",
    groupID,
    memberID,
    generation,
    map[int]uint64{partition: lastProcessedOffset + 1},
); err != nil {
    _ = producer.Abort()
    return err
}
return producer.Commit()
```

The broker allocates and fences the producer ID and epoch. The high-level
producer retains that session across reconnects and serializes lifecycle calls.
`NOT_COORDINATOR` responses update a per-transaction coordinator cache and are
retried with a bounded delay. Fencing, authorization, validation, and ambiguous
network failures are returned to the caller as errors rather than retried.

`Commit` and `Abort` may be called again with the same session after an
uncertain response. The broker is authoritative for final transaction state;
`Describe` returns the typed state, message count, and offset count.

## Go Consumer Resume Contract

The Go consumer fetches the broker-owned committed `nextOffset` after
assignment and rejoin. Applications that commit after processing should commit
`lastProcessedOffset + 1`. Reconnects roll back the local fetch position to the
last broker-committed offset, providing at-least-once processing.

`AutoOffsetResetEarliest` and `AutoOffsetResetLatest` are sent as
`autoOffsetReset=earliest|latest` in polling and streaming commands.
`AutoOffsetResetError` leaves reset selection disabled and surfaces an
out-of-range condition.

The current broker data path is committed-only: transactional records remain
hidden until a commit marker is durable, open transactions form the visibility
boundary, and aborted records are skipped. The Go SDK does not expose a
`read_uncommitted` option because that is not part of the current broker wire
contract.

## Go Client Authentication

Publisher and consumer configs accept connection credentials:

```go
cfg.Principal = "game-server"
cfg.AuthToken = os.Getenv("CURSUS_AUTH_TOKEN")
```

When both fields are set, every new or reconnected SDK connection performs
`AUTH principal=<principal> token=<token>` after protocol negotiation and
before application commands. The two fields must be configured together.
Authentication and authorization failures are returned as typed
`*sdk.BrokerError` values. TLS remains required when credentials cross an
untrusted network.