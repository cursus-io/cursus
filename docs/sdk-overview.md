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

    SDK->>COORD: JOIN_GROUP / SYNC_GROUP
    COORD-->>SDK: OK assignments=[0,1,2]

    SDK->>ANY: METADATA topic=T
    ANY-->>SDK: OK leaders=L0,L1,L2

    loop Per Partition
        SDK->>LEADER: CONSUME topic=T partition=P offset=O
        LEADER-->>SDK: batch(messages)
    end

    loop Heartbeat
        SDK->>COORD: HEARTBEAT
        COORD-->>SDK: OK
    end
```
