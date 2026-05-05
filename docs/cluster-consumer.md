# Cluster Consumer Architecture

Consumer가 클러스터 환경에서 메시지를 소비하기 위한 FindCoordinator 기반 라우팅 아키텍처.

## Overview

Kafka와 동일한 패턴으로, Consumer Group 관련 커맨드는 Coordinator 브로커로, 데이터 커맨드는 파티션 리더로 라우팅합니다.

## Command Routing

```mermaid
flowchart TB
    Consumer[Consumer SDK]

    subgraph "1. Coordinator Discovery"
        ANY[Any Broker]
        FC[FIND_COORDINATOR]
        Consumer -->|"①"| ANY
        ANY -->|"group=my-group"| FC
        FC -->|"OK host=H port=P"| Consumer
    end

    subgraph "2. Group Commands → Coordinator"
        COORD[Coordinator Broker]
        Consumer -->|"②"| COORD
        COORD --- JOIN[JOIN_GROUP]
        COORD --- SYNC[SYNC_GROUP]
        COORD --- HB[HEARTBEAT]
        COORD --- LEAVE[LEAVE_GROUP]
        COORD --- FETCH[FETCH_OFFSET]
        COORD --- COMMIT[COMMIT_OFFSET]
        COORD --- BATCH[BATCH_COMMIT]
    end

    subgraph "3. Data Commands → Partition Leader"
        META[METADATA]
        Consumer -->|"③"| ANY
        ANY -->|"leaders=H1:P1,H2:P2"| META
    end

    subgraph "4. Consume from Leaders"
        L1[Partition 0 Leader]
        L2[Partition 1 Leader]
        Consumer -->|"CONSUME P0"| L1
        Consumer -->|"CONSUME P1"| L2
    end
```

## Command Routing Table

| Command | Target | Error Handling |
|---|---|---|
| `FIND_COORDINATOR` | Any broker | Retry with next broker |
| `JOIN_GROUP` | Coordinator | NOT_COORDINATOR → re-discover |
| `SYNC_GROUP` | Coordinator | NOT_COORDINATOR → re-discover |
| `LEAVE_GROUP` | Coordinator | NOT_COORDINATOR → re-discover |
| `HEARTBEAT` | Coordinator | NOT_COORDINATOR → re-discover |
| `FETCH_OFFSET` | Coordinator | NOT_COORDINATOR → re-discover |
| `COMMIT_OFFSET` | Coordinator | NOT_COORDINATOR → re-discover |
| `BATCH_COMMIT` | Coordinator | NOT_COORDINATOR → re-discover |
| `CONSUME` | Partition Leader | NOT_LEADER → update leader cache |
| `STREAM` | Partition Leader | NOT_LEADER → update leader cache |

## Consumer Lifecycle (Cluster)

```mermaid
sequenceDiagram
    participant C as Consumer
    participant B as Any Broker
    participant CO as Coordinator
    participant PL as Partition Leader

    C->>B: FIND_COORDINATOR group=G
    B-->>C: OK host=H port=P

    C->>CO: JOIN_GROUP topic=T group=G member=M
    CO-->>C: OK generation=1 member=M-1234 assignments=[0,1]

    C->>CO: FETCH_OFFSET topic=T partition=0 group=G
    CO-->>C: 0

    C->>B: METADATA topic=T
    B-->>C: OK leaders=H1:P1,H2:P2

    loop Poll Loop
        C->>PL: CONSUME topic=T partition=0 offset=0 member=M-1234 group=G generation=1
        PL-->>C: batch(messages)
    end

    par Heartbeat (every 3s)
        C->>CO: HEARTBEAT topic=T group=G member=M-1234 generation=1
        CO-->>C: OK
    end
```

## Error Recovery

```mermaid
flowchart TD
    SEND[Send Command]
    SEND --> CHECK{Response?}

    CHECK -->|NOT_COORDINATOR| REDISC[Re-discover Coordinator]
    REDISC --> PARSE{host/port in response?}
    PARSE -->|Yes| UPDATE[Update coordinator addr]
    PARSE -->|No| FIND[FIND_COORDINATOR]
    UPDATE --> RETRY[Retry command]
    FIND --> RETRY

    CHECK -->|NOT_LEADER LEADER_IS addr| ULEAD[Update partition leader cache]
    ULEAD --> RETRY

    CHECK -->|OK / data| DONE[Process response]
```

## Implementation Details

### Go SDK

- `findCoordinator()` — sends `FIND_COORDINATOR` via `ConnectWithFailover`
- `getCoordinatorConn()` — connects to coordinator, falls back to `findCoordinator` on failure
- `fetchMetadata()` — sends `METADATA topic=<topic>`, populates `partitionLeaders` map
- `getPartitionLeaderAddr(partitionID)` / `updatePartitionLeader(partitionID, addr)` — thread-safe leader cache
- `ensureConnection()` — prefers partition leader address, falls back to any broker
- `handleBrokerError()` — parses NOT_LEADER, updates partition leader, triggers rebalance on GEN_MISMATCH
- `handleNotCoordinator()` — re-discovers coordinator from response or via FIND_COORDINATOR

### Known Issue: Cluster Consumer Blocking

Go SDK의 Consumer가 클러스터에서 `FETCH_OFFSET` 응답을 기다리면서 블로킹되는 문제가 있습니다.
Python/Java SDK는 매 커맨드마다 새 TCP 연결을 사용하여 이 문제를 회피합니다.
Go SDK는 persistent 연결을 사용하기 때문에, 브로커가 같은 coordinator 주소의 다른 TCP 연결에서 온
FETCH_OFFSET을 처리하지 못하는 것으로 추정됩니다.
