# Architecture Overview

## Purpose and Scope

This document provides a high-level introduction to cursus, a lightweight message broker system.

It covers the system's purpose, core components, and architectural design. For detailed information about specific subsystems, see [Architecture Overview](./contributing/README.md) and [Core Systems](./core/README.md).

For setup instructions, see [Getting Started](./user-guide/README.md).

## System Architecture

```mermaid
flowchart TB
    subgraph "Clients"
        P[Producer]
        C[Consumer]
    end

    subgraph "Network Layer"
        TCP["TCP :9000\nserver.RunServer()"]
        HTTP1["HTTP :9080\nHealth Check"]
        HTTP2["HTTP :9100\nPrometheus Metrics"]
    end

    subgraph "Broker Core"
        CMD[CommandHandler]
        TM[TopicManager]
        DM[DedupMap\n30-min window]

        subgraph "Topic"
            T[Topic]
            P0[Partition 0]
            P1[Partition 1]
            PN[Partition N]
        end

        subgraph "Persistence"
            DH0[DiskHandler 0]
            DH1[DiskHandler 1]
            DHN[DiskHandler N]
            SEG[(Segment Files)]
        end

        subgraph "Consumer Delivery"
            SM[StreamManager]
            CG[ConsumerGroup]
        end
    end

    P -->|TCP| TCP
    C -->|TCP| TCP
    TCP --> CMD
    CMD --> TM
    TM --> DM
    DM -->|unique| T
    T --> P0 & P1 & PN
    P0 --> DH0
    P1 --> DH1
    PN --> DHN
    DH0 & DH1 & DHN -->|WriteBatch| SEG
    P0 & P1 & PN --> SM
    SM --> CG
    CG --> C
    HTTP1 & HTTP2 -.->|observability| CMD
```

## What is cursus?

cursus is a lightweight message broker built around **logically separated but physically distributed data management**.

It provides publish-subscribe messaging with topic partitioning, consumer groups, and durable disk persistence, designed for single-node deployments with minimal operational complexity.

## Key characteristics:

- **Topic-based messaging**: Messages are organized into named topics with configurable partitions
- **Durable persistence**: All messages are persisted to disk using segment-based log files
- **Consumer groups**: Multiple consumer groups can independently consume the same topic
- **Deduplication**: Built-in 30-minute message deduplication window
- **Observable**: Prometheus metrics, health checks, and structured logging


## Network Interfaces

cursus exposes three network ports, each serving a distinct purpose:

| Port | Protocol | Handler | Purpose |
|------|----------|---------|---------|
| 9000 | TCP      | `server.RunServer()` | Main broker operations (`PUBLISH`, `CONSUME`, `CREATE`, etc.) |
| 9080 | HTTP     | `startHealthCheckServer()` | `/live`, `/ready`, and compatible `/health` probes |
| 9100 | HTTP     | `metrics.StartMetricsServer()` | Prometheus exporter with scrape-time broker state |


## Core Data Flow

### End-to-End Data Flow Sequence

```mermaid
sequenceDiagram
    participant PROD as Producer
    participant SRV as Server :9000
    participant TM as TopicManager
    participant PART as Partition
    participant DH as DiskHandler
    participant DISK as Segment Files
    participant SM as StreamManager
    participant CONS as Consumer

    PROD->>SRV: [4-byte len][PUBLISH topic msg]
    SRV->>TM: Publish(topic, message)
    TM->>TM: dedup check (FNV hash)
    alt duplicate within 30min
        TM-->>PROD: ERROR: duplicate
    else unique message
        TM->>PART: Enqueue(message)\nkey-hash or round-robin
        PART->>DH: AppendMessage(writeCh)
        DH-->>DISK: flushLoop batch write\n(50 msgs or 50ms)
        PART->>SM: NotifyNewMessage
        SM->>CONS: push to MsgCh
        CONS-->>PROD: (async delivery)
    end

    Note over CONS,DISK: Disk-based replay (CONSUME)
    CONS->>SRV: [4-byte len][CONSUME topic partition offset]
    SRV->>PART: ReadCommitted(offset) by default
    PART->>DISK: mmap read (up to 8192 bytes)
    DISK-->>SRV: message batch
    SRV-->>CONS: [4-byte len][msg1][4-byte len][msg2]...
```

### Mermaid Graph Overview

```mermaid
graph LR
    P[Publisher] -->|TCP| S[Server :9000]
    S --> TM[TopicManager]
    TM -->|dedup check| T[Topic]
    T -->|hash/round-robin| Part[Partition]
    Part -->|async| DH[DiskHandler]
    DH -->|writeCh| FL[flushLoop]
    FL -->|WriteBatch| Seg[Segment Files]
    Part -->|notify| SM[StreamManager]
    SM -->|push| C[Consumer]
    C -->|CONSUME/STREAM| Part
    Part -->|ReadCommitted / ReadMessages| Seg
```

### Key flow characteristics:

- **Deduplication**: `TopicManager.Publish()` checks dedupMap using message ID (hash of payload) to prevent duplicate processing within 30 minutes 
- **Partition Selection**: `Topic.Publish()` uses key-based hashing for ordered delivery or round-robin counter for load balancing
- **Dual-path delivery**: `Partition.Enqueue()` sends to both disk (via DiskHandler) and consumer channels
- **Asynchronous writes**: DiskHandler batches up to 50 messages or flushes after 50ms linger timeout (configurable)
- **Consumer isolation**: Each ConsumerGroup receives messages independently through dedicated channels

## Message Persistence

Messages are persisted using a segment-based append-only log architecture

Each topic-partition pair gets its own DiskHandler instance:

- Writes asynchronously via `flushLoop()` goroutine
- Batches up to 50 messages or flushes after 50ms linger (configurable)
- Rotates segments at 1GB boundaries (configurable via `log_segment_bytes`)
- Uses `mmap`(memory-mapped I/O) for reads
- Stores messages with 4-byte big-endian length prefixes

This architecture enables parallel I/O across partitions and efficient sequential reads. For detailed persistence mechanics, see [Disk Persistence System](./core/storage/disk-persistence.md).

## Cluster Architecture

cursus supports a 3-node Raft-based cluster with coordinator and partition-leader routing.

### Cluster Topology

```mermaid
flowchart TB
    subgraph "Raft Cluster"
        direction TB
        B1["Broker-1\n:9001\nRaft Leader"]
        B2["Broker-2\n:9002"]
        B3["Broker-3\n:9003"]

        B1 <-->|"Raft replication\nlog entries"| B2
        B2 <-->|"Raft replication\nlog entries"| B3
        B1 <-->|"Raft replication\nlog entries"| B3
    end

    subgraph "Clients"
        PROD[Producer SDK]
        CONS[Consumer SDK]
    end

    subgraph "Coordination"
        COORD["Coordinator\n(consistent hash\nper group)"]
    end

    PROD -->|PUBLISH / METADATA| B1
    CONS -->|FIND_COORDINATOR| B1
    B1 -->|coordinator=B2| CONS
    CONS -->|JOIN_GROUP / HEARTBEAT| COORD
    COORD --- B2
    CONS -->|CONSUME P0| B1
    CONS -->|CONSUME P1| B3
    CONS -->|CONSUME P2| B2
```

### Routing Model

```mermaid
graph TB
    subgraph Client
        SDK[SDK Consumer/Producer]
    end

    subgraph Cluster
        B1[Broker-1<br/>Raft Leader]
        B2[Broker-2]
        B3[Broker-3]
    end

    SDK -->|1. FIND_COORDINATOR group=G| B1
    B1 -->|coordinator=B2| SDK
    SDK -->|2. JOIN_GROUP, HEARTBEAT, COMMIT| B2
    SDK -->|3. METADATA topic=T| B1
    B1 -->|P0=B1, P1=B3, P2=B2| SDK
    SDK -->|4. CONSUME P0| B1
    SDK -->|4. CONSUME P1| B3
    SDK -->|4. CONSUME P2| B2
```

### Three Connection Types

| Connection | Target | Discovery | Commands |
|---|---|---|---|
| Any broker | Any node | Config | `FIND_COORDINATOR`, `METADATA`, `CREATE`, `LIST` |
| Coordinator | Per-group (consistent hash) | `FIND_COORDINATOR` | `JOIN_GROUP`, `SYNC_GROUP`, `LEAVE_GROUP`, `HEARTBEAT`, `COMMIT_OFFSET`, `FETCH_OFFSET` |
| Partition leader | Per-partition | `METADATA` | `CONSUME`, `STREAM`, `PUBLISH` |

### Coordinator Pattern

```mermaid
sequenceDiagram
    participant C as Consumer SDK
    participant B1 as Broker-1
    participant B2 as Broker-2 (Coordinator)
    participant B3 as Broker-3

    C->>B1: FIND_COORDINATOR group=G1
    B1-->>C: OK host=B2 port=9002

    C->>B2: JOIN_GROUP topic=T group=G1 member=M
    B2-->>C: OK generation=1 member=M-1234 assignments=[0,1]
    C->>B2: SYNC_GROUP topic=T group=G1 member=M-1234 generation=1
    B2-->>C: OK generation=1 member=M-1234 assignments=[0,1]

    C->>B2: HEARTBEAT topic=T group=G1 member=M-1234 generation=1
    B2-->>C: OK member=M-1234 generation=1

    Note over C,B3: If coordinator changes...
    C->>B2: HEARTBEAT topic=T group=G1 member=M-1234 generation=1
    B2-->>C: ERROR: NOT_COORDINATOR host=B3 port=9003
    C->>B3: HEARTBEAT topic=T group=G1 member=M-1234 generation=1
    B3-->>C: OK member=M-1234 generation=1
```

### Partition Leader Routing

```mermaid
sequenceDiagram
    participant C as Consumer SDK
    participant B1 as Broker-1
    participant B2 as Broker-2 (P0 Leader)
    participant B3 as Broker-3 (P1 Leader)

    C->>B1: METADATA topic=T
    B1-->>C: OK leaders=B2:9002,B3:9003

    C->>B2: CONSUME topic=T partition=0
    B2-->>C: [messages]

    C->>B3: CONSUME topic=T partition=1
    B3-->>C: [messages]

    Note over C,B3: If partition leader changes...
    C->>B2: CONSUME topic=T partition=0
    B2-->>C: ERROR: NOT_LEADER LEADER_IS B3:9003
    C->>B3: CONSUME topic=T partition=0
    B3-->>C: [messages]
```

### Raft Consensus

Group state changes (JOIN, LEAVE, COMMIT) are persisted via Raft. The coordinator may not be the Raft leader — in that case, `applyViaLeader` forwards the Raft log entry to the leader internally via `RAFT_APPLY`.

```mermaid
graph LR
    Coord[Coordinator Broker] -->|applyViaLeader| Leader[Raft Leader]
    Leader -->|raft.Apply| FSM1[FSM Node 1]
    Leader -->|replicate| FSM2[FSM Node 2]
    Leader -->|replicate| FSM3[FSM Node 3]
```

### Advertised Addresses

Each broker registers its client-facing address (`ClientAddr`) in the FSM on startup. This allows any broker to resolve any other broker's external address for `METADATA`, `FIND_COORDINATOR`, and `NOT_LEADER` responses.

```yaml
# Docker Compose example
broker-1:
  environment:
    - ADVERTISED_CLIENT_HOST=localhost
    - ADVERTISED_BROKER_PORT=9001
```
