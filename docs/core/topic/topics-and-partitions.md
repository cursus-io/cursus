# Topics and Partitions

This document provides a detailed technical explanation of topics and partitions in cursus, including their internal structure, partition selection strategies, and ordering guarantees. Topics serve as logical message streams that are horizontally scaled through partitioning, enabling both parallelism and ordering semantics.

For information about consumer groups and how they interact with partitions, see [Consumer Groups](./consumer-groups.md). For details on how messages are persisted within partitions, see [Disk Persistence System](../storage/disk-persistence.md).

## Overview

A Topic is a named message stream that is divided into one or more Partitions. Each partition operates independently with its own message queue and disk storage, allowing cursus to parallelize message processing and storage operations. The system supports two partition selection strategies: key-based routing for ordering guarantees and round-robin for load balancing.

## Topic and Partition Structure

```mermaid
flowchart TB
    subgraph "TopicManager"
        TM["topics: map[string]*Topic\ndedupMap: sync.Map\n30-min dedup window"]
    end

    subgraph "Topic: orders"
        direction TB
        TMETA["Name: orders\ncounter: uint64 (round-robin)\nmu: sync.RWMutex"]

        subgraph "Partition 0"
            P0CH["ch: chan Message\ncap 10,000"]
            P0DH["DiskHandler\nwriteCh → flushLoop"]
            P0SEG[("segment-0.log\nsegment-1.log")]
            P0SUBS["subs: map[groupName]chan\ncap 10,000 each"]
        end

        subgraph "Partition 1"
            P1CH["ch: chan Message\ncap 10,000"]
            P1DH["DiskHandler\nwriteCh → flushLoop"]
            P1SEG[("segment-0.log")]
            P1SUBS["subs: map[groupName]chan\ncap 10,000 each"]
        end

        subgraph "Partition N"
            PNCH["ch: chan Message\ncap 10,000"]
            PNDH["DiskHandler\nwriteCh → flushLoop"]
            PNSEG[("segment-0.log")]
            PNSUBS["subs: map[groupName]chan\ncap 10,000 each"]
        end

        subgraph "ConsumerGroups"
            CGA["Group A\n[Consumer0, Consumer1]\nMsgCh cap 1000 each"]
            CGB["Group B\n[Consumer0]\nMsgCh cap 1000"]
        end
    end

    TM --> TMETA
    TMETA --> P0CH & P1CH & PNCH

    P0CH -->|run goroutine| P0SUBS
    P0CH -->|EnqueueBatch| P0DH
    P0DH --> P0SEG

    P1CH -->|run goroutine| P1SUBS
    P1CH -->|EnqueueBatch| P1DH
    P1DH --> P1SEG

    PNCH -->|run goroutine| PNSUBS
    PNCH -->|EnqueueBatch| PNDH
    PNDH --> PNSEG

    P0SUBS -->|forward goroutine\npartitionID % consumerCount| CGA
    P1SUBS -->|forward goroutine| CGA
    P0SUBS -->|forward goroutine| CGB
    P1SUBS -->|forward goroutine| CGB

    subgraph "Partition Selection"
        KEY["Key != ''\n→ hash(Key) % N"]
        RR["Key == ''\n→ counter++ % N"]
    end

    TMETA -.->|msg with key| KEY
    TMETA -.->|msg without key| RR
```

## Topic Structure

The Topic struct represents a logical message stream and manages partition assignment and consumer group registration.

### Topic Data Structure

| Field          | Type                   | Purpose                                                                 |
|----------------|-----------------------|-------------------------------------------------------------------------|
| Name           | string                | Unique identifier for the topic                                         |
| Partitions     | []*Partition          | Array of partition instances                                             |
| counter        | uint64                | Round-robin counter for partition selection. Incremented on each publish without a key to implement simple round-robin distribution. |
| consumerGroups | map[string]*ConsumerGroup | Registered consumer groups for this topic                               |
| mu             | sync.RWMutex          | Protects topic-level state                                               |

## Partition Structure

Each Partition is an independent message processing unit with its own buffered channel, subscription map, and disk handler.

### Partition Data Structure

| Field  | Type                     | Capacity/Purpose                       |
|--------|--------------------------|----------------------------------------|
| id     | int                      | Zero-based partition identifier        |
| topic  | string                   | Parent topic name                       |
| ch     | chan types.Message       | 10,000 message buffer                   |
| subs   | map[string]chan types.Message | Consumer group subscription channels |
| mu     | sync.RWMutex             | Protects partition state                |
| dh     | interface{}              | DiskHandler for persistence             |
| closed | bool                     | Shutdown flag                            |

Key Design Decisions:

- 10,000 message buffer: Provides backpressure handling and decouples producers from consumers 
- Per-group channels: Each consumer group gets an isolated subscription channel with its own 10,000 message buffer 
- Dedicated goroutine: Each partition runs a goroutine that distributes messages from ch to all subs channels 

## Message Distribution - Partition Selection Strategies

The `Topic.Publish()` method implements two distinct partition selection strategies based on the presence of a message key.

### Strategy 1: Key-Based Routing

When a message contains a Key field, the partition is selected using a hash function to ensure all messages with the same key go to the same partition.

Algorithm:
- Generate 64-bit FNV-1a hash of the key: `keyID := util.GenerateID(msg.Key)`
- Modulo partition count: `idx = int(keyID % uint64(len(t.Partitions)))`

Hash Function Implementation:
- The `GenerateID()` function uses FNV-1a (Fowler-Noll-Vo) hashing, a fast non-cryptographic hash function:

FNV-1a 64-bit:
1. Initialize hash = 14695981039346656037
2. For each byte in string:
   - hash = hash XOR byte
   - hash = hash * 1099511628211
3. Return hash

### Strategy 2: Round-Robin Routing

When a message has no key `(msg.Key == "")`, partitions are selected in round-robin fashion using an atomic counter.

Algorithm:
- Read and increment counter: `idx = int(t.counter % uint64(len(t.Partitions))); t.counter++`
- Select partition at index

### Comparison Table

| Aspect         | Key-Based Routing                  | Round-Robin Routing                  |
|----------------|----------------------------------|-------------------------------------|
| Trigger        | msg.Key != ""                     | msg.Key == ""                        |
| Algorithm      | Hash(key) % partitionCount        | counter++ % partitionCount           |
| Ordering       | Guaranteed per key                | No ordering guarantee                |
| Distribution   | Depends on key distribution       | Uniform across partitions            |
| Use Case       | User sessions, entity updates     | Load distribution, batch jobs        |
| Thread Safety  | Hash is pure function             | Counter protected by t.mu lock       |

## Ordering Guarantees

cursus provides conditional ordering guarantees based on the partition selection strategy and consumer group configuration.

### Within-Partition Ordering

- **Guarantee**: Messages within a single partition are always delivered in FIFO order.
- **Implementation**: Each partition uses a Go channel (ch chan types.Message) which provides FIFO semantics. The run() goroutine reads from this channel sequentially and distributes to consumer groups in order.

### Key-Based Ordering

Guarantee: All messages with the same key are delivered to the same partition in the order they were published.

Why it works:
- Hash function is deterministic: same key always produces same hash
- Partition selection is deterministic: same hash always selects same partition
- Within partition, FIFO ordering is maintained

Example Scenario:

```
Messages:
  - {Key: "user-123", Payload: "login"}     → Hash → Partition 2
  - {Key: "user-456", Payload: "purchase"}  → Hash → Partition 0
  - {Key: "user-123", Payload: "update"}    → Hash → Partition 2
  - {Key: "user-123", Payload: "logout"}    → Hash → Partition 2

Result:
  Partition 0: ["purchase"]
  Partition 2: ["login", "update", "logout"]  ← Ordered sequence
```

## No Ordering Across Partitions

Important: There is no ordering guarantee between messages in different partitions, even with round-robin distribution.

## Dual Write Path

Each message follows two parallel paths for durability and low-latency delivery:

### Path 1: Disk Persistence (Asynchronous)

- `DiskHandler.AppendMessage()` writes to buffered channel
- Background flushLoop batches and persists to disk
- Provides durability for crash recovery

### Path 2: In-Memory Distribution (Synchronous)

- Message placed in partition's ch channel
- `run()` goroutine immediately distributes to consumer groups
- Provides low-latency delivery to active consumers

## Partition Capacity and Backpressure

### Channel Capacities

The system uses fixed-size buffered channels at multiple levels:

| Channel                     | Capacity | Purpose                           |
|-----------------------------|----------|-----------------------------------|
| Partition ch                | 10,000   | Partition-level message buffer    |
| Consumer group subscription | 10,000   | Per-group distribution buffer     |
| Consumer MsgCh              | 1,000    | Per-consumer delivery buffer      |

### Backpressure Behavior

When channels reach capacity, the system exhibits blocking behavior:

- **Partition channel full**: `Enqueue()` blocks until space available 
- **Consumer group channel full**: `run()` goroutine blocks on send 
- **Consumer channel full**: Consumer group distribution goroutine blocks 

### Dynamic Partition Addition

Topics support dynamic partition expansion through the `AddPartitions()` method. Partitions cannot be reduced.

Key Points:

- Partition IDs are sequential and immutable
- Existing partitions are unaffected
- New partitions immediately participate in round-robin selection
- Consumer groups must be re-registered to discover new partitions

## Implementation Summary

### Key Files and Components

| Component            | Key Functions                                      |
|---------------------|----------------------------------------------------|
| Topic struct         | `NewTopic()`, `Publish()`, `RegisterConsumerGroup()`    |
| Partition struct     | `NewPartition()`, `Enqueue()`, `run()`                  |
| Partition selection  | Key-based and round-robin logic                    |
| Hash functions       | `GenerateID()`, `Hash()`                               |
| Topic manager        | `CreateTopic()`, `Publish()`                            |


## Concurrency Model

### Topic-Level Lock:

- **Protects**: counter, consumerGroups map
- **Acquired during**: partition selection, consumer group registration
- **Type**: sync.RWMutex for read-heavy operations

### Partition-Level Lock:

- **Protects**: subs map, closed flag
- **Acquired during**: consumer group registration, shutdown
- **Type**: sync.RWMutex

This completes the technical documentation for topics and partitions in cursus. 

The system provides a flexible partitioning model with deterministic key-based routing for ordering guarantees and round-robin distribution for load balancing, all backed by buffered channels and independent disk persistence per partition.
