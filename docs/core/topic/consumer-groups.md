# Consumer Groups

This document explains how consumer groups work in cursus, including their structure, registration process, load balancing mechanism, and message distribution strategy. Consumer groups enable multiple consumers to share the load of processing messages from a topic while maintaining ordering guarantees within partitions.

For information about topic and partition structure, see [Topics and Partitions](./topics-and-partitions.md). For the broader topic management system, see [Topic Management System](./topic-management.md).

## Purpose and Functionality

Consumer groups provide a mechanism for horizontal scaling of message consumption. Multiple consumers can join a group to collectively process messages from a topic, with cursus automatically distributing partitions among the consumers. 

Each partition's messages are delivered to exactly one consumer within a group, ensuring that ordering is preserved within each partition while enabling parallel processing across partitions.

```mermaid
graph TD
    T[Topic: orders] --> P0[Partition 0]
    T --> P1[Partition 1]
    T --> P2[Partition 2]
    T --> P3[Partition 3]

    subgraph "Consumer Group A"
        C0[Consumer 0]
        C1[Consumer 1]
    end

    P0 -->|assigned| C0
    P1 -->|assigned| C1
    P2 -->|assigned| C0
    P3 -->|assigned| C1

    subgraph "Consumer Group B (independent)"
        C2[Consumer 0]
    end

    P0 -.->|independent copy| C2
    P1 -.-> C2
    P2 -.-> C2
    P3 -.-> C2
```

## Key capabilities:

- **Load balancing**: Partitions are distributed evenly across consumers using modulo arithmetic
- **Ordering guarantees**: All messages from a single partition go to the same consumer
- **Independent consumption**: Multiple consumer groups can consume the same topic independently
- **Static assignment**: Partition-to-consumer mapping is established at registration time

## Data Structures

### ConsumerGroup Structure

The ConsumerGroup struct contains an array of Consumer instances. Each Consumer has a buffered channel (MsgCh) with capacity 1000 that receives messages from assigned partitions.

### Key Parameters:

| Component                | Buffer Size | Purpose                              |
|--------------------------|------------|--------------------------------------|
| Consumer.MsgCh           | 1000       | Consumer's message receive buffer     |
| Partition group channel  | 10000      | Per-group buffer in each partition    |
| Partition main channel   | 10000      | Partition's internal message buffer   |


## Registration Process

### RegisterConsumerGroup Method

The RegisterConsumerGroup method establishes a consumer group for a topic. It performs the following operations:

- **Check for existing group**: Returns existing group if already registered
- **Create consumer instances**: Allocates consumerCount consumers with buffered channels
- **Register with each partition**: Each partition creates a dedicated channel for the group
- **Establish partition-to-consumer mapping**: Uses modulo arithmetic for distribution
- **Start forwarding goroutines**: Bridge partition group channels to consumer channels

## Load Balancing Algorithm

### Partition-to-Consumer Distribution

cursus uses a deterministic modulo-based distribution algorithm:

```
target_consumer_index = partition_id % consumer_count
```

This ensures:

- **Even distribution**: Partitions are spread uniformly across consumers
- **Deterministic mapping**: Same partition always goes to same consumer
- **Ordering preservation**: All messages from a partition are processed in order

### Distribution Example:

```
Partition ID	Consumer Count	Target Consumer	Calculation
0	3	0	0 % 3 = 0
0 % 3 = 0
1	3	1	1 % 3 = 1
1 % 3 = 1
2	3	2	2 % 3 = 2
2 % 3 = 2
3	3	0	3 % 3 = 0
3 % 3 = 0
4	3	1	4 % 3 = 1
4 % 3 = 1
5	3	2	5 % 3 = 2
5 % 3 = 2
```

## Message Distribution

### From Partition to Consumer

Messages flow through multiple channels before reaching a consumer:


Message Path:

- **Partition channel** (Partition.ch): Receives published messages 
- **Partition run goroutine**: Fans out messages to all registered group channels 
- **Group channel** (subs[groupName]): Per-group buffer in each partition 
- **Forwarding goroutine**: Bridges group channel to specific consumer 
- **Consumer channel** (Consumer.MsgCh): Consumer's receive buffer 


## Partition Run Loop

The partition's `run()` method distributes messages to all registered consumer groups:

```
func (p *Partition) run() {
    for msg := range p.ch {
        p.mu.RLock()
        for _, subCh := range p.subs {  // Each group gets a copy
            subCh <- msg
        }
        p.mu.RUnlock()
    }
}
```

This design enables multiple independent consumer groups to consume the same topic without interference.

## Multi-Group Consumption

### Independent Consumer Groups

Multiple consumer groups can consume the same topic simultaneously. Each group maintains:

- Independent offset tracking (via direct channel consumption or disk reads)
- Separate partition-to-consumer mappings
- Isolated message delivery

## Concurrency and Thread Safety

### Locking Strategy

Consumer group registration and access use read-write mutexes:

| Operation                     | Lock Type   | Scope       |
|-------------------------------|------------|-------------|
| RegisterConsumerGroup          | Write lock | Topic.mu    |
| Consume channel lookup         | Read lock  | Topic.mu    |
| RegisterGroup                  | Write lock | Partition.mu|
| Partition message broadcast    | Read lock  | Partition.mu|


The locking hierarchy ensures:

- Consumer group map is protected during registration
- Partition subscription map is protected during group registration
- Message broadcasting holds minimal locks (read locks only)

## Key Characteristics

### Behavior Summary

| Aspect                  | Behavior                                           |
|-------------------------|---------------------------------------------------|
| Partition assignment     | Static, established at registration             |
| Distribution algorithm   | Modulo: partition_id % consumer_count           |
| Ordering guarantee       | Per-partition ordering maintained               |
| Group isolation          | Groups consume independently                     |
| Rebalancing              | Not supported (static assignment)               |
| Consumer failure         | Channel blocks, may lead to backpressure        |
| Buffer overflow          | Goroutine blocks if consumer channel full       |

### Limitations

- **Static assignment**: Consumers cannot be added/removed without re-registration
- **No automatic rebalancing**: Partition distribution doesn't adjust to consumer changes
- **No offset management**: Applications must track their own offsets for disk reads
- **Channel-based only**: Active consumption through channels, no pull-based API in this layer

## Consumer Group Lifecycle

```mermaid
sequenceDiagram
    participant APP as Application
    participant TM as TopicManager
    participant TOP as Topic
    participant PART as Partition
    participant CG as ConsumerGroup
    participant CONS as Consumer

    APP->>TM: RegisterConsumerGroup(topic, group, count)
    TM->>TOP: RegisterConsumerGroup(group, count)

    alt group already exists
        TOP-->>APP: return existing ConsumerGroup
    else new group
        TOP->>CG: create ConsumerGroup
        loop for each consumer (0..count-1)
            CG->>CONS: create Consumer with MsgCh (cap 1000)
        end

        loop for each partition
            TOP->>PART: RegisterGroup(groupName)
            PART->>PART: create subs[groupName] chan (cap 10000)
            PART->>PART: start forwarding goroutine\n(subs[group] → Consumer.MsgCh\nusing partitionID % consumerCount)
        end

        TOP-->>APP: return ConsumerGroup
    end

    Note over APP,CONS: Message delivery after registration
    APP->>CONS: receive from Consumer.MsgCh
```

## Rebalance Flow

```mermaid
flowchart TD
    START([New consumer wants to join]) --> CHECK{Group\nalready\nregistered?}
    CHECK -->|Yes| RETURN[Return existing ConsumerGroup\nno rebalance supported]
    CHECK -->|No| CREATE[Create ConsumerGroup\nwith N consumers]
    CREATE --> ASSIGN[Assign partitions via\npartitionID % consumerCount]
    ASSIGN --> GOROUTINE[Start forwarding goroutines\nper partition per group]
    GOROUTINE --> READY([Group ready to consume])

    RETURN --> NOTE1[Static assignment\nConsumers cannot be\nadded or removed\nwithout re-registration]

    READY --> RECV[Consumer receives\nfrom MsgCh]
    RECV --> PROC[Process message]
    PROC --> RECV

    PROC -->|consumer failure| BLOCK[Channel blocks\nBackpressure propagates\nto partition]
    BLOCK --> RECOVER{Application\nrecovers?}
    RECOVER -->|Yes| RECV
    RECOVER -->|No| OVERFLOW[Buffer overflow\ngoroutine blocks]
```

# Summary

Consumer groups in cursus provide load balancing and ordering guarantees through a deterministic partition assignment mechanism. The modulo-based distribution ensures even load across consumers while maintaining per-partition message ordering.

Multiple consumer groups can independently consume the same topic, each with its own partition-to-consumer mapping and isolated message delivery channels.

The implementation uses goroutines to bridge partition group channels to consumer message channels, with buffering at multiple levels (partition: 10000, group channel: 10000, consumer: 1000) to handle bursts and provide backpressure protection.
