# Message Flow

Cursus has a durable partition-log data path and separate coordinator control paths. The in-process subscription channels are an optimization/API for embedded consumers; network consumers use `CONSUME` or `STREAM` against partition logs.

## Normal Publish

```mermaid
sequenceDiagram
    participant C as Producer
    participant B as Broker controller
    participant T as Topic/Partition leader
    participant D as DiskHandler
    participant R as Replicas

    C->>B: PUBLISH topic=... key=... message=...
    B->>B: authenticate, authorize, parse policy
    B->>T: route to partition leader
    T->>T: producer epoch/sequence and dedup checks
    T->>D: append assigned offset
    D-->>T: local write result
    T->>R: replicate with leader/epoch fence
    R-->>T: ISR/quorum result
    T->>T: advance committed HWM
    T-->>C: OK/ack with assigned offset
```

Keyed records use FNV-1a 64-bit hash modulo partition count when the topic policy is `hash_key`; unkeyed records use round-robin. `round_robin` policy ignores keys. Ordering is defined only within one partition.

Acknowledgement strength depends on `acks` and distribution settings. Async enqueue, buffered-writer flush, file sync, replica append, and committed HWM are distinct milestones.

## Consumer Group Read

```mermaid
sequenceDiagram
    participant C as Consumer
    participant G as Group coordinator
    participant L as Partition leader

    C->>G: JOIN_GROUP / SYNC_GROUP
    G-->>C: member, generation, assignments
    C->>G: FETCH_OFFSET per assignment
    G-->>C: committed nextOffset
    C->>L: CONSUME or STREAM isolation=read_committed
    L-->>C: records bounded by HWM/LSO
    C->>C: process records
    C->>G: BATCH_COMMIT lastProcessedOffset+1
    G-->>C: OK or fencing/error
```

The broker committed offset is authoritative. A lower request offset does not replay records before an existing commit. A missing offset follows `autoOffsetReset`; an offset removed by retention returns `OFFSET_OUT_OF_RANGE`.

`read_committed` is the default: it returns ordinary records and committed transaction output, skips aborted/control records, and stops at the earliest unresolved transaction. `read_uncommitted` exposes the raw committed partition log.

## Transactional Consume-Process-Produce

```mermaid
sequenceDiagram
    participant C as Transactional client
    participant X as Transaction coordinator
    participant P as Output partition leaders
    participant G as Group coordinator

    C->>X: INIT_PRODUCER_ID, BEGIN_TXN
    C->>X: TXN_PUBLISH (staged)
    C->>X: SEND_OFFSETS_TO_TXN (one consumer scope)
    C->>X: END_TXN result=commit
    X->>X: persist committing state
    X->>P: append idempotent records and markers
    X->>G: fenced bulk offset commit
    X->>X: persist committed decision
    X-->>C: OK state=committed
```

Output records remain invisible to `read_committed` until the partition marker and final coordinator decision agree. A restored `committing` transaction is retried. Each completed epoch must be reinitialized before the next transaction; uncertain finalization retry keeps the old epoch.

## Event Sourcing

`APPEND_STREAM` hashes the aggregate key to a partition leader, checks `version = current + 1`, appends/replicates the event, advances committed state, and updates the stream index. `READ_STREAM` and `STREAM_VERSION` are leader-routed and bounded by committed HWM. Snapshots are quorum-replicated optimizations; committed event replay remains authoritative.

## Embedded Fan-out

`TopicManager.RegisterConsumerGroup` creates in-process partition/group/consumer channels using configured capacities. It provides low-latency fan-out inside the broker process, but it is not the dynamic network group coordinator and does not replace durable offset commits, generation fencing, or log replay.

## Failure Handling

- routing failures return `NOT_LEADER` or `NOT_COORDINATOR` with redirect data,
- stale group members/generations and producer epochs are fenced,
- malformed/unauthorized commands fail before mutation,
- partial active-segment tails are truncated during restart recovery,
- HWM is clamped to durable tail,
- stream socket loss without a close-control frame is retryable through the committed offset,
- retention gaps require explicit earliest/latest/error handling.
