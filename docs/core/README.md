# Core Systems

Cursus separates client handling, partition data, coordinator metadata, storage, and cluster ownership while preserving one wire contract in standalone and distributed modes.

## Component Map

| Area | Primary packages | Authority |
|---|---|---|
| Server and protocol | `pkg/server`, `pkg/controller`, `pkg/protocol` | Framing, authentication, command dispatch, structured responses, redirects. |
| Topics and partitions | `pkg/topic` | Partition selection, producer fencing, HWM/LSO, transaction visibility, in-process fan-out. |
| Consumer groups | `pkg/coordinator` | Membership, generation, assignment, ownership fencing, durable monotonic offsets. |
| Transactions | `pkg/transaction`, controller transaction handlers | Transactional-id sessions, producer epochs, staging, prepared/final decisions, recovery. |
| Storage | `pkg/disk` | Segment/index files, buffering, sync, mmap reads, retention, active-tail repair. |
| Cluster | `pkg/cluster` | Raft metadata, broker registry, coordinator/leader routing, replication and internal transport. |
| Event sourcing | `pkg/eventsource`, controller/topic integration | Aggregate version checks, stream indexes, snapshots, committed replay. |
| SDK | `sdk` | Go producer/consumer, redirects, offsets, read isolation, transactions, auth, event store. |

## Data And Control Paths

Normal records are owned by a partition leader. Consumer membership/offsets are owned by the selected group coordinator. Transaction lifecycle state is owned by the selected transaction coordinator. Distributed metadata mutations are replicated through the Raft FSM; standalone transaction state is appended to the broker transaction journal before acknowledgement.

A transaction does not collapse those owners into one in-memory object. The transaction coordinator stages intent, partition leaders append idempotent output and markers, the group coordinator applies one fenced bulk source-offset scope, and the final transaction decision gates `read_committed` visibility.

## Configuration Defaults

| Area | Default |
|---|---:|
| Client TCP / health / metrics ports | `9000` / `9080` / `9100` |
| Disk flush batch / linger | `50` records / `50ms` |
| Disk sync interval | `500ms` |
| Disk write channel | buffered, capacity `1024` |
| Segment / sparse index size | `1GiB` / `10MiB` |
| Sparse index interval | `4096` bytes |
| Retention | `168h`, unlimited bytes, delete policy |
| Partition / consumer / broadcast buffers | `10000` / `1000` / `10000` |
| Distribution | disabled unless configured |

Configuration validation normalizes invalid values. Cleanup policy accepts `delete`, `compact`, or `delete,compact`; compaction is standalone-only and is rejected for event-sourcing topics.

## Concurrency

- `TopicManager` and `Topic` use read/write locks for registries and partition metadata.
- Each partition has its own channels, storage handler, producer state, and transaction visibility index.
- `DiskHandler.writeCh` is buffered; `flushLoop`, `syncLoop`, and the retention loop are independent goroutines.
- Disk metadata, file I/O, and index lifecycle use separate locks with a documented acquisition order.
- Group and transaction managers serialize authoritative state. Transactions recover from the standalone journal or distributed replicated revisions/snapshots.
- Cluster forwarding must cross the broker-internal authentication/mTLS boundary; internal commands are not client APIs.

## Startup

1. load and normalize configuration,
2. initialize storage and topic managers,
3. restore coordinator/Raft snapshots and durable offsets,
4. recover partition tails, HWM, producer, stream, and transaction indexes,
5. retry durable `committing` transactions,
6. start client, internal, health, and metrics listeners,
7. report readiness only after required cluster/storage authority is available.

## Shutdown

The broker stops accepting work, closes client/stream activity, drains partition/storage channels, flushes and syncs active files, persists checkpoints, stops coordinator loops, and closes cluster resources. Shutdown is best-effort under process kill; restart recovery is the correctness path for abrupt failure.

## Guarantees

- per-partition ordering, not global ordering,
- durable group `nextOffset` and generation/owner fencing,
- at-least-once consumer processing when commit follows processing,
- idempotent producer retries within the producer epoch/sequence contract,
- transaction output visibility gated by partition markers and the durable current-epoch coordinator decision,
- event-stream optimistic concurrency per aggregate partition,
- retained committed reads bounded by recovered HWM/LSO.

External side effects are never part of a broker transaction. Time/size retention can remove replay history, so applications must monitor offset gaps and choose reset/rebuild policy explicitly.

## Detailed Documents

- [Message Flow](message-flow.md)
- [Server](server.md)
- [Consumer Groups](topic/consumer-groups.md)
- [Disk Persistence](storage/disk-persistence.md)
- [Wire Protocol](../protocol-spec.md)
