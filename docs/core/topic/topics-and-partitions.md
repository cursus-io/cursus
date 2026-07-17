# Topics And Partitions

A Cursus topic is a named partitioned log. Each partition has independent offsets, storage segments, producer state, committed visibility, replication ownership, and consumer-group assignment.

## Topic Policy

Topic metadata includes:

- partition count and replication factor,
- idempotent and event-sourcing flags,
- `partitioner=hash_key|round_robin`,
- retention hours/bytes,
- read/write authorization policy and ACLs.

The policy is returned by `DESCRIBE`/`METADATA` and enforced by command handlers and partition leaders.

## Partition Selection

### Hash key

For `hash_key`, a non-empty key uses FNV-1a 64-bit hash:

```text
partition = hash(key) % partitionCount
```

The same key maps to the same partition while the partition count is unchanged. Unkeyed records fall back to round-robin.

### Round robin

For `round_robin`, an atomic topic counter distributes every record across partitions and ignores keys.

Adding partitions changes the modulo and can remap future keyed records. Applications requiring stable key placement must plan partition expansion explicitly.

## Ordering

Cursus preserves logical offset order inside one partition. It does not define a total order across partitions. Records for one key inherit partition order only when the same partition policy/count is used.

Consumer groups assign each partition to at most one active member in one generation. A stale member cannot commit that partition after ownership moves.

## Partition State

A partition owns these boundaries:

| Boundary | Meaning |
|---|---|
| LEO | Next locally assignable offset. |
| Flushed tail | Next offset written through the local buffered file writer. |
| HWM | Next offset committed by the active durability/replication path. |
| LSO | First offset not safe for `read_committed`, or HWM when no transaction blocks visibility. |
| Earliest | First retained logical offset. |

`read_committed` reads below both HWM and LSO. It hides transaction control/aborted records and requires a matching partition marker plus current-epoch coordinator decision. `read_uncommitted` reads raw records below HWM.

## Producer State

Idempotent records use `(producerId, epoch, seqNum)` per partition. Higher epochs fence lower ones; a new epoch begins at sequence 1; duplicate retries do not append again; gaps are rejected. Producer state is checkpointed and rebuildable from partition records.

## Storage

Each partition has one `DiskHandler` with base-offset segment/index files. Async writes use a buffered channel and batch/linger policy; direct and follower writes preserve assigned offsets. Default data segments roll at 1 GiB, index capacity, or seven days. Retention can delete closed segments by time/size. Standalone keyed topics can also compact closed segments while preserving logical offsets, unkeyed records, transaction/control records, and producer recovery anchors.

## Cluster Ownership

In distributed mode, topic metadata identifies a partition leader, replicas, ISR, and leader epoch. Client data commands route to the leader. Replication carries leader/epoch fences so stale leaders cannot append as current authority. HWM advances only through the configured committed path and is restored with durable-tail clamping.

## Channel Layer

Each partition also runs an embedded in-process fan-out layer:

| Channel | Default capacity |
|---|---:|
| partition input | 10000 |
| per-group subscription | 10000 |
| consumer receive | 1000 |

These values are configurable. Full channels apply backpressure. This channel layer uses static modulo assignment for directly registered embedded consumers; network clients use the dynamic coordinator, disk reads, and durable offsets.

## Partition Expansion

Partitions can be added, not removed. Expansion creates storage/partition state for new IDs and updates metadata. Network consumer groups must rejoin/sync to receive new assignments. Existing records and offsets do not move, but future keyed routing may change.

## Key Files

- `pkg/topic/manager.go`: topic registry, policy, and publish entry points.
- `pkg/topic/topic.go`: partition selection and topic-local registration.
- `pkg/topic/partition.go`: offsets, HWM/LSO, producer/transaction visibility, and channels.
- `pkg/disk`: segments, indexes, sync, retention, and recovery.
- `pkg/coordinator`: dynamic group ownership and durable offsets.
