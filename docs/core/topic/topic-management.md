# Topic Management

`TopicManager` is the in-process registry and entry point for topic/partition operations. Durable group membership belongs to `Coordinator`; transaction lifecycle belongs to `transaction.Manager`; cluster metadata and leader ownership belong to the cluster subsystem.

## TopicManager

`TopicManager` owns:

- the topic map and create/update/delete lifecycle,
- storage handler acquisition for each partition,
- normal/sync/batch publish entry points,
- raw partition reads and explicit flush,
- connection to the stream manager,
- propagation of group and transaction decision resolvers.

It does not use a global payload-hash deduplication map. Retry safety comes from idempotent producer `(producerId, epoch, seqNum)` state and broker transactions where requested.

## Create And Update

`CreateTopicWithPolicy` normalizes policy before mutation.

| Existing state | Requested partitions | Result |
|---|---:|---|
| missing | positive | create topic and partitions |
| exists | equal | update normalized policy, no partition change |
| exists | greater | append new partitions and update policy |
| exists | lower | reject; partition count never shrinks |

New and existing partitions receive the current transaction decision resolver so `read_committed` uses coordinator authority.

## Delete

`DeleteTopic` removes the topic from the registry, closes storage handlers through the provider when supported, and removes the broker-owned topic log directory after verifying the target remains under the configured log root. Deletion is destructive and must remain an authenticated admin operation.

## Publish Entry Points

| Method family | Behavior |
|---|---|
| `Publish` / `PublishToPartition` | asynchronous local append path (`acks=0` semantics at this layer) |
| `PublishWithAck` / `PublishToPartitionWithAck` | synchronous local write path |
| idempotent variants | force producer epoch/sequence validation |
| batch variants | group records by partition before append |

Distributed command handling routes to the partition leader and applies replication/quorum logic before returning the corresponding client acknowledgement. Calling `TopicManager` directly is not a substitute for cluster routing.

## Topic

A `Topic` owns partition selection, its partition slice, policy, and embedded consumer groups. `hash_key` uses key hashing and falls back to round-robin for empty keys; `round_robin` ignores keys. Policy controls retention overrides, read/write authorization, replication metadata, idempotent mode, and event-sourcing mode.

## Partition

A `Partition` owns:

- the storage handler and logical offset sequence,
- HWM/LSO and retained offset range,
- idempotent producer state and checkpoints,
- transaction marker/open indexes and coordinator decision resolver,
- stream notifications and embedded fan-out channels,
- event-stream indexing hooks.

Only the partition data path may decide record visibility. Raw disk reads do not enforce transaction isolation.

## Embedded Consumer Groups

`RegisterConsumerGroup` creates in-process consumers and modulo assignments using configured channel capacities. This layer is static and channel-based. Network clients instead use `JOIN_GROUP`, `SYNC_GROUP`, heartbeats, generation fencing, durable offsets, and log reads through the protocol coordinator.

## Thread Safety

- `TopicManager.mu` protects the topic registry and resolver propagation.
- `Topic.mu` protects partition/policy/embedded-group state and round-robin selection.
- `Partition` uses dedicated locks for lifecycle/channels, producer state, transaction index, and disk operations.
- storage handlers serialize metadata and I/O separately.

Do not hold manager/topic locks across network forwarding or Raft apply. Coordinator and cluster mutations have their own authority and locking.

## Related Contracts

- [Topics And Partitions](topics-and-partitions.md)
- [Consumer Groups](consumer-groups.md)
- [Disk Persistence](../storage/disk-persistence.md)
- [Protocol Specification](../../protocol-spec.md)
