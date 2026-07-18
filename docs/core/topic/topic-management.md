# Topic Management

`TopicManager` is the in-process registry and entry point for topic/partition operations. Its topic definitions are durable in the standalone manifest or cluster FSM. Durable group membership belongs to `Coordinator`; transaction lifecycle belongs to `transaction.Manager`; partition leader ownership belongs to the cluster subsystem.

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

New and existing partitions receive the current transaction decision resolver so `read_committed` uses coordinator authority. Retention and cleanup policy are propagated to every partition handler. An existing event-sourcing topic remains protected even if a later idempotent create request omits or clears `event_sourcing`.

Topic names use a portable storage contract: 1-249 ASCII bytes containing letters, digits, `.`, `_`, `-`, or `=`; `.` and `..` are reserved. This prevents a protocol topic identifier from escaping the broker-owned log root.

`cleanup_policy` accepts `delete`, `compact`, and `delete,compact`. Compact policies are rejected when distribution is enabled or the topic is event-sourcing. Standalone compaction details are in [Log Compaction](../storage/log-compaction.md).

For standalone create/update, new partition handlers are staged while the topic lock excludes publishers. The complete target definition is atomically persisted before policy or partition count becomes visible. A persistence failure closes/evicts staged handlers and leaves the live definition unchanged. In distributed mode the FSM retains existing partition leader epoch/HWM state on repeated create and allocates metadata only for newly added partitions.

## Startup Recovery

Standalone brokers load `{log_dir}/__topic_metadata.json` before coordinator initialization and static-group registration. The versioned manifest restores partition count, idempotent/event-sourcing flags, cleanup/retention, partitioner, auth policy, and ACLs. Unknown fields, duplicate topics, unsupported versions, invalid names, and malformed policy fail broker startup instead of silently weakening authorization or cleanup behavior.

Brokers upgraded from versions without the manifest do not guess security or event-sourcing policy from segment filenames. If persisted partition logs exist without a manifest, or a manifest omits a persisted topic directory, startup fails and lists the orphaned topics. Operators must migrate or archive those directories and provide authoritative definitions before restart. A normal `CREATE` also rejects a name whose orphaned logs remain, preventing deleted data from being silently resurrected.

The internal offset topic is recreated by the coordinator and then enters the manifest on a new data directory. Existing pre-manifest offset logs require the same explicit migration as application topics.

Distributed brokers keep topic definitions in the FSM and snapshot format version 6. Snapshot restore rebuilds the topic registry before committed HWM reconciliation. Version 5 and older snapshots can reconstruct partition count/idempotent mode from partition metadata, using the historical default topic policy because those snapshots did not retain the richer definition.

## Delete

`DeleteTopicDurable` first commits removal from standalone metadata. It then removes the topic from the registry, stops partition workers, closes storage handlers, and removes the broker-owned topic log directory after verifying the target remains under the configured log root. A manifest failure leaves the topic live. Once the manifest removal commits, physical cleanup failures are logged for operator remediation but do not turn the logically successful deletion into an error response. Deletion is destructive and must remain an authenticated admin operation.

## Publish Entry Points

| Method family | Behavior |
|---|---|
| `Publish` / `PublishToPartition` | asynchronous local append path (`acks=0` semantics at this layer) |
| `PublishWithAck` / `PublishToPartitionWithAck` | synchronous local write path |
| idempotent variants | force producer epoch/sequence validation |
| batch variants | group records by partition before append |

Distributed command handling routes to the partition leader and applies replication/quorum logic before returning the corresponding client acknowledgement. Calling `TopicManager` directly is not a substitute for cluster routing.

## Topic

A `Topic` owns partition selection, its partition slice, policy, and embedded consumer groups. `hash_key` uses key hashing and falls back to round-robin for empty keys; `round_robin` ignores keys. Policy controls cleanup/retention overrides and read/write authorization. Replication, idempotent mode, and event-sourcing mode are separate topic metadata.

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

- `TopicManager.mu` protects the topic registry, resolver propagation, and whole-manifest definition snapshots.
- `Topic.mu` protects partition/policy/embedded-group state and round-robin selection.
- `Partition` uses dedicated locks for lifecycle/channels, producer state, transaction index, and disk operations.
- storage handlers serialize metadata and I/O separately.

Do not hold manager/topic locks across network forwarding or Raft apply. Coordinator and cluster mutations have their own authority and locking.

## Related Contracts

- [Topics And Partitions](topics-and-partitions.md)
- [Consumer Groups](consumer-groups.md)
- [Disk Persistence](../storage/disk-persistence.md)
- [Protocol Specification](../../protocol-spec.md)
