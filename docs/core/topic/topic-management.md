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

Protocol `CREATE` is implemented as a `DefinitionPatch`. Pointer presence distinguishes omission from an explicit zero, false, or empty ACL. Defaults are evaluated only when the topic is absent; an existing definition is read and merged while `TopicManager.mu` is held.

| Existing state | Requested partitions | Result |
|---|---:|---|
| missing | positive | create topic and partitions |
| exists | omitted/equal | preserve partitions; patch only supplied fields |
| exists | greater | append new partitions and patch supplied fields |
| exists | lower | reject; partition count never shrinks |

`replication_factor`, `idempotent`, and `event_sourcing` are immutable after creation. Restating the current value is accepted; a conflicting explicit value is rejected. Effective changes increment the durable definition revision and no-op patches retain it. New and existing partitions receive the current transaction decision resolver so `read_committed` uses coordinator authority. Retention and cleanup policy are propagated to every partition handler.

Topic names use a portable storage contract: 1-249 ASCII bytes containing letters, digits, `.`, `_`, `-`, or `=`; `.` and `..` are reserved. This prevents a protocol topic identifier from escaping the broker-owned log root.

`cleanup_policy` accepts `delete`, `compact`, and `delete,compact`. Compact policies are rejected when distribution is enabled or the topic is event-sourcing. Standalone compaction details are in [Log Compaction](../storage/log-compaction.md).

For standalone create/update, new partition handlers are staged while the topic lock excludes publishers. The complete target definition is atomically persisted before policy or partition count becomes visible. A persistence failure closes/evicts staged handlers and leaves the live definition unchanged. In distributed mode the Raft command carries the complete new-topic defaults plus the presence-aware patch. It also carries the merged legacy topic fields so older FSM readers in a rolling upgrade can apply the same authoritative definition while ignoring the new JSON fields. CREATE command construction is serialized on the leader so these compatibility fields cannot lose disjoint concurrent patches. The new FSM still merges against its current authoritative definition while holding the FSM lock. Existing partition leader epoch/HWM state is retained and metadata is allocated only for newly added partitions.

## Startup Recovery

Standalone brokers load `{log_dir}/__topic_metadata.json` before coordinator initialization and static-group registration. Manifest version 2 restores revision, replication factor, partition count, idempotent/event-sourcing flags, cleanup/retention, partitioner, auth policy, and ACLs. Version 1 definitions are normalized to revision 1 and replication factor 3 and are written as version 2 on the next mutation. Unknown fields, duplicate topics, unsupported versions, invalid names, and malformed policy fail broker startup instead of silently weakening authorization or cleanup behavior.

Brokers upgraded from versions without the manifest do not guess security or event-sourcing policy from segment filenames. If persisted partition logs exist without a manifest, or a manifest omits a persisted topic directory, startup fails and lists the orphaned topics. Operators must migrate or archive those directories and provide authoritative definitions before restart. A normal `CREATE` also rejects a name whose orphaned logs remain, preventing deleted data from being silently resurrected.

The internal offset topic is recreated by the coordinator and then enters the manifest on a new data directory. Existing pre-manifest offset logs require the same explicit migration as application topics.

Distributed brokers keep topic definitions in the FSM and snapshot format version 7. Snapshot restore normalizes older definitions, rebuilds the topic registry, and then reconciles committed HWMs. Version 6 definitions receive revision 1 and infer replication factor from consistent replica metadata, with 3 as the fallback when old metadata has no replicas. Version 5 and older snapshots can reconstruct partition count/idempotent mode from partition metadata and use the historical default topic policy. Version 7 fails closed when revision or replication factor is absent.

## Delete

`DELETE topic=<name> [if_exists=true]` is an authenticated admin operation. The option is explicitly opt-in: legacy deletion of a missing topic still returns `topic_not_found`, while `if_exists=true` returns `deleted=false`. The broker-owned `__consumer_offsets` topic is always protected.

Deletion takes an exclusive topic-lifecycle fence. It fails closed while any group for the topic has active members or while an open/committing transaction references the topic. Inactive groups receive normal lifecycle tombstones and their offsets are removed. Terminal transactions retain their decision and operations for other topics but drop operations for the deleted topic. Distributed producer sequence state is removed. Event-sourcing indexes and snapshot stores are closed before partition storage is removed. These rules prevent a same-name recreation from reviving old records, offsets, producer sequences, transaction operations, or event-sourcing state.

In standalone mode the command preflights active references without mutation, then `DeleteTopicDurable` commits manifest removal before removing the topic from the registry, stopping partition workers, closing handlers, and deleting the broker-owned log directory after path validation. Only after that logical commit does the command write inactive-group tombstones and rewrite transaction state. A manifest or pre-commit event-state failure therefore leaves the topic, offsets, and transaction references live. Post-commit storage or dependency failures return `deleted=true cleanup_pending=true`; `if_exists=true` retries dependency cleanup. A missing topic with stale group/transaction references cannot be recreated, and orphan storage is also rejected, so old state cannot attach to a new definition. In distributed mode the leader serializes lifecycle cleanup and topic/partition removal in the Raft apply order. FSM replay and snapshots retain the absence of topic, group, transaction-reference, and producer state. Node-local storage failures are tracked by the materialization reconciler instead of rolling back the committed cluster state.

## Data Reset Is Not Delete

Cursus has no safe `TRUNCATE` or `PURGE` operation. `DELETE` followed by `CREATE` is prohibited as an implicit reset because it changes topic identity and cannot make record removal, log start/end/HWM reset, group lifecycle fencing, producer/transaction cleanup, event-sourcing cleanup, the standalone manifest, and distributed FSM snapshots atomic as one guarded decision. A future first-class operation needs an expected definition revision (or equivalent guard), explicit admin authorization, and a fail-closed or fencing contract for active producers and consumers; [issue #164](https://github.com/cursus-io/cursus/issues/164) tracks that work. Operators and Kubernetes reconcilers should require an explicit desired `absent` tombstone and an independent approval boundary for deletion; removing a topic from a desired list alone must not trigger it.

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
