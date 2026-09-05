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

`cleanup_policy` accepts `delete`, `compact`, and `delete,compact`. Compact policies are accepted for application topics in standalone and distributed mode, but remain invalid for event-sourcing topics. A distributed create or update that enables compaction requires every active broker to advertise lifecycle protocol version 2; actual cleaner passes additionally require full ISR and matching authoritative HWM, lifecycle epoch, and cleanup policy. See [Log Compaction](../storage/log-compaction.md).

For standalone create/update, new partition handlers are staged while the topic lock excludes publishers. The complete target definition is atomically persisted before policy or partition count becomes visible. A persistence failure closes/evicts staged handlers and leaves the live definition unchanged. In distributed mode the canonical Raft command contains the complete new-topic definition, a presence-aware patch, and the committed-HWM format marker. Unknown or duplicate scalar topic fields are rejected. CREATE command construction is serialized on the leader, and the FSM merges against its current authoritative definition while holding the FSM lock. Existing partition leader epoch/HWM state is retained and metadata is allocated only for newly added partitions.

## Startup Recovery

Standalone brokers load `{log_dir}/__topic_metadata.json` before coordinator initialization and static-group registration. Runtime reads and writes only manifest version 3, which requires revision, replication factor, and lifecycle epoch for every topic. Unknown fields, duplicate topics, unsupported versions, invalid names, missing required fields, and malformed policy fail broker startup instead of silently weakening authorization or cleanup behavior.

Brokers upgraded from versions without the manifest do not guess security or event-sourcing policy from segment filenames. If persisted partition logs exist without a manifest, or a manifest omits a persisted topic directory, startup fails and lists the orphaned topics. Earlier storage must be archived for forensics or removed as part of a complete clean bootstrap; it cannot be converted into a current manifest. A normal `CREATE` also rejects a name whose orphaned logs remain, preventing deleted data from being silently resurrected.

The internal offset topic is recreated by the coordinator and then enters the manifest on a new data directory. Existing pre-manifest offset logs are unsupported and require the same complete clean bootstrap as application topics.

Distributed brokers keep topic definitions in the FSM. Snapshot restore accepts only version 9, validates every definition and nested group/transaction snapshot before mutation, rebuilds the topic registry, and reconciles each partition to its explicit committed HWM. Snapshots through version 8 and version-9 state with missing markers or required fields fail with a clean-bootstrap error.

## Delete

`DELETE topic=<name> [if_exists=true]` is an authenticated admin operation. The option is explicitly opt-in: without it a missing topic returns `topic_not_found`, while `if_exists=true` returns `deleted=false`. The broker-owned `__consumer_offsets` topic is always protected.

Deletion takes an exclusive topic-lifecycle fence. It fails closed while any group for the topic has active members or while an open/committing transaction references the topic. Inactive groups receive normal lifecycle tombstones and their offsets are removed. Terminal transactions retain their decision and operations for other topics but drop operations for the deleted topic. Distributed producer sequence state is removed. Event-sourcing indexes and snapshot stores are closed before partition storage is removed. These rules prevent a same-name recreation from reviving old records, offsets, producer sequences, transaction operations, or event-sourcing state.

In standalone mode the command preflights active references without mutation, then `DeleteTopicDurable` commits manifest removal before removing the topic from the registry, stopping partition workers, closing handlers, and deleting the broker-owned log directory after path validation. Only after that logical commit does the command write inactive-group tombstones and rewrite transaction state. A manifest or pre-commit event-state failure therefore leaves the topic, offsets, and transaction references live. Post-commit storage or dependency failures return `deleted=true cleanup_pending=true`; `if_exists=true` retries dependency cleanup. A missing topic with stale group/transaction references cannot be recreated, and orphan storage is also rejected, so old state cannot attach to a new definition. In distributed mode the leader serializes lifecycle cleanup and topic/partition removal in the Raft apply order. FSM replay and snapshots retain the absence of topic, group, transaction-reference, and producer state. Node-local storage failures are tracked by the materialization reconciler instead of rolling back the committed cluster state.

## Truncate

`TRUNCATE topic=<name> expected_revision=<N>` is the admin-only data-reset operation. It retains the complete definition, increments the definition revision and lifecycle epoch, empties every partition, and resets LEO/HWM to zero. It fails closed when a group has active members, an open/committing transaction references the topic, the revision is stale, or the target is broker-owned `__consumer_offsets`.

Successful truncation removes inactive groups and offsets, producer sequence state, terminal transaction references, and event-sourcing indexes/snapshots. In standalone mode the version 3 manifest commits the new epoch before physical storage reset. A synced epoch marker is the publication boundary; an absent or mismatched marker fences all access, makes broker readiness fail, and restart resumes cleanup. In distributed mode one serialized `TOPIC_TRUNCATE` transition updates definition and partition metadata, advances partition leader epochs, drops replicated record/producer state, and records node-local failures for reconciliation. Message replication, HWM commits, and event snapshots carry the lifecycle epoch so delayed pre-truncate work cannot enter the empty generation.

Distributed truncate is rejected unless every active broker advertises lifecycle protocol version 1. Mixed protocol generations are not a supported runtime mode. Snapshot version 9 and manifest version 3 require a clean bootstrap when deploying from an earlier recovery format, and downgrade is unsupported. `DELETE` followed by `CREATE` is still not a substitute because it changes topic identity and has a different retry contract.

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
