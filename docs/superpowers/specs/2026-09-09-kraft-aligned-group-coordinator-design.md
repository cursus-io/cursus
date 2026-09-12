# KRaft-Aligned Group Coordinator Placement

## Decision

Replace Cursus's group-to-active-broker consistent-hash ring with Kafka-style
group-to-`__consumer_offsets` partition mapping. The durable leader of that
partition is the only group coordinator.

## Ownership

For a group `g`, the router computes
`GenerateID(ConsumerMetadataGroupPartitionKey(g)) % partitionCount`, where
`partitionCount` is read from the durable FSM metadata for
`__consumer_offsets-0`. It then reads the durable metadata for that offsets
partition. Its active leader is returned by `FIND_COORDINATOR` and receives
all group commands.

Record keys remain record-specific compaction identities. The producer selects
the partition using the group-derived key, so all group records still land on
the group coordinator partition without compacting lifecycle and offset
records into one another.
This unifies group ownership and offset-log leadership.

## Failure Semantics

The controller leader may use soft heartbeat state to propose broker fencing
and ISR/partition-leader changes, but routing never observes soft state. A
router result changes only after the corresponding `PARTITION` Raft entry
applies. If the offsets partition has no active leader, discovery returns a
retryable unavailable error; it does not fall back to a different broker or
report a successful offset commit.

Group membership and generation state are recovered from the same replicated
offsets partition before the selected leader serves the group. A group is not
accepted merely because a broker is locally reachable.

## Group Lifecycle Migration

`GROUP_SYNC` cannot simply be removed unless member and generation state have
a replacement durability boundary. The implemented replacement is a
version-4 group lifecycle *snapshot* record in the selected offsets partition.

1. The offsets-partition leader derives the next membership/generation and
   assignment snapshot without mutating the live group, then appends it.
2. A successful append is applied locally and is the durability point for the
   client response. Failed append leaves the live group unchanged.
3. Before serving a locally owned group, a broker replays the replicated
   offsets metadata and restores its latest lifecycle snapshot. Recovered
   member leases begin afresh, preventing an inherited local timer from
   prematurely expiring a member after leader change.
4. The compatibility target is `origin/main`: its v1-v3 registration and
   offset records are read once and materialized into v4 state. Intermediate
   records written by this feature branch are not a compatibility contract.
   `GROUP_SYNC` replay remains readable only for `origin/main` recovery and
   is never emitted by the new runtime for register, join, leave, or expiry.

## Compatibility

There is no hash-ring fallback in distributed mode: a cluster must bootstrap
the internal offsets topic topology before serving group discovery. This is
already a readiness requirement. Existing record keys and replay format are
unchanged. Standalone mode is unchanged.

## Tests

- Routing resolves a group to the durable leader of its offsets partition.
- Adding or removing unrelated active brokers does not change that result.
- A durable `PARTITION` leader change changes the coordinator only after FSM
  apply; an inactive or absent leader is never returned.
- The existing Docker failover test stops the current coordinator, observes a
  durable partition-leader-backed replacement, and verifies a fenced retry or
  durable offset commit.
- Lifecycle snapshot replay restores generation and member assignments after
  coordinator ownership moves; a failed lifecycle append does not advance
  fencing state.
