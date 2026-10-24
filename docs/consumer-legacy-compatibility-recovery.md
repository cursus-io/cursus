# Legacy consumer compatibility recovery

An upgraded broker can finish startup from an older Raft snapshot and retained
Raft suffix while its consumer recovery envelope still has unknown historical
authority. Capability-only routing activation previously invalidated the
startup ownership cache. The subsequent ownership reload required independent
Raft compatibility state and source provenance that the old snapshot never
recorded. Brokers could therefore report ready and list groups while group
status and offset fetch returned `coordinator_not_available`.

## Existing evidence and support boundary

The mixed group snapshot is not an independent historical Raft baseline. A
missing recovery envelope, or an envelope whose compatibility/source flags are
false, stays unknown after this recovery. Retained Raft entries are replayed by
the existing FSM restore path; scanning surviving consumer records cannot
reconstruct discarded Raft history or establish source-partition provenance.

Compatibility recovery instead proves a narrower operation: materialize the
existing snapshot plus every committed consumer source, under one current FSM
metadata cut, before publishing current local ownership. It does not grant
permission for destructive or partition-scoped bootstrap.

1. Require local coverage of every consumer metadata partition through its
   authoritative FSM HWM, including partitions outside the current hash route.
2. Drain consumer admission, hold the FSM transition fence and immutable local
   partition views, and revalidate the topology/process/epoch vector.
3. Certify the physical range `[0, HWM)` without gaps, including records hidden
   by transaction visibility. Local HWM alone is insufficient. Read errors,
   missing prefixes/tails and unavailable sources fail before publication.
4. Replay committed visible records strictly into detached state. Use the
   existing snapshot for Raft-only compatibility state, but require modern
   acknowledged snapshot revisions to have matching or superseding committed
   evidence. Registration epochs and revisions govern replacement; a new
   registration epoch can supersede an older offset. Invalid records and key
   conflicts are errors, not reasons to skip history.
5. Revalidate the recovery lease, restore generation, authority revision and
   held coverage immediately before publication. Publish recovered state and
   current ownership together. Already activated routing retains its durable
   activation bit and receives the required ownership tuples.

This path conservatively rejects unknown-authority histories with compacted
holes or a truncated prefix. Such inputs need independently verifiable retained
evidence; an empty log is not proof that the lost history never existed. Known
authority continues through the existing strict/scoped recovery path. No new
checkpoint, snapshot version, envelope version or broker protocol number is
introduced. No topic, group or offset is deleted or reset by the migration.

Errors preserve the installed consumer state and keep requests fenced. Normal
Raft startup/replication may still advance its own durable state; this is not an
offline filesystem transaction. The compatibility validator itself performs
reads and detached preparation before it publishes anything.

## Cluster activation

An additive `consumer_routing_ready` registration field is true only after the
local runtime has completed recovery, has known strict bootstrap authority and
has recovered current local ownership. Older registrations omit it and cannot
satisfy a new activation. Registration refresh propagates both readiness and
revocation without changing the existing lifecycle/recovery protocol numbers.

The leader waits for every registered process to be active and ready and every
current Raft voter to be represented. The command carries the complete process
identity set. FSM application rechecks its size, uniqueness, incarnations,
capabilities and readiness. A delayed registration, restart, revocation or added
broker invalidates the proposal. Historical activation commands omit that set
and retain their original replay semantics; this is necessary for existing
Raft logs. Coordinated upgrades remain required. An old leader's historical
activation is not retrospectively rewritten, and activated legacy data is
handled by the certified compatibility path.

Legacy availability and new strict activation are separate conditions. A
successfully recovered unknown-authority cluster serves its existing route
without claiming eligibility for stronger bootstrap operations.

## Apache Kafka comparison

The design and regression matrix were selected against
[Kafka 4.1.0 CoordinatorLoaderImpl](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/coordinator/group/CoordinatorLoaderImpl.scala),
[the 4.1 distribution design](https://kafka.apache.org/41/implementation/distribution/)
and [the 4.1 upgrade procedure](https://kafka.apache.org/41/getting-started/upgrade/).

Cursus adopts committed-cut replay, ownership fencing, separate binary upgrade
and feature activation, and accounting for records and bytes read. Kafka's
coordinator loader tracks written/committed boundaries and transaction markers,
and fails loading when leadership is lost. Cursus cannot assume Kafka's
single authoritative partition log: its legacy state also depends on Raft
snapshots, replay and historical source partitions. Kafka's ability to load a
compacted authoritative log therefore does not justify certifying Cursus's
unknown mixed snapshot from surviving records. Cursus rejects that ambiguity.

The safety benefit is preserving unknown authority and refusing incomplete
inputs. The availability benefit is recovering already activated legacy data
without clearing activation or stored offsets. The cost is a full physical
scan plus visible replay over all source partitions at startup/ownership
recovery. Each physical pass is paged at 1,024 records/1 MiB (one already
accepted oversized record may exceed the byte budget). Certification is reused
only inside the same held views/fence; there is no persistent cache. Existing
`cursus_recovery_work_total` counters include pages, decoded frames and decoded
key/payload bytes for this pass as well as replay. These are logical work
counters, not device I/O or an efficiency improvement claim.

## Regression and incident validation

Synthetic tests cover missing committed coverage, prefix/interior/tail holes,
corrupt records, key mismatches, uncommitted tails, short replay pages, new
registration epochs, unsupported snapshot revisions, stale leases/restores,
expired coverage, cluster process changes, readiness revocation, historical
activation and repeated activated-snapshot restore. Failed preparation must
leave exported group state unchanged and unknown authority must remain unknown.

Private incident verification uses fresh writable copies of one immutable
three-broker backup. Neither the backup nor its payloads, group names or offset
values are repository fixtures. Required checks are 3/3 Ready, full ISR and
all 42 original HWMs unchanged before synthetic writes; individual status for
all 19 groups and exact fetch equality for all 26 stored commits, including
the Commerce group. Listing 19 groups alone is not recovery evidence.

Repeat those comparisons after restart, synthetic append/read/commit and exact
request retries, coordinator termination/rejoin, and recovery of data first
started by the affected candidate image. Synthetic writes intentionally advance
their new partition and the consumer metadata HWM; report those deltas rather
than describing the post-write HWM as unchanged. Production/Temporal changes
and deployment approval are outside this verification.
