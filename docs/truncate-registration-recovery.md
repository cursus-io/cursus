# Truncate cleanup and group re-registration

## Cause

Baseline: `a49138e7922aa66b14cb4482a8b084e5b9c1b8cd`.

The cleanup worker captures current local groups, not groups at the truncate
commit. It accepts only a tombstone newer than the captured registration and all
observed live records. A re-registration therefore replaces the proof needed by
an old truncate. An owner that already materialized that truncate has no pending
operation to generate another tombstone. Generating one would destroy new state.

Topic lifecycle epochs, group registration epochs, and membership generations
are independent domains. Registration epochs order one group's registrations and
deletions; membership generations do not order topic operations. Ordinary metadata
replay also copies historical membership into a new registration when no matching
lifecycle snapshot exists; the baseline applies its epoch check only to strict replay.
This can explain a local generation 14 versus an owner's fresh generation 0, but
production history must be inspected to attribute those exact values conclusively.

## Kafka comparison

- [Accepted KIP-516](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/127406437/KIP-516%2BTopic%2BIdentifiers)
  uses topic identity to distinguish deletion/recreation with the same name.
  Adopt the identity principle, not numeric comparison of unrelated epochs.
- [Kafka 3.9 OffsetMetadataManager](https://github.com/apache/kafka/blob/3.9.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java)
  generates durable records and applies them through replay. Adopt committed
  evidence and preserve coordinator ownership fencing. Kafka offset cleanup is
  not a Cursus whole-group truncate contract.
- [Kafka 3.9 operations](https://kafka.apache.org/39/operations/basic-kafka-operations/)
  treats topic administration and consumer-group administration separately.
  Cursus must retain its existing explicit whole-group cleanup scope checks.

The Cursus boundary captures the authoritative committed end of every
consumer-metadata partition in the serialized Raft truncate application. Bind
the boundary to the topic incarnation/lifecycle and internal topic identity.
Persist it in snapshots, never infer it from wall-clock timestamps or current HWM.
During the bounded scan, classify registration records on either side of this
boundary. Preserve a later registration only with a complete validated prefix
and evidence that any previous live group was deleted before the new registration.
The final local cleanup check must use the same proof. Missing boundary/history
must not authorize a new deletion or exempt a live registration.

The snapshot addition is optional recovery evidence. Old snapshots that already
discarded the truncate command cannot reconstruct its historical boundary from
the current HWM. They remain fail closed; retained Raft replay can reconstruct it.
No production state mutation or destructive workaround is part of this work.

## Implemented behavior

The scanner retains the highest live group epoch before the cut, the latest
validated deletion epoch, and the latest registration's position relative to the
cut. A later same-name registration survives only when a committed scoped
tombstone proves the previous live epoch was deleted. A first registration also
requires known absence of historical Raft-only groups; contention or unknown
legacy provenance does not establish absence. Timestamp values never order the
operation. Shared and pattern subscriptions retain their existing fail-closed
scope protection; registration initial offsets do not authorize preservation.

An already superseded local group can be discarded using its own tombstone;
normal ownership recovery installs the later group from unchanged durable
records. No tombstone is written for a protected registration. Only a recovered,
fenced owner may write a deletion for a group proven live before the cut.

The final Raft-apply check consumes the same validated operation-bound proof and
nonblockingly checks the current partition identity, generation, committed HWM
and snapshot status without reading disk. Rollback or replacement invalidates it.
Snapshot restore and definition updates leave truncation pending until that
check succeeds. Membership generations are no longer copied across registration
epochs during ordinary replay. The normal same-epoch compatibility path remains.

Checkpoint version 2 binds the cut and accumulated evidence to the current
targets and durable log generation. Version 1, damaged or mismatched checkpoints
are discarded and reread. Record/byte/time limits, cancellation and the existing
scheduler/backoff remain. Completed batches retain per-registration proofs so
more than 64 later groups cannot starve subsequent targets. A restart may repeat
completed batches; cursors for the active matching batch remain reusable. These
proofs are disposable and never replace the log or coordinator startup replay.

## Compatibility and deployment conditions

- Wire commands and consumer metadata record formats are unchanged. Existing
  truncate Raft commands reconstruct the cut when replayed by the new binary.
- Lifecycle capability 8 advertises the optional snapshot-11 boundary field.
  New snapshots omit it while **any registered broker**, including an offline
  broker, advertises less than 8. Old readers reject unknown snapshot fields;
  after boundary-bearing snapshots are written, downgrading to an old reader is
  unsupported. Snapshot formats 9/10/11 without the field still load conservatively.
- Upgrade all registered brokers before relying on persisted boundaries. The
  cluster chart's expected binary contract becomes 8; this updates the packaging
  assertion and does not weaken readiness or storage validation.
- For the reported already-pending truncate, upgrading alone is sufficient only
  if the node can replay that truncate with its preceding authoritative metadata
  state, or restore a new snapshot that actually contains its historical cut.
  A post-truncate old snapshot plus the current metadata log does **not** prove
  the missing cross-log order. Such a node reports `truncate ordering evidence
  unavailable`; a fresh snapshot or checkpoint cannot invent that evidence.
  Establish which historical artifacts survive before choosing an operational
  recovery procedure. This PR neither edits production files nor prescribes a
  destructive reset to manufacture proof.

## Reproduction evidence

The deployed commit reproduces a fully scanned three-record history ending at
`Next=End=3`, with epoch 3 registration evidence, while repeatedly returning
`waiting for a newer committed group tombstone`. A separate baseline regression
fails because a fresh registration inherits membership generation 14; the fixed
replay returns generation 0 with no inherited members.

The isolated three-broker delayed-follower scenario passed its first run: the
owner and other broker completed truncate, the same group was registered with
an active member and committed offset, then the follower restarted and recovered
Ready/full ISR. Its cleanup scanned 8 records / 3,921 frame bytes in 2 reads after
restart. These are small-fixture counters, not estimates for the reported
286,710,274-byte production scan. Final validation results are recorded in the PR.

## Regression and fault checks defined before implementation

- Delayed follower cleanup, completed owner cleanup, same-name registration,
  follower restart: preserve registration, members and nonzero offsets; converge
  readiness and full ISR in an isolated three-broker fixture.
- Empty registrations and active new sessions; ownership movement; registration
  between bounded passes; repeated truncate; snapshot persistence/restore.
- Equal and widely separated topic/group epochs; missing, malformed, conflicting,
  transactional and out-of-commit evidence; changed metadata incarnation.
- Checkpoint resume/invalidation bound to the operation and target; reject old
  cache format. Retain cancellation, backoff, fairness and per-pass byte limits.
- Record replay bytes, records and reads across restart, not just requests.
- Verify ordinary and strict replay never transfer old membership across group
  registration epochs. Run focused tests, race tests and cluster recovery tests.
