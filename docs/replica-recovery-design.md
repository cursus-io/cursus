# Replica recovery with durable lineage and bootstrap

Approved scope: Cursus #246, following merged #251. Three dependent, independently
testable layers are published as ready-for-review PRs through `gh stack`.

## Storage evidence

The retained log remains authoritative. A versioned, checksummed per-partition
derived index records leader-epoch boundaries and cumulative SHA-256 checkpoints.
Canonical decoded records are hashed, so legacy disk encodings compare with their
re-encoded replicas. Hashes include all persisted record fields and offsets.
Epoch zero is unknown history, never evidence of a common generation.

The first explicit proof request rebuilds a missing, damaged, stale or unsupported
index from retained records. Startup and ordinary writes never rebuild history or
synchronously publish the sidecar. Once initialized, periodic sync advances the
index by at most 1,024 durable records per pass; invalidation suspends that work
until another explicit proof request. Proof APIs sync the log, catch up the index
and durably publish it before returning evidence. Failed publication fails closed
and never reverses a successful durable append. Rebuilding cannot rewrite
committed log data. Truncation, retention, compaction and restart invalidate stale
evidence. Proofs
cover explicit start/end offsets; different starts are not comparable. Compacted
gaps require the existing compacted recovery path or a retained-range bootstrap.

## Negotiated recovery

Advertise a separate additive recovery capability in broker metadata without
raising the existing lifecycle protocol requirement. Only mutually capable peers
use indexed proofs. Old peers continue using the existing digest protocol.
Unknown mandatory recovery features fail closed.

A bounded set of cumulative prefix proofs allows one source exchange to find a
proven common boundary even for long legacy or same-epoch divergent tails. The
source only compares evidence below its authoritative HWM. Responses remain
checksummed and bound to partition, replica, source, leader, leader epoch and
lifecycle epoch. The final record digest remains a compatibility check.

## Retained-range bootstrap

When the requested prefix predates retention or cannot be proven, an active ISR
source supplies a fenced committed retained range. The receiver stages a bounded
transfer, validates checksums and offsets, and installs through a durable pending
manifest. Restart completes or safely rejects an interrupted installation.
Transaction, producer, consumer-offset and application snapshot state must remain
consistent with the installed range. Only a configured recovering replica outside
ISR can replace its local log. A stale fence is rejected before any mutation.

HWM advances only after durable installation and derived-index preparation.
Incremental catchup and existing prefix proof/readmission checks follow bootstrap.
Never expose a public reset endpoint or mutate the source's committed records.

## Validation and observability

Test index rebuild/corruption and legacy histories; rollback/retention/compaction;
old/new capability combinations; stale leader/lifecycle/source fencing; restarts
during staging, installation and catchup; transaction and unrelated-partition
isolation. A three-node fault test must demonstrate bounded recovery and stable
full ISR for at least twice the configured replica-lag timeout. Report mode,
source, offsets, progress, retries, rebuild/bootstrap/readmission transitions and
terminal versus transient failures. CI and the initial CodeRabbit review must be
resolved before handoff. PR merging remains user-owned.
