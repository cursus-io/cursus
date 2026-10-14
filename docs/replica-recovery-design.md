# Replica recovery with durable lineage and bootstrap

Approved scope: Cursus #246, following merged #251. Three dependent, independently
testable layers are published as ready-for-review PRs through `gh stack`.

Implementation status: storage lineage and indexed wire protocol 1 are present.
Production registration advertises recovery protocol **0** until the complete
bootstrap bundle is installed. The retained-range bootstrap section below is
planned behavior, not an available recovery service in this layer.

## Kafka comparison and regression contract

[KIP-101](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=177052956)
uses leader-epoch boundaries to avoid discarding a valid prefix unnecessarily.
Cursus adopts that recovery-cost objective, but must prove cumulative record
equality for same-epoch divergence and legacy epoch-zero records. An epoch match
alone cannot justify truncation in Cursus. Tail-relative probes preserve the last
good record when only the final record diverges; a matching final record cannot
hide earlier divergence.

[KIP-320](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/87295403/KIP-320%2BAllow%2Bfetchers%2Bto%2Bdetect%2Band%2Bhandle%2Blog%2Btruncation)
fences fetchers using leader epochs. Cursus keeps its source, receiver, leader,
leader-epoch and lifecycle fences through storage mutation. The existing legacy
wire fields and lifecycle protocol 6 remain compatible; unknown mandatory
recovery versions fail closed.

[KIP-392](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/95653762/KIP-392%2BAllow%2Bconsumers%2Bto%2Bfetch%2Bfrom%2Bclosest%2Breplica)
accounts for replicas retaining different log starts. The implemented epoch and
out-of-range paths in Kafka's versioned
[4.1 AbstractFetcherThread](https://github.com/apache/kafka/blob/4.1/core/src/main/scala/kafka/server/AbstractFetcherThread.scala)
also distinguish truncation from recovery below a retained start. Cursus cannot
make different cumulative-hash anchors comparable by selecting the larger start.
It requires a separately fenced retained-range bootstrap with derived-state
recovery. Until that is complete, unavailable recovery stays unavailable: this
layer's capability gate prevents premature activation, but does not solve
retention recovery.

Regression checks derived before implementation cover every small-span rollback
boundary, nonzero anchors, a near-million-record tail with one bad record,
MaxUint64 arithmetic, real-disk and source/receiver FSM recovery with single-tail,
early reconvergent and long divergence, authoritative-HWM caps, stale fences and
incomparable anchors. Direct and forwarded registration must advertise the same
identity, incarnation, transaction shard count, lifecycle 6 and recovery 0.
Recovery tests count retransferred records and JSON record-batch bytes as well as
exchanges; these are not claims about bounded disk bytes or elapsed time.

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

Recovery capability is separate additive broker metadata, without raising the
existing lifecycle protocol requirement. Protocol 1 remains implemented and
explicit capability-1 fixtures exercise it, but both production registration
paths use one process-owned payload advertising 0. There is no public activation
flag or setter. Production peers continue using the existing digest protocol.
Unknown mandatory recovery features fail closed. A later complete bundle must
install source, transport, receiver, resumable installation, derived-state recovery
and provider wiring before advertising protocol 2; its startup advertisement
remains 0 until ready. Do not temporarily activate protocol 1 during startup.

A bounded set of cumulative prefix proofs allows one source exchange to find a
proven common boundary even for long legacy or same-epoch divergent tails. The
source only compares evidence below its authoritative HWM. Responses remain
checksummed and bound to partition, replica, source, leader, leader epoch and
lifecycle epoch. The final record digest remains a compatibility check.

Candidates are the exact tail, then tail minus 1, 3, 7, ... records, followed by
the retained-start anchor. At most 63 slots precede that anchor, giving at most
64 strictly descending endpoints over the full uint64 range. For stable,
contiguous histories with the same retained anchor, let
`D = tail - min(longest_common_prefix, source_HWM)` be the required rollback and
`R` the chosen rollback, with offsets expressed as exclusive ends. If `D = 0`,
`R = 0`; if `D = 1`, `R = 1`. For retained spans at most MaxInt64 and `D > 0`,
`D <= R < 2D`. Over the full uint64 range the candidate cap gives
`D <= R < 4D`. These bound records removed; with a fixed source HWM they also
bound retransferred records within the receiver's original tail by `R`. New
records beyond that tail are ordinary catch-up outside this bound. These bounds
do not bound bytes, hashing work or
wall-clock time, and do not apply to incomparable or compacted/gapped histories.

## Retained-range bootstrap

This is the next layer, not implemented by the capability gate. Today a
`BootstrapRequired` response stops recovery without mutation or a legacy fallback.
The planned behavior is: when the requested prefix predates retention or cannot
be proven, an active ISR
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

## Outstanding audit follow-ups

- Cold sidecar rebuilds still scan retained history under recovery/storage locks.
  Measure bytes hashed, disk I/O, fence/lock contention and elapsed time; introduce
  byte/time budgets separately. Kafka's
  [4.1 LeaderEpochFileCache](https://github.com/apache/kafka/blob/4.1/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java)
  loads persisted epoch checkpoints, while Cursus currently rebuilds authoritative
  cumulative evidence on its first explicit proof request. A record cap on later
  periodic updates does not fix cold-start work or variable-size records.
- Compacted recovery retains full-prefix rescans. Measure repeated scan bytes and
  work before claiming an efficient retained-range solution.
- Consumer metadata still has a transitional global strict replay gate. Validate
  availability under busy unrelated traffic and prepare partition-scoped recovery;
  Kafka's [4.1 GroupCoordinatorService](https://github.com/apache/kafka/blob/4.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java)
  has per-partition election/resignation hooks. This layer does not change that
  architecture or claim to fix its availability cost.
