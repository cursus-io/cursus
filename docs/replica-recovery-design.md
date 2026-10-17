# Replica recovery with durable lineage and bootstrap

Approved scope: Cursus #246, following merged #251. Three dependent, independently
testable layers are published as ready-for-review PRs through `gh stack`.

Implementation status: storage lineage and indexed wire protocol 1 are present.
Production registration advertises recovery protocol **0** until the complete
bootstrap bundle is installed. The retained-range bootstrap section below is
planned behavior, not an available recovery service in this layer.

## Recovery design and regression contract

Recovery must preserve proven common records while detecting same-epoch
divergence and legacy epoch-zero records. An epoch match alone cannot justify
truncation. Tail-relative probes preserve the last good record when only the
final record diverges; a matching final record cannot hide earlier divergence.

Source, receiver, leader, leader-epoch and lifecycle fences remain in force
through storage mutation. The existing legacy wire fields and lifecycle
protocol 6 remain compatible; unknown mandatory recovery versions fail closed.

Different cumulative-hash anchors cannot be made comparable by selecting the
larger start. Recovery below a retained start requires a separately fenced
retained-range bootstrap with derived-state recovery. Until that is complete,
unavailable recovery stays unavailable: this layer's capability gate prevents
premature activation, but does not solve retention recovery.

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

## Receiver verification authority

Response validation is bound to the original request, with a separate ISR
admission check. The Raft-authoritative HWM, push replication and possible
same-epoch content divergence require local content-generation evidence;
matching epoch/offset tuples alone cannot certify a repaired log.

Implemented receiver witnesses bind the original partition, pre-challenge disk
generation, verification attempt and reconciliation-marker revision. The actual
terminal apply returns opaque authority only after data and required ES indexing
succeed. Final admission rechecks that authority after snapshot transfer, and
heartbeat/direct proof builders share the same current-generation eligibility.
Legacy error-only apply APIs cannot manufacture admission authority. Protocol
zero still proves only its existing last-record challenge, not full-prefix
equivalence. Checked generation validation adds constant local work, no extra
network round or retained-history scan; existing challenge/hash costs remain.
ES recovery uses a fixed committed view that bypasses only its unchanged owned
reconciliation marker while retaining transaction visibility. Unknown or changed
index content generations require a reset/replay to remove divergent entries;
repeated verification and append catch-up within one certified generation reuse
index progress. This derived-index rebuild cost is separate from constant-size
witness validation.

A consumer log repair records every affected partition's latest required
generation. Clean mutation rejection does not discharge it. Strict full replay
requires actual verified repair evidence for the whole pending set; one atomic,
nonwaiting multi-disk generation check publishes replay and readiness. A busy,
rewritten or unavailable disk keeps admission closed for retry. Scoped bootstrap
rebuilds remain possible before prefix verification and cannot clear full-repair
requirements. Regression checks cover earlier-prefix rewrites with identical
tails, snapshot-transfer races, marker replacement, repeated and multi-partition
repairs, and a raw disk rewrite immediately before replay publication.
The shared proof builder withholds offsets-partition ISR admission until pending
full replay finishes, keeping recovery verification available
if concurrent non-ISR replication advances the boundary before replay.

## Speculative tail and committed-watermark repair

Both tail-repair entry points serialize truncation with current recovery-state
validation through the checked physical rollback engine. They retain separate
publication policies:
speculative repair preserves the old HWM and its authority; committed reconciliation
publishes the Raft watermark only after index rebuild and durable checkpoints.
Canonical divergence rollback may still lower a stale local HWM.

The shared repair engine releases the partition mutex before disk maintenance or
raw scans. Compaction can sample LEO equal to the cluster HWM, then wait for the
partition HWM after a normal append creates a speculative tail; repair waiting
for maintenance while holding the partition mutex deadlocks that valid interleaving.
Reconciliation, visibility and checkpoint exclusions keep mutation serialized
without this lock inversion. A changed reconciliation marker must survive repair.
Uncertain mutation or rebuild failure keeps the partition unavailable; speculative
repair must not create an authoritative checkpoint from an unknown watermark.

Regression checks defined before implementation cover that exact sampled-LEO interleaving
for both entry points, watermark policy and bounds, raw producer/transaction
rebuild, marker replacement, checkpoint failure, no-op and snapshot/bootstrap
guards. This changes liveness and publication safety, not recovery asymptotics:
destructive repair still reads all retained record bytes and rebuilds all retained
producer/transaction state; no-op tail repair needs no scan. Byte/time budgets and
incremental recovery remain separate proposals.

## Outstanding audit follow-ups

- Lineage now rebuilds through resumable physical chunks bounded by 1 MiB,
  1,024 records and 64 segment EOF transitions, with one MaxMessageSize-bounded
  first-record allowance. Hashing and sidecar I/O occur outside storage locks;
  an immutable scalar publication is separate from mutable builder progress.
  A 128-entry endpoint cache and byte-aware checkpoints bound repeated historical
  work. Cold restart still scans the retained bytes once and never trusts the old
  sidecar; durable restart reuse remains a separate proposal. Budgets cannot
  preempt a filesystem operation already in progress.
- Full-prefix compacted proofs and bootstrap reconstruction retain full scans.
  Ordinary retained pages reuse process-local validated physical positions as
  described below; that does not remove the work of those separate proofs.
- Consumer metadata still has a transitional global strict replay gate. Validate
  availability under busy unrelated traffic and prepare partition-scoped recovery.
  This layer does not change that architecture or claim to fix its availability cost.

## Bounded lineage reconstruction contract

Derived-index work proceeds incrementally outside the acknowledgement path,
preserving canonical same-epoch content proofs and rebuilding cold from the log.
Neither checkpoint size/mtime nor the process-local content generation establishes
restart authority. Durable restart reuse remains a separate proposal; the existing
sidecar is preserved. A private paged metadata export is implemented in disk
storage, as documented in `pkg/disk/BOOTSTRAP.md`. The metadata source registry
and authenticated transport are implemented; provider and receiver integration
remain separate work.

The regression contract is bounded physical reads (bytes including headers,
records, and empty-segment transitions), completed-chunk resume after cancellation,
zero historical work for cached endpoints, suffix-only append work, canonical
hash equivalence, byte-aware sparse checkpoints, marked-hole validation, corrupt
EOF rejection, append/rotation cursor continuation and destructive-generation
invalidation. A fixed prefix may stop at its certified boundary; a durable tail
requires clean EOF. Tests must also cover mutation during publication, one builder
under concurrent requests, slow sidecar sync without delayed data acknowledgement,
shutdown without late publication, and cold restart ignoring old sidecar progress.
Measure physical bytes and journal work, not just request counts. Budgets are
cooperative: a filesystem read or fsync already in progress cannot be preempted;
one first oversized frame is explicitly limited by MaxMessageSize.

Public lineage requests continue to prepare the entire captured durable tail
before answering an earlier endpoint. They therefore still fail closed on later
corruption in that captured tail. The private cursor's fixed-prefix stop rule is
for bounded internal endpoint work and future reuse; it does not change that
public API contract or make arbitrary public historical proofs ignore corruption.

## Retained-page physical progress

Ordinary retained pages reuse a bounded cache of 128 validated raw cursor
checkpoints. Each checkpoint contains the local content generation, retained
anchor, exact segment, byte position and certified logical predecessor boundary;
it contains no record payload and is never persisted or transmitted. Cold seeks
start at the intersecting segment base and validate every skipped frame, ignoring
the sparse index. Append and normal rotation preserve progress; destructive
generation changes, handler replacement and restart require cold validation.
The API does not detect out-of-band rewriting of bytes already certified in the
same process generation.

A context-aware page gate owns the cache. It is never nested with the lineage
builder gate. Storage locks cover durability capture and individual raw chunks,
bounded by 1 MiB, 256 records and 64 EOF transitions; one first oversized record
may exceed the byte budget by at most MaxMessageSize plus its frame header.
Decode, identity/order/gap validation and page assembly occur after releasing
storage locks. The capture budget shrinks with the remaining page limits so a
one-record previous-record proof does not read a large suffix. Read counters
include actual header, payload and read-ahead bytes, including failed requests.
A filesystem read or fsync already in progress remains non-preemptible.

Checkpoint publication and successful page return recheck availability, generation,
retained anchor and segment membership. Completed skipped-prefix chunks survive
cancellation; canceled calls return no partial page. Selection never skips past
the next request's start. The cache keeps the position before the last included
record and before the first omitted record, allowing sequential source catch-up
to interleave previous-record digests with suffix pages without rescanning history.
The first omitted frame still requires identity/order/gap validation. Marked
leading, interior and trailing holes retain segment-local authority; unmarked
holes fail. Fixed prefixes can stop at their certified boundary, while a captured
durable tail needs clean EOF including partial-header rejection.

Regression checks cover actual source catch-up with previous-record digests,
record and byte limits, physical bytes and decoded frames, cold corruption before
arbitrary starts and forged indexes, bounded metadata eviction, concurrent seeks,
marked empty-segment transitions, queued and in-flight cancellation with exact
retry, mutation before publication, append/rotation, destructive ABA and cold
reopen. Sequential pages have source-byte work plus bounded boundary rereads.
Arbitrary seeks or enough interleaved followers can evict useful checkpoints and
repeat cold segment work; no unconditional linear-work guarantee is made for
those workloads. Full retained scans, bootstrap and ordinary readers keep their
existing contracts.

## Metadata bootstrap transport

The discovery endpoint binds the process-owned metadata source before consumer
metadata recovery waits. `BOOTSTRAP_BEGIN`, `BOOTSTRAP_CHUNK`, and
`BOOTSTRAP_RELEASE` require cluster authentication and strict version-1 DTOs.
Only a valid authenticated Begin extends the server request deadline to 120
seconds; handshake and ordinary request deadlines remain unchanged. Client
cancellation closes its private connection. It never releases a shared attempt:
the attempt owner explicitly releases after consumption or abandonment, while
source creation deadlines and ready-export expiry bound uncertain delivery.

Discovery shutdown cancels requests, closes listeners and active sockets, then
joins handlers before closing the source registry and its storage. A filesystem
read already in progress remains joined even after its socket closes. Remote
cancellation, deadline, fencing, capacity, continuity and storage failures retain
distinct structured classifications. Production recovery capability remains zero;
the metadata provider, receiver integration and general replica bootstrap bundle
are not active.
