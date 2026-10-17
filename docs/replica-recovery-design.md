# Replica recovery with durable lineage and bootstrap

Implementation status: indexed and retained-prefix proofs, fenced bootstrap
transport and installation, detached consumer metadata recovery, and runtime
capability negotiation are implemented. A process advertises recovery protocol
**2** only after its source, consumer cache, replica receiver and internal listener
are ready. Startup registration and JOIN wait for that bundle.

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
retained-range bootstrap with derived-state recovery. An unavailable source,
stale fence, or incomplete installation keeps recovery and admission pending.

Regression checks derived before implementation cover every small-span rollback
boundary, nonzero anchors, a near-million-record tail with one bad record,
MaxUint64 arithmetic, real-disk and source/receiver FSM recovery with single-tail,
early reconvergent and long divergence, authoritative-HWM caps, stale fences and
incomparable anchors. Direct and forwarded registration must advertise the same
identity, incarnation, transaction shard count, lifecycle 6 and installed recovery
capability. Restored registration cannot stand in for a new process registration.
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
existing lifecycle protocol requirement. Protocol 1 remains supported. Installed
production peers advertise protocol 2; a peer advertising 0 selects the existing
digest protocol. There is no public activation flag or setter. Unknown mandatory
features fail closed. The source also checks its own current process registration
before serving a negotiated request.

New voters must register their current complete capability before membership
changes. The captured Raft configuration index fences that change. An existing
voter may reconnect without changing membership; failed admission never removes
an existing voter as rollback. A failed membership change can leave a retryable
registered nonvoter.

Consumer route enforcement activates through a durable Raft command only when
all registered brokers, including inactive brokers, and all current voters have
compatible process registrations. Snapshots preserve this decision. Once enabled,
legacy or incomplete registrations are rejected, so downgrading a broker requires
an explicitly supported migration rather than silently disabling the fence.

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

When the requested prefix predates retention or cannot be proven, an active ISR
source supplies a fenced committed retained range. `BootstrapRequired` selects
this path without speculative truncation or a legacy downgrade. The receiver stages a bounded
transfer, validates checksums and offsets, and installs through a durable pending
manifest. Restart completes or safely rejects an interrupted installation.
Transaction, producer, consumer-offset and application snapshot state must remain
consistent with the installed range. Only a configured recovering replica outside
ISR can replace its local log. A stale fence is rejected before any mutation.

Ordinary topics transfer an empty application catalog and rebuild producer and
transaction state from the raw retained records. Event-sourcing topics additionally
require a certified snapshot catalog when retained history no longer starts at
zero. Both source capture and receiver installation select this behavior from
the authoritative topic definition, never a request-supplied flag.

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
- Consumer metadata retains a global atomic replay gate. After durable route
  activation, nonowned partitions use captured finite ends while owned partitions
  must include their latest committed local end. This removes moving remote-end
  starvation; it does not implement independent per-partition admission gates.

## Bounded lineage reconstruction contract

Derived-index work proceeds incrementally outside the acknowledgement path,
preserving canonical same-epoch content proofs and rebuilding cold from the log.
Neither checkpoint size/mtime nor the process-local content generation establishes
restart authority. Durable restart reuse remains a separate proposal; the existing
sidecar is preserved. A private paged metadata export is implemented in disk
storage, as documented in `pkg/disk/BOOTSTRAP.md`. The metadata source registry
and authenticated transport are implemented, together with the consumer metadata
recovery provider. The replica receiver installs through the same fenced recovery runtime.

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
distinct structured classifications.

The process command handler owns the consumer metadata recovery cache under
`@recovery/consumer-metadata`, outside valid topic directories. Staging requires
current active source and requester incarnations advertising capability 2. It
downloads raw records and transaction markers before consumer recovery gates;
fixed local leases evaluate the receiver's current transaction decisions on every
read. Fresh source certificates allow same-end reuse or suffix-only transfer and
history construction; a certificate miss requires a full replacement. Old leases
retain their fixed ends through appends and replacements.

Two process-wide inbound permits cover transfer and local publication work,
independently of six source captures. The adopted-store limit is the effective
partition limit plus two, including retired, pinned and undeleted generations;
shared disk and metadata-history budgets remain charged until ownership ends.
Shutdown joins staging and readers, and reports physical cleanup debt without
spinning. Production recovery capability remains zero until the general replica
receiver, resumable installation and remaining recovery bundle are complete.

## Automatic retained recovery and bounded log turns

For mutually capable peers outside ISR, request preparation selects a proof
before reading a previous record or trimming a speculative tail. Ordinary
contiguous logs retain indexed recovery. Current compacted policies and durable
marked history use retained-prefix recovery, including after a policy changes to
delete. Historical fallback requires validated marked authority; corruption,
unsupported storage accessors and I/O failures do not authorize bootstrap.
Retained mode preserves the original local tail and witness. A challenge beyond
the source's committed cut requests full replacement without speculative trim.

Candidate discovery reads only metadata. Each partition builds its proof during
its own turn, with cancellable lock waits and storage scans. Publication rechecks
the original partition, complete definition, membership, committed boundary and
both broker process identities. Expensive proof work runs outside global FSM
gates. Ordinary speculative truncation still requires its checked mutation path;
filesystem calls already in progress remain synchronous.

Each log turn allows at most 64 fetch iterations and a cooperative 30-second
deadline, including initial proof construction. Durable progress survives yielding
and later partitions receive their own turns. The next pass resumes from the
durable tail. A final apply is recorded before yielding; a proof is not regenerated
when no next fetch iteration is available. The catch-up loop has one process owner
and shutdown cancels and joins it before closing its dependencies.

Cold proof construction and validation can scan retained bytes; warm proofs reuse
their bounded caches. This scheduling limit does not imply bounded total disk
work or a hard filesystem deadline.

Proof preparation permits the committed boundary to advance during non-mutating
work while retaining the original request. Truncation and local watermark changes
still require the exact captured boundary. Recovery lock waits use a cancellable
queue: later readers cannot continually bypass a waiting witness writer, and
cancellation leaves no detached goroutine holding a partition or metadata gate.

Production snapshot recovery shares the remaining turn budget with log fetches.
Each snapshot step transfers at most one 256-record catalog page; the next turn
resumes its revision and last successfully published key using the original
terminal receiver witness. Request rebuilding cannot invalidate that witness
between pages. Cancellation preserves completed progress; changed process,
membership, lifecycle, catalog revision or storage authority discards the attempt.
Only completed catalog recovery can publish verification and submit an ISR proof.
Snapshot certification may still replay retained bytes within a step.

The server owns heartbeat, registration, topology and recovery workers and joins
them before closing their dependencies. Registration continues until the local
FSM reflects the current process identity and advertised configuration; an older
successful request cannot suppress a later update. Handler shutdown also joins
consumer recovery and transaction monitors, including synchronous initialization.

The installed runtime bundle advertises recovery capability 2. Metadata routing
activation is a separate durable decision requiring compatible registrations
from every registered broker and every current voter. Mixed-version clusters
retain the compatible digest path until both recovery peers support the new mode.

Consumer metadata writers capture the group partition, partition count, original
owner, leader epoch and topic lifecycle from one metadata view. Forwarding keeps
that route unchanged. Append preparation, replica delivery and the durable commit
validate it, so a delayed request cannot acquire a newer owner's authority. Local
group growth no longer changes the distributed offsets partition count. Legacy
unprotected writes remain accepted pending durable cluster-wide routing activation;
finite remote recovery cuts are not enabled by this route validation alone.

## Consumer ownership and transaction records

Ordinary metadata writes carry the original group owner, partition epoch,
partition count and lifecycle through forwarding, replication and commit. The
record payload must identify the same group. After durable activation, an
unrouted raw offsets write cannot bypass that authority. Transaction records
instead carry the original transaction coordinator, producer epoch and exact
transaction revision. Their offset snapshot or decision marker must match the
durable transaction and its touched metadata partition; a transaction ID alone
is insufficient authorization.

A finite consumer replay captures ownership before remote work. Publication
rechecks partition mapping, owner epochs and lifecycle. Nonowned remote commits
may advance beyond the captured end; owned partitions must supply their latest
committed end from durable local storage. A changed owner or mapping discards the
attempt. The captured ownership certificate is published with replay and is never
restamped from a later topology. A coordinator whose certificate is missing stays
pending while the recovery worker obtains a new one outside ordinary admission.

## Recovery work metrics

The existing `/metrics` exporter includes `cursus_recovery_work_total{stage,unit}`
and `cursus_recovery_attempts_total{stage,outcome}`. Labels are fixed enums; topic,
partition, broker, path, attempt identity and error text are excluded. Work counts
include partial writes, failed reads and retries. They describe logical file/API
work, not physical device I/O or network framing bytes. Counters reset on process
restart; compare deltas within each process lifetime.

Stages distinguish full and retained exports, retained page/scan reads, checksum
verification, chunk reads and staging writes, detached cache copies/replay, and
metadata/replica RPCs. Units distinguish read, write, hash and transferred payload
bytes, decoded records/bytes, sync calls, page attempts, cache hits/misses and
retries. Transfer bytes count returned chunk payloads before validation; a transport
failure that returns no payload contributes an attempt but no observable payload
bytes. Sync counts count attempted calls, not completed durability. A checksum
read belongs to `hash_bytes`; do not add it to a separately inferred file-read
estimate. Decode bytes and read bytes overlap and must not be summed as disk I/O.

The `lineage` stage records retained-frame and sidecar-header reads, journal
writes, attempted source/journal syncs and endpoint-cache hits/misses.
`canonical_hash_bytes` counts bytes fed to cumulative hashes, including the
previous digest and encoded length. It is computation input, not disk I/O.
Canceled rebuilds retain their work counters. A background call that requires
an explicit foreground rebuild is counted as a failed preparation call; this
does not by itself indicate corrupt authoritative records.

For example, `sum by (stage, unit) (increase(cursus_recovery_work_total[5m]))`
shows work during a recovery interval. Compare `metadata_rpc` transfer bytes and
`retained_page` reads between cold, suffix-only and unchanged-end recovery. A
low request count alone is insufficient: cold seeks, cache eviction, checksum
verification and full derived-state rebuilds may still read retained history.
`sum by (stage, outcome) (increase(cursus_recovery_attempts_total[5m]))` separates
successful calls, failures and cancellations without exposing error identities.

`catchup_rpc` counts incremental response JSON bytes before decoding, including
rejected payloads; `replica_rpc` counts bootstrap chunk payloads. These units
exclude framing and are distinct from decoded application payload size.
`bootstrap_install` counts completed installation/resume attempts.

Run with `--log-level=debug` to inspect `replica_recovery` turn events: protocol,
source, leader/lifecycle epochs, start/current/target offsets, remaining lag,
exchange count, elapsed milliseconds and quoted failure reason. Phases distinguish
incremental work, repaired divergence, bootstrap selection, snapshot replay,
prefix verification and proof submission. Proof submission is not an assertion
that the leader already admitted ISR. Bootstrap installation start and completion
are logged at info level, including local resume and failures. A stale-fence or
connection error requires a new attempt; checksum/catalog/safety failures keep
readiness closed and require investigation rather than an operator offset reset.

Validate convergence with full ISR held for at least twice the ISR heartbeat
expiry interval while committing consumer offsets. Preserve separate evidence
for mixed binaries, retention bootstrap, interrupted installation, stale fences,
transaction decisions and unrelated partitions; one healthy restart test does
not establish all of those conditions.

The retained recovery acceptance suite runs with Docker and
`RUN_E2E_RETAINED_BOOTSTRAP=1 go test -v -count=1 -timeout 30m ./test/e2e-cluster -run 'Test(RetainedBootstrap|IndexedRecoveryBoundsLargeLegacyTailOnThreeNodes)'`.
It verifies retained bootstrap, missing/damaged lineage rebuild and a later
restart, a 10,000-record divergent legacy tail, delayed old-leader responses,
and process exits after durable truncation, bootstrap replacement and incremental
append. Each recovery requires stable full ISR and compares committed records;
the bootstrap cases also retain transaction decisions and unrelated group offsets.
Process-exit hooks and the offline tail fixture exist only in the dedicated
`e2e_faults` image. The fixture changes only a stopped test broker's uncommitted
tail, and is not an operator recovery command.
