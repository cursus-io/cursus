# Durable broker-time retention

## Contract and deletion unit

`LOG_RETENTION_TIME_ENABLED=true` (configuration `log_retention_time_enabled`,
flag `--retention-time-enabled`) requests the durable contract. It defaults to
false. A newly accepted record receives `AppendTimeMS`, the accepting broker's
wall-clock Unix time in milliseconds. Client values are discarded. This field
is distinct from application event time (`Timestamp`), filesystem mtime and the
receiving follower's clock. The optional CDM6 disk envelope protects it with the
record CRC32C; records without the field retain their CDM5 encoding.

Replication JSON, incremental replay, retained bootstrap, detached metadata,
canonical lineage serialization, raw compaction and tail repair preserve the
value. Public v3 client framing does not acquire a new producer timestamp mode.
The initial layer's codec support alone does not enable assignment or deletion.

Age deletion is strictly `now > max(AppendTimeMS) + retention duration` for a
closed segment containing only known times. The active segment is retained.
The check uses millisecond resolution and avoids duration overflow. Deletion
reclaims a contiguous prefix of whole segments. A bootstrap that combines
old and young records delays reclamation until the youngest expires; it never
makes an older record acquire the bootstrap's creation time. Compaction retains
the original mtime as before and copies the immutable time in surviving records.

An enabled contract holds a segment containing any unknown legacy time. It
does not invent an old date, use bootstrap time, or reset age on restart. This
can retain legacy segments indefinitely under age policy. Migrate those data
through an explicitly chosen data-retention policy; there is no automatic
legacy backfill. A disabled broker with only legacy records retains the prior
mtime predicate. A mixed segment is never assigned the legacy mtime fallback.
Known timed records keep their contract even if read by a compatible reader
whose local request flag is false.

Byte retention remains an independent policy: it can delete records younger
than the age duration. Both policies require a committed, flushed prefix below
the first unresolved transaction and reject incomplete snapshot/bootstrap or
rollback state. Neither policy may use corrupt record evidence. Event history
records (event type, event ID or aggregate version) conservatively hold the
segment for both policies: this change does not supply a snapshot-aware proof
that an event history is disposable. This may increase disk use; pressure is
not permission to remove recovery history. Internal metadata topics retain
their existing separate policy.

## Upgrade, activation and downgrade

1. Back up the complete consistent broker state before upgrading. Include log
   roots, topic metadata manifest, Raft state and format marker, checkpoints,
   transaction/event snapshots and bootstrap publication metadata. Copying only
   data files is not a supported cluster recovery procedure.
2. Install a reader supporting CDM2 through CDM6. With the flag false, new user
   appends remain legacy until cluster activation. The existing membership JOIN
   contract requires an exact current lifecycle protocol; it is not relaxed
   by this change. Mixed-release rolling JOIN/rejoin from protocol 7 or 8 to 9
   is therefore **not supported**. Use the existing coordinated broker upgrade
   procedure; do not claim zero-downtime rolling upgrade from protocol 7 or 8.
3. Request activation on a controller-capable broker. The controller checks all
   registered processes, including inactive registrations, and all current Raft
   voters for lifecycle protocol 9, retained-bootstrap recovery capability and
   a valid incarnation. The FSM repeats the registry checks before committing
   activation. Old registrations are refused afterward. Until activation, a
   broker explicitly requesting the feature rejects new leader appends rather
   than silently writing a different time contract.
4. Activation is durable Raft state, included in snapshots. All compatible
   brokers honor it regardless of their request flag. Standalone mode writes
   `retention_time: true` to the topic manifest before opening restored topics;
   the flag must remain true on restart. The previous strict manifest reader
   rejects this field.

Successful distributed recovery upgrades the local `.cursus-raft-format` to
12, even before optional activation. Previous binaries reject that marker.
Do not edit the marker or remove the manifest field to force a downgrade.
Downgrade requires restoring a consistent pre-upgrade backup with a compatible
release; this can lose post-backup writes. The Raft snapshot envelope remains
version 11, with the activation field covered by its strict schema. These
fences also protect empty logs from accidentally forgetting activation.

## Clocks and availability

The clock is a broker authority, not a timestamp authenticity oracle. Hosts must
have trusted, bounded-error wall clocks before startup. Within a process,
wall-clock divergence of more than 60 seconds from monotonic elapsed time
blocks new timed appends and age deletion. A backward or forward clock step
therefore fails closed until the clock is corrected. Byte policy remains
independent and still observes commit/transaction/event protections.

After restart, follower writes, rollback or bootstrap, the next leader append
seeds from the durable predecessor. Backward skew up to 60 seconds clamps new
time to that predecessor. A predecessor more than 60 seconds ahead rejects
new appends. A follower never restamps a replicated record. This preserves
ordering through a leadership change while trading availability for evidence.

No process-local monotonic clock can detect an incorrect initial wall clock or
a restart that establishes a new incorrect anchor. Timestamp meaning and age
accuracy are consequently conditional on the operator's clock-error bound;
this is not a guarantee against an arbitrary fast host clock. The 60-second
step guard is not a distributed clock-synchronization protocol.

Future record times are held for age deletion and counted as `future_time`.
CRC corruption is held as `corrupt_data`. Arbitrarily far-future but valid
imported evidence is not rewritten to a guessed date: it can hold age deletion
indefinitely, and may block promotion appends. Correct the source of clock
error and restore trustworthy evidence. Independent byte policy can reclaim
ordinary committed records; it cannot override event/transaction protection.
These boundaries are deliberate conservative fallbacks, not silent TTL caps.

## Derived state, recovery and observation

There is no durable time index. A disposable per-segment summary stores maximum
time, unknown-time presence, event protection and validation result. Its first
use reads each frame with CRC validation. File identity, size, mtime and the
existing content-generation invalidations detect replacement. Ordinary appends
do not invalidate closed summaries; successful prefix removal retains summaries
for untouched files. No full-log scan is introduced on every tick. A cold
summary scan can delay requests on that partition; its cost is proportional to
the segment being evaluated. The existing offset index is not time authority.

Restart, compaction, truncate or bootstrap publication can require rebuilding
summaries. Missing state costs reads and decoding, zero summary writes and zero
summary temporary disk bytes. Corrupt source frames block deletion until repaired.
Publication/replacement keeps the existing durable generation and cleanup
checkpoint invalidation primitives; it does not replace the metadata compaction
or consumer cleanup algorithms or require their unmerged branches.

Observe `cursus_recovery_work_total{stage="retention_time"}` with units
`read_bytes`, `decoded_frames`, `cache_hits`, `cache_misses`, and
`cursus_recovery_attempts_total{stage="retention_time"}`. These are logical
filesystem API bytes, not physical device I/O. `cursus_retention_holds_total`
has bounded reasons `clock_step`, `unknown_time`, `future_time`, `corrupt_data`,
`event_history`, `committed_boundary`, `partition_unavailable`. Alert on sustained
holds together with storage growth and broker clock health. A hold counter is
not a count of unique records. There are no topic/partition/path metric labels.

## Reproducible verification

`TestRetentionRecoveryBaselineMtimeDrift` retains the audited baseline predicate
and demonstrates new mtimes on actual isolated incremental and bootstrap files.
It does not modify mtimes. `TestDurableRetentionRecoveryGranularity` compares
the same record times at controlled cutoffs with different segment boundaries.
Compaction, interrupted bootstrap replacement, restart, cold backup restore,
missing/corrupt offset indexes, corrupt record CRC, legacy mixtures, HWM, open
transactions, event histories, capability activation and snapshot restore have
dedicated regression tests. Clock fixtures control policy time only; benchmark
latency uses the real monotonic clock.

Run the changed packages and race tests in an isolated Linux container with no
external network or published ports. Benchmarks `BenchmarkRetentionTimeSummary`
and `BenchmarkRetentionTimeAppend` exclude startup, fixture creation and teardown
from their timers. `TestRetentionTimeRecoveryWork` reports retained log size,
metadata overhead and subsequent bootstrap read/write/hash/transfer work. Keep
physical I/O, total process CPU/RSS and benchmark allocations distinct.
