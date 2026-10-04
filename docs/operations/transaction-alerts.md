# Transaction coordinator alerts

The broker exports transaction coordinator state from the same in-memory state
used to serve requests. Recommended production alerts are:

- `cursus_transaction_recovery_ready != 1` for any ready broker. The broker
  should normally fail startup before this can occur.
- `cursus_transaction_oldest_active_seconds` above the application's maximum
  transaction duration. This detects stuck open or committing transactions.
- A sustained increase in `cursus_transactions{state="committing"}`. A brief
  non-zero value is normal while a commit is being applied.
- Unexpected growth in `cursus_transactions_expired`, paired with journal size
  and filesystem alerts. The standalone journal rewrites atomically after 256
  records or 16 MiB and retains the latest state per transactional ID.

Alert thresholds are workload-specific. Compare age and counts with broker
readiness, storage errors, Raft leadership, and consumer metadata recovery
metrics before taking recovery action. Metrics and diagnostic commands are
read-only and never create groups, transactions, topics, or offsets.

Reinitializing an open transactional producer first persists an abort decision
and writes abort markers for the old epoch. The broker issues the new epoch
only after that work succeeds. A failed initialization can be retried; do not
delete its topic or transaction journal to bypass a recovery error. Reinitializing
a committed transaction also waits for its consumer offsets to be materialized.
In a cluster, the transaction coordinator waits for the replicated final
decision to be applied locally before completing the request, even when it is
not the Raft leader. A local apply timeout leaves the durable decision intact
for retry and recovery.

Recovery and timeout sweeps report individual transaction failures while
continuing the remaining batch. A storage or coordinator failure for one
transaction must not suppress timeout resolution for unrelated transactions.
Prepared commit recovery after consumer membership changes remains a separate
operational risk until its fencing and offset-visibility contracts are resolved.

The coordinator persistence layer supports version 5 offset-reservation
snapshots. These bind a transaction and producer epoch to its input partitions
independently of later membership changes. A pending reservation fences stable
offset reads, ordinary offset commits, and group deletion; commit resolution
persists monotonic offsets before releasing the fence. Ambiguous append errors
retain the fence and retries use a new snapshot revision. Older brokers cannot
read version 5 records, so a data directory containing them must not be opened
with an older binary.

Reservation snapshots also retain the latest terminal decision for each
transactional ID and producer epoch. A delayed prepare cannot resurrect a
released fence, including when abort reaches the coordinator before prepare.
Unknown commits and contradictory terminal decisions fail closed. These
watermarks survive restart and group snapshots; they are retained until group
deletion and do not expire on a timer. Capacity validation for workloads with
unbounded distinct transactional IDs remains part of the resource-bounds gate.

Distributed consumer-metadata recovery scans replication-committed control
records beyond unrelated open transactions. Application read-committed reads
still stop at the last stable offset. Recovery filters unresolved transactional
records and never reads past the flushed replication high-water mark; otherwise
a pending transaction could hide a later membership or reservation checkpoint.
An unreadable reservation record fails recovery instead of silently dropping
its input fence.

This storage layer alone does not resolve the prepared-commit risk above.
Transaction commands do not yet create these reservations. Controller integration
must validate all reservation RPCs, cover abort and timeout cleanup, and gate
activation on cluster compatibility before claiming recovery across membership
changes.

`FETCH_OFFSET`, `CONSUME`, and `STREAM` use the group coordinator's stable offset
view. A pending reservation returns the retryable availability error
`unstable_offset_commit`; existing consume caches and running streams also check
this fence. Consume caches are scoped to the group, member, and generation so a
reused connection cannot skip another consumer's input. A failed coordinator read
never falls back to a partition leader's potentially stale local group state.

Internal resume lookups request `FETCH_OFFSET ... include_found=true` to
distinguish a committed zero from a group with no committed offset. The default
public response remains `OK offset=N`. Old coordinators without the explicit
`found` field fail these internal lookups closed. Mixed-version consumption is
therefore not yet a supported upgrade path; reservation activation and rollout
compatibility remain a release gate.
