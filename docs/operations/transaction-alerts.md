# Transaction coordinator alerts

The broker exports transaction coordinator state from the same state used to
serve requests. The Helm charts install baseline rules when
`monitoring.enabled=true`:

- `cursus_transaction_recovery_ready != 1` means a broker has not recovered
  transaction state and must not serve transactional work.
- `cursus_transaction_oldest_active_seconds` above the workload's maximum
  transaction duration detects stuck open or committing transactions.
- Sustained growth in `cursus_transactions{state="committing"}` indicates that
  partition markers, consumer-offset reservations, or final decisions are not
  completing.
- Unexpected growth in `cursus_transactions_expired` should be correlated with
  journal size, disk headroom, and storage errors.

Alert thresholds are workload-specific. Compare age and counts with broker
readiness, Raft leadership, replication safety, request admission, and consumer
metadata recovery before taking action. Metrics and diagnostic commands are
read-only and never create groups, transactions, topics, or offsets.

Reinitializing an open transactional producer first persists an abort decision
and writes abort markers for the old epoch. The broker issues the new epoch only
after that work succeeds. A failed initialization is retryable; do not delete
its topic, transaction journal, or consumer-offset data to bypass recovery.
Reinitializing a committed transaction also waits for its offsets to be
materialized.

Transactional input offsets use durable version-5 reservation snapshots. The
reservation binds the transaction and producer epoch to its input partitions,
survives membership changes and restart, and fences stable offset reads,
ordinary offset commits, rebalance completion, and group deletion until a
durable commit or abort releases it. Resolution persists monotonic offsets
before releasing the fence. Ambiguous append errors retain the fence and are
safe to retry with the same transaction identity.

Distributed consumer-metadata recovery scans replication-committed control
records beyond unrelated open transactions while filtering unresolved records.
It never reads past the flushed replication high watermark. An unreadable
reservation record fails recovery instead of silently dropping the fence.

The standalone transaction journal uses checksummed version-3 records. It
writes collection and map deltas for a growing transaction and compacts delta
chains into full checkpoints once reclaimable record or byte debt crosses the
configured internal threshold. Versions 1 and 2 remain readable. Preserve the
journal and its checksummed cut manifest together with partition and offset data
during backup and restore.

For incident response, follow the transaction section of the
[production incident runbook](production-runbook.md). Capture the first error,
journal validation, coordinator owner, state counts, and oldest active age
before restarting a broker.
