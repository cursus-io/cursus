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
