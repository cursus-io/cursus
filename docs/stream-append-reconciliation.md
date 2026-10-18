# Recovering an ambiguous stream append

An `APPEND_STREAM` follower or Raft reply can fail after the event is already
durable. The leader keeps that event at its original offset and fences new
writes. A subsequent producer retry now attempts to complete that same append
under the partition write lock instead of remaining permanently fenced.

## Safety contract

- A typed in-memory outcome retains the original event and leader/lifecycle
  fence. Recovery compares the full stored record with that event.
- A surviving single-record tail without an in-memory outcome must have the
  current leader epoch and the next committed aggregate version. Aggregate
  replay additionally verifies the payload digest and EventID uniqueness
  against the retained committed prefix.
- The original event is re-sent to every current ISR member at the original
  offset. Existing follower idempotency accepts identical bytes and rejects
  conflicts. No recovery path here truncates, rewrites, resets offsets, or
  bypasses EventID/producer deduplication.
- The leader/lifecycle fence and replica sets are checked before replication
  and again before commit. The existing epoch-fenced Raft commit must apply
  locally before the exact recovery marker can be cleared. The committed
  stream index is refreshed before validating the incoming retry.
- Storage recovery failures, changed fences/replica sets, snapshot recovery,
  transactions, multiple tail records, missing records, and identity conflicts
  remain fail-closed. Recovery does not quarantine or delete divergent data.

For event-sourcing topics, refusing unsupported retained tails deliberately
prevents falling through to the previous HWM reconciliation path, which would
truncate them. An old-epoch or multiple-record tail can therefore keep writes
blocked until explicit recovery. This availability trade-off is intentional:
automatic write resumption must not discard uncertain events. Repeated retries
are tested to preserve both the committed prefix and the entire retained tail.
Non-event-sourcing topics retain their existing preparation behavior.

Each producer retry makes one bounded attempt using existing internal request
timeouts. There is no new background worker, persistent database, wire command,
or disk-format migration. If no producer retries, this path is not invoked.

## Validation and operational limits

Regression tests cover a follower storing the event but losing its wire reply,
an uncertain Raft commit, concurrent retries, conflicting retry payloads, other
aggregates, changed epochs/replica sets, and invalid retained-tail identities.
A disk-close/reopen test checks the committed prefix and a surviving same-epoch
tail with its authoritative watermark restored. This is not a simulation of a
complete production Raft snapshot restore or a promise that every old-epoch
tail can be auto-committed after an election.

Deploying this code does not itself recover a running old binary's memory. For
an already blocked production broker, preserve backups and the uncommitted tail,
verify authoritative watermarks and replica contents, and separately authorize
any restart/rollout. Keep Temporal generation paused during that maintenance.
Never clear a marker manually or interpret consumer lag zero as recovery while
the producer HWM remains frozen. Confirm readiness on the affected broker,
advancing HWM and IOCP backlog reduction after recovery.

The EventID lookup optimization in PR #270 is independent of this correctness
fix; neither change alone proves the production drain has completed.
