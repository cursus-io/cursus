# Aggregate replay identity lookup

Aggregate replay still treats the retained, stable committed log as its identity
authority. Duplicate delivery of the same aggregate version, EventID and payload
digest returns the existing offset; conflicting identity is rejected. Replication
acknowledgements, commit fencing, retained data and storage formats are unchanged.

Previously every new APPEND_STREAM scanned the whole retained partition to prove
EventID absence. A 100,000-event partition therefore repeatedly decoded its history
even when consumers were caught up. The partition now lazily derives an exact
EventID set from that log, then scans only the newly stable suffix before lookup.
The initial scan uses 1024-record visible pages to match the underlying reader.
The index stores complete IDs, not collision-prone shortened hashes.

The cache is disposable and never grants durability on its own:

- Restart or partition recreation reconstructs it from the existing committed log.
- Retention-floor changes rebuild it so expired identities have the same semantics
  as the previous retained-log scan. Memory is proportional to retained unique IDs.
- Tail truncation/replica rollback and snapshot visibility transitions invalidate
  its generation, including rollback followed by refill to the same tail offset.
- Read/recovery errors invalidate the cache and fail closed; a partial scan cannot
  prove absence. Uncommitted/aborted messages are not identity evidence.
- Aggregate check-and-append is serialized per stream index (partition), including
  standalone writers targeting different keys. Distributed callers retain their
  existing mutation fence through replication and commit acknowledgement.

No new database, migration, sidecar, offset reset or retention change is needed.
Rollback to the previous binary can read the same log. The first lookup after
restart has a rebuild cost; warm lookups and catch-up work do not rescan the prefix.

Validation includes cross-key concurrent duplicate attempts, same-version retries,
payload/identity conflict behavior, incremental read boundaries, retention, rollback
and refill, read failures, transaction visibility and real disk close/reopen.
The warm-lookup microbenchmark is not an end-to-end throughput guarantee: existing
fsync, replication, network, and publisher serialization costs remain.

Production rollout is separate from code review: preserve backups, validate recovery
on copies, retain unresolved IOCP spool entries, and keep Temporal paused throughout
the maintenance window. Never remove deduplication or bypass acknowledgements to
accelerate drain.
