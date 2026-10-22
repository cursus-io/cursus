# Durable local cleanup-cache fence

Cleanup progress cannot persist the existing `ContentGeneration`: its handler
pointer and revision are process-local. The additional `.cleanup-generation`
file is a random, local content-identity token, not a replication proof.

An unchanged partition retains its token across restart and append. Before a
possible prefix rewrite, the existing content-generation mutation gate writes
a new 32-byte token in place and fsyncs it. A failed barrier prevents mutation
and fences the writer. Bootstrap/range recovery and torn active-tail repair
also invalidate before replacement. An invalidation followed by a crash before
mutation only loses cache reuse; a crash after mutation cannot reuse the older
token. Losing a newly created token file also only loses cache reuse.

Callers pin partition replacement/rollback, capture the token, and call
`SyncCleanupCheckpointContext` before publishing any checkpoint that skips
log bytes. The token alone does not establish durability of newly appended
bytes. No checkpoint cache is authoritative and no on-disk event format changes.
Older binaries do not maintain this fence; derived cleanup checkpoints must be
removed before a downgrade. Ordinary log data and offsets must be retained.

Kafka's [versioned OffsetMetadataManager](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java)
maintains current-state maps, while its [LogCleaner](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaner.java)
rewrites obsolete historical entries in the background. Cursus needs a local
prefix-identity fence for resumable deletion-proof scanning because its current
group cache is not sufficient deletion authority. This does not implement
Kafka's state index or compaction algorithm or claim equivalent performance.

Tests cover append/reopen stability, rollback invalidation and an invalidation
write failure preserving the original prefix. Existing storage recovery,
bootstrap, compaction and rollback tests run with race detection. These tests
check software ordering; they do not certify physical-device power-loss behavior.
