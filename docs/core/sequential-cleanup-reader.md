# Sequential reads for consumer cleanup

`types.BoundedReplayReader` is a local storage primitive for deriving cleanup
proofs from a committed log prefix. It does not decide group deletion.

The caller supplies a fixed exclusive committed end and retains `ReplayCursor`
between calls. A call reads at most one physical segment, the requested record
count, and the requested serialized record-byte budget. Record length headers
and file metadata are additional work. The returned counts include decoded
records skipped before the requested offset or inspected beyond a compacted
gap. The caller must also bound the number of page calls, including empty pages.

The reader bypasses sparse indexes. It starts at the retained segment and then
resumes at a byte position, so a missing index cannot silently turn every retry
into another prefix scan. Compacted gaps advance the physical cursor without
inventing records. Inspecting an offset beyond the committed end does not
consume its cursor position: a later commit must still be readable.

The caller must pin partition identity and exclude rollback/replacement while
reading or applying proof. Discard cursors when identity, mutation generation,
or retained start changes. Concurrent compaction must be excluded as well;
distributed internal consumer metadata currently disables compaction. Cursors
are derived memory state, never an authoritative checkpoint.

Corrupt checksums, truncated records and records above the storage maximum fail closed.
The reader never treats a partially written record as an empty compacted gap.
A first record above the requested page budget returns `ReplayByteBudget`
without allocating its payload. Cleanup now reserves a process-wide
read/validation slot and retries that record alone with its exact stored size;
see [the progress and resource contract](cleanup-record-progress.md). The
primitive's hard caller-supplied bound is unchanged. `ReadBytes` reports logical
file-read bytes including headers and failed probes, while `Bytes` remains
the sum of complete serialized payloads presented to the decoder. Neither
counter claims physical device I/O.

The design comparison uses Kafka 3.9.0's
[GroupMetadataManager loading path](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala):
adopt resumable replay and explicit loading work, but use Cursus's committed
prefix and physical cursor. Kafka can grow its buffer for `minOneMessage`;
this primitive keeps a hard byte limit, with explicit cleanup reservation
providing first-record progress at its caller. Kafka's normal coordinator load and this
narrow Cursus cleanup primitive have different responsibilities.

Regression checks cover multiple small pages with a missing sparse index,
record/byte totals matching the physical log, oversized and truncated records,
and an uncommitted record beyond a compacted gap becoming committed later.
