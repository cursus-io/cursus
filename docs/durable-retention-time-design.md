# Durable retention time

## Evidence and design, before implementation

The official main baseline is `a49138e7922aa66b14cb4482a8b084e5b9c1b8cd`.
`retention.go` uses the closed log's mtime. `compaction.go` copies that mtime to
the replacement log; this path must not be described as resetting retention.
Incremental recovery decodes records and writes new local frames. Retained
bootstrap serializes canonical records into a private export and installs newly
created files. Neither path transports the source log mtime. Segment roll uses
local creation time, independently of record age.

Primary comparisons:

- [Accepted KIP-32](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/61318265/KIP-32+-+Add+timestamps+to+Kafka+message)
  proposes persisted record timestamps and distinguishes producer CreateTime
  from broker LogAppendTime. Followers preserve the leader's timestamp.
- [Accepted KIP-33](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/61318517/KIP-33+-+Add+a+time+based+log+index)
  proposes a derived sparse time index and segment maximum timestamps.
- [Kafka 4.1.0 UnifiedLog](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java)
  actually checks `now - largestTimestamp > retentionMs` and protects deletion
  with the high watermark. Future timestamps produce warnings.
- [Kafka 4.1.0 LogSegment](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java)
  actually rebuilds timestamp maxima from validated batches during recovery,
  and falls back to file mtime when no record timestamp exists. This differs
  from some missing-index wording in the original KIP proposal.

Adopt an immutable broker append time, checksum-protected in each disk record,
preserved by all replica and canonicalization paths. Application `Timestamp`
remains separate. A receiver must never replace append time with its own clock.
Use the maximum of all known append times in a closed segment. Different
segment boundaries can delay physical reclamation, never advance a record's
eligibility. Do not introduce Kafka wire formats or producer timestamp modes.

Use a disposable in-memory segment summary rather than a persistent sparse
time index: retention needs maxima, not offset lookup by time. Build summaries
once per segment/content generation from checksum-validated records. Missing
summaries cost reads and decoding, not authoritative metadata writes. Unknown
legacy time or corrupt evidence must not authorize time deletion. Byte limits
remain an independent policy, subject to the same visibility protections.

## Planned regression and fault checks

1. Real isolated original, incremental and bootstrap files with identical
   records: compare the baseline mtime predicate at a controlled cutoff; no
   operating-volume mtime changes. Repeat with durable record time.
2. Same/different segment boundaries, compaction mtime preservation, legacy/new
   mixtures, record checksum corruption, missing derived state, restart and
   bootstrap installation interruption.
3. Leader assignment versus client event time, follower skew, clock steps,
   future timestamps, leadership changes, HWM and transaction/event history
   protections, byte deletion, generation/checkpoint invalidation.
4. Measure logical frame and filesystem API bytes separately from physical
   device I/O; report reconstruction reads/writes, decoded records, allocations,
   CPU/wall time, temporary files and subsequent recovery work. State exclusions.

## Review boundaries

The first layer adds durable record-time transport and codec tests without
enabling time assignment or changing deletion. The second layer applies the
retention contract, activation/clock/visibility gates and recovery tests.
Existing cleanup and metadata-compaction work owns its algorithms. Preserve
the main branch's generation primitives and invalidate derived summaries at
those existing boundaries; do not depend on unrelated issue branches.
