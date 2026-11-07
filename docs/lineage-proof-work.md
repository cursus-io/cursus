# Lineage proof work

The comparison and regression contract below were defined before implementation.

The hosted interrupted-recovery fixture recorded 419,214,612 receiver lineage
read bytes before readback for 34,686,641 payload bytes. These counters measure
application reads, not physical I/O or all network traffic. Inspection shows
that different proof endpoints repeatedly start at the same sparse checkpoint,
and a request needing only a few records still captures up to 256 records.

Kafka 4.1.0 `AbstractFetcherThread.scala` retains fetch progress across delayed
partitions and rechecks the current leader epoch before applying truncation
results. Adopt reuse only inside a validated generation. Cursus must retain its
complete retained-prefix hashes, push replication, Raft commit authority and
strict proof publication checks. Kafka epoch agreement is not a replacement
for Cursus content proof. Source:
https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/AbstractFetcherThread.scala

Implemented changes:

* Compute sparse requested endpoints in ascending order, return the existing
  descending wire order, with the same endpoints and digest domain.
* Start from the nearest validated in-memory endpoint or checkpoint at or below
  the requested end. Endpoints remain bounded to 128 entries and belong to the
  current builder generation. No persisted cache becomes authoritative.
* Limit endpoint raw reads to the number of records still required, preserving
  byte and segment-transition budgets, checksum checks and cancellation.

Regression contract:

1. Use 4,097 records with 8 KiB payloads; compute reference canonical digests
   independently of endpoint selection. Verify every returned digest and order.
2. Measure cold reconstruction separately from warm endpoint work. Within a
   checkpoint interval, request increasing endpoints: each newly required
   record should be read and hashed once while its validated cursor is cached.
3. Repeat the exact endpoint set with zero additional historical read/hash work.
   Exercise more than 128 distinct endpoints and require bounded cache size,
   correct digests and finite checkpoint fallback after eviction.
4. Keep existing cancellation, mutation invalidation, sidecar corruption,
   checksum corruption, segment rotation and concurrent append tests. A stale
   generation must never publish a proof, even if its cached digest matches.
5. Run lineage tests with the race detector, then the interrupted mixed-load
   E2E. Compare receiver read/hash/decode work within process lifetimes, report
   transfer and journal bytes separately, and retain full content verification.

The intended effect is less redundant work and shorter availability gaps. Cold
reconstruction remains linear in retained history; sparse-cache eviction may
require bounded rescans. No production latency or throughput SLO follows from
the small fixture, and no stronger durability guarantee is introduced.

## Measured results

The regression failed on the original implementation. For 32 increasing
endpoints requiring 265,536 frame bytes, it read 33,457,664 bytes and hashed
528 records. The corrected implementation read exactly 265,536 bytes and
hashed 32 records. A sparse candidate set requiring 742 records changed from
12,546,624 read bytes and 1,360 hashes to 6,157,116 bytes and 742 hashes.
All candidate digests and their original descending wire order matched an
independent canonical reference chain.

Cold reconstruction remained 33,997,034 read bytes and 4,097 hashed records
for 33,996,906 frame bytes. The extra 128 bytes are bounded header lookahead.
All native lineage tests passed in 36.812 seconds, including retained-start
changes, cancellation, sidecar damage, publication failures and concurrent
append/rotation/retention. The Linux lineage and raw-frame race suite passed in
15.099 seconds, including the added growing-tail check and stale-cursor rejection
after truncate/reappend. These local measurements do not establish a new cluster
recovery cost or availability SLO. The endpoint microcase improvement must not
be applied as a ratio to the earlier hosted recovery measurement; the interrupted
mixed-load fixture must still measure that workload independently.
