# Cleanup progress for large committed records

## Design and pre-implementation checks

Baseline: official `cursus-io/cursus` main
`a49138e7922aa66b14cb4482a8b084e5b9c1b8cd`. The audit fixture encodes
2,813,411 bytes and appends a 2,813,618-byte storage record. Two 1-MiB reads
return zero records at offset zero; an 8-MiB read returns one record at offset
one. This is encoder/storage evidence, not administrative or Raft evidence.

[Accepted KIP-74](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/65864887/KIP-74+Add+Fetch+Response+Size+Limit+in+Bytes)
proposes the first-message exception and partition rotation. Kafka 4.1.0's
[ConsumerConfig](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java)
and [consumer documentation](https://kafka.apache.org/41/configuration/consumer-configs/)
describe first-batch progress separately from broker admission limits.
The versioned [LocalLog](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java)
passes `minOneMessage` to
[LogSegment](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java),
which expands `adjustedMaxSize` to the first batch size while retaining the
maximum committed position. These are implementation observations, not just
inferences from the KIP.

Cursus adopts first-record progress. It keeps its physical cursor, committed
cut, checksum, group/key checks, lifecycle/leader generations, transaction
controls/reservations and checkpoint invalidation. The disk reader continues
to enforce its caller's byte limit. Cleanup explicitly retries an intact first
record with a one-record request of exactly the reported size, never an
arbitrary 8-MiB budget. A partial turn yields before using this exception.

The durable record maximum is `disk.MaxMessageSize == util.MaxMessageSize ==
wire.MaxFramePayload` (64 MiB), including the storage envelope and checksum,
excluding its four-byte length prefix. `record_size.go` enforces this on append;
`replay_page.go` checks it before allocating. Metadata encoding alone has no
such admission limit. `consumer_offset_writer.go` routes encoded metadata
through internal PUBLISH and the partition leader; wire/replication envelopes
also consume the 64-MiB wire allowance. Thus a 64-MiB metadata JSON document
is not necessarily publishable. Existing accepted storage records remain
readable regardless of those different envelope costs. No write limit or
operating disk-headroom default is changed here.

The selected minimal change reserves one process-wide replay/validation slot,
including decoding and evidence validation, not just disk allocation. The
ordinary turn remains 128 records / 1 MiB / cooperative 10 ms. Only the first
record may use the explicit exception, and it ends the turn. Reservation is
nonblocking: no wait for memory while holding lifecycle or partition locks.
Payloads are not retained in progress/checkpoints. Cancellation is checked
before retry and after validation; a single synchronous checksum/JSON decode
can exceed 10 ms and is not preemptible. This trades a longer exceptional turn
for finite recovery work; splitting validation would require a separate parser
and durable parser state and is outside this focused change.

### Memory accounting

The reservation is exclusive across coordinators in one process. It covers
at most one 64-MiB serialized record (or a normal 1-MiB page), its decoded
strings, JSON validation, canonical copies and temporary validation maps.
The input ceiling is **not** a 64-MiB heap claim: JSON arrays of small values
can expand into much larger Go structs. A conservative provisioning envelope
for this existing decoder is **256 times the serialized bytes plus 16 MiB**
of fixed/page overhead, at one active scan per process; at the storage ceiling
that is **16 GiB + 16 MiB**. It accounts for typed slice growth and copies,
raw JSON/decoder buffers, strings, map entries and canonical re-encoding.
This is intentionally conservative; the measured peak for the fixtures is
reported separately. It does not include the broker's pre-existing group
state, other request paths, allocator/GC retention or the OS page cache, and
is not a hard RSS limit or a guarantee against OOM on an undersized broker.
The slot is an admission reservation, not a preallocation of this envelope.

The high worst-case allowance is the cost of retaining the existing decoder
and accepting all historically valid storage records. No concurrency setting
multiplies it within cleanup. A smaller hard working-heap cap for every
possible 64-MiB structured record would require incremental validation rather
than reducing the admission limit for new writes. That remains a distinct
resource tradeoff; benchmark peak heap must not be substituted for this bound.

Before implementation, define these checks:

- The same real large-record fixture stalls on baseline and advances on fix,
  with an unrelated historical record before the target committed deletion.
- Exact byte boundary, insufficient remaining budget, >1 MiB, >8 MiB and
  exact storage maximum; over-maximum length, checksum damage and truncation
  must fail closed with no checkpoint/cursor advance.
- Cancellation, reservation contention/release, transient read failure and
  checkpoint restart must preserve progress without weakening invalidation.
- Preserve existing control, reservation, generation, unrelated-group and
  continuation tests; measure frame bytes separately from physical I/O,
  decoded work, memory, CPU, cancellation and unrelated request latency.
- Exercise normal admission/write paths and DELETE/TRUNCATE in three isolated
  brokers, lost replies, temporary reads, re-registration and checkpoint restart.
  Direct disk fixtures do not satisfy this integration requirement.

## Measurements

Linux amd64, Go 1.25.0, Docker on a Windows host; one non-race test process.
Each row scans 257 small historical records and a target tombstone, with an
optional encoder-produced lifecycle record inserted between small records.
Initialization, fixture appends, coordinator startup and the initial GC are
outside the timed window. The filesystem is warm. Other isolated development
tests were running on the host; these are observations, not latency SLOs.

| Large fixture | Records / disk decodes | Serialized bytes | Logical read bytes | Elapsed | Process CPU | Sampled peak heap | Local GetGroup p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| None | 258 / 258 | 83,993 | 85,025 | 14.10 ms | 16.33 ms | 2,055,592 B | 13.32 us (5 samples) |
| 32 x 1,024 assignments | 259 / 259 | 2,897,611 | 2,898,651 | 323.14 ms | 445.96 ms | 22,109,416 B | 164.49 us (262 samples) |
| 128 x 1,024 assignments | 259 / 259 | 11,394,247 | 11,395,287 | 1,189.05 ms | 1,441.42 ms | 81,765,192 B | 74.66 us (829 samples) |

Heap samples use `runtime.ReadMemStats` on a 1-ms ticker; peaks between samples
can be missed. Baseline heaps were 992,840 / 3,775,608 / 29,056,264 bytes;
the sampled peak includes pre-existing process memory. Total allocations
during the windows were 1,255,720 / 36,901,792 / 146,139,576 bytes. CPU includes
the measurement sampler and GC. Local GetGroup is a concurrent lookup, not a
network request, heartbeat, admission or offset-write latency measurement.

Every physical record is deserialized exactly once in these successful scans.
The large record needs a four-byte length probe before its reserved read;
remaining-budget probes and per-record four-byte headers are included in
logical read bytes. Metadata validation still uses the existing JSON decoder:
identity extraction, raw-field decoding, typed decoding, canonicalization and
semantic validation are CPU work, not additional disk reads. Checkpoint restart
tests assert that validated large payloads are not read/decoded again.

`/proc/self/io` physical `read_bytes` and `write_bytes` deltas were zero in
all three warm-cache scan windows. That does not mean recovery costs no device
I/O in a cold cache. The tests made no network transfers and did not write
checkpoints in these scan-only windows. Checkpoint/restart and the three-broker
scenarios are separate tests. Never label serialized or logical bytes as
physical I/O, or these scan-only numbers as total broker recovery cost.

Windows baseline and fixed disk/coordinator tests also cover exact 1-MiB and
64-MiB storage boundaries, >8-MiB progress, over-maximum headers, truncation,
checksum damage, invalid unrelated schema/key, cancellation and slot release.
The first sandbox run failed checkpoint replacement with Windows Access Denied;
the same isolated tests passed outside that sandbox. Other existing host tests
hit the 5% disk headroom threshold and were rerun in Linux; production defaults
were not modified. The controller control-record regression now follows
`CleanupContinue` turns instead of assuming all partitions fit in one 10-ms
turn under race instrumentation.

Three-broker fault/restart verification and its request/IO observations are
recorded once the isolated run finishes; they are not implied by this table.
