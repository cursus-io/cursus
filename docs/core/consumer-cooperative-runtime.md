# Cooperative broker and Go client runtime

Implementation and local validation record (2026-10-23). The runtime is opt-in;
these measurements do not establish production capacity or acceptance for other SDKs.

## Kafka comparison

[Accepted KIP-848](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
and [Kafka 4.1.0 CurrentAssignmentBuilder](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/modern/consumer/CurrentAssignmentBuilder.java)
separate target assignment from current ownership. The versioned implementation
waits in UNREVOKED_PARTITIONS for the old owner and excludes partitions whose
current epoch is still allocated from a recipient's grant. Adopt this ordering,
independent member reconciliation, and retention of unaffected workers.

Cursus keeps its metadata log, wire commands, registration/topic lifecycle
identities and transaction reservations. An ownership token includes the group
registration, member session, topic lifecycle and partition grant epoch. It is
not a group-wide generation. An unresolved transaction reservation prevents a
replacement grant; an already durable commit decision remains materializable.
The Go API must expose message-bound ownership for asynchronous commits.

## Runtime and compatibility

JOIN_GROUP, HEARTBEAT and LEAVE_GROUP explicitly select protocol=cooperative.
Membership responses contain the member session and its current grants, including
pending revocation keys. Heartbeats acknowledge only exact revocation keys after
the corresponding workers and handlers have stopped. Losing a response never
authorizes a client to infer a new grant. Unchanged heartbeats do not append a
full lifecycle checkpoint. Expiration rechecks clocks and session identities
under the mutation lock. Publication follows durable metadata append.

CONSUME, STREAM, COMMIT_OFFSET and transaction offset authorization carry the
same token. Reads validate ownership immediately before delivery, including when
the data leader differs. The existing stable-offset lookup already requires the
authoritative coordinator on each page, including for eager groups. Activation
adds one final ownership check, including for tokenless reads, to reject silent
protocol downgrade. No long-lived eager-mode cache can authorize a later
cooperative group without a durable invalidation protocol. SDK delivery checks
cancellation, and commit submission checks the worker
identity. Delayed operations keep their original grant; they never acquire the
replacement worker's authority. An already authorized operation may complete
while revocation is pending.

Existing eager commands retain their protocol and assignment behavior. Activation is explicit and requires
capable registered processes and voters; admission must reject later incompatible
processes. Persist the activation barrier in a versioned controller snapshot.
An old SDK cannot silently downgrade a cooperative group. Binary downgrade is
not achieved by draining clients: retained new-format records/snapshots still
require capable readers. Java/Python clients and their measurements are separate
work; this change's end-to-end acceptance applies to the broker and Go SDK.

## Validation defined before implementation

### Broker read work and synchronous append experiments

The broker follow-up targets the group offset lock held across durable append.
With the bounded SDK pipeline already installed, three 9,600-record runs at
`c7f19f6` accumulated 9.39 s in `GetStableOffset` lock waits and 9.04 s in offset
snapshot append waits. These are cumulative goroutine waits, not elapsed-time
percentages or independent costs that can be added together.

[Kafka 4.1.0 CoordinatorRuntime](https://github.com/apache/kafka/blob/4.1.0/coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java)
separates committed-state reads from write responses held until commitment.
[OffsetMetadataManager](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java)
uses timeline lookups and rejects stable offset requests for pending
transactional offsets. Cursus adopts committed-state visibility and keeps its
transaction barrier. It does not introduce Kafka's event processor or timeline
storage: its existing group and membership locks still serialize writes,
reservations, and ownership changes.

A temporary committed-offset read window was prototyped while keeping the
write/membership locks and excluding groups with reservations. Blocked append,
failed append, response loss/restart, blocked resolver, and concurrent reservation
tests passed. It reduced read-lock wait in a diagnostic profile, but its added
throughput benefit was inconsistent across the paired runs, including lower
join throughput. The first broker follow-up (`6e78a23`) discarded that window.
The subsequent contention investigation below revisits it together with the
read-only timeout scan and commit-collection changes; write/membership locks,
synchronous acknowledgement, admission and transaction barriers remain.

Profiling also exposed repeated decode work in the broker's committed-page
reader. Every request started with 1,024 raw records even when the caller wanted
eight visible records, then discarded the unreturned tail. The following
request decoded that tail again. The new scan starts with the visible demand,
grows geometrically only across completely filtered pages, and retains the
1,024-record scan cap. After visible progress it returns to the remaining demand.
Byte budgets, oversized-first progress, fixed HWM/LSO limits, marker filtering,
and the cursor through empty filtered pages are unchanged.

[Kafka 4.1.0 LogSegment.read](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java)
bounds the returned file slice by the requested byte size and maximum readable
position, with an explicit first-batch size exception. Cursus stores individual
records and filters them on the broker, so it also bounds the initial raw record
demand rather than copying Kafka's batch representation. Regression checks count
records read across 200 eight-record pages, verify progress across 4,096 aborted
records, and retain the existing byte-budget and transaction visibility tests.
This changes read work, not persisted bytes or the durable commit policy.
The selected change adds no log record, background goroutine, persistent cache,
or recovery state. Full topic snapshots and ownership checkpoints retain their
existing recovery costs. Acceptance measures metadata payload bytes and records,
waits for every committed offset to reach log end, and preserves the existing
duplicate bound and three-broker fault checks.

The real-disk `BenchmarkReadCommittedSmallPage` reads eight 128-byte records
from a 1,600-record committed backlog. Three 100-iteration runs at INFO logging,
Go 1.26.2, two container CPUs and GOMAXPROCS=2 measured:

| Per-page metric | Baseline `c7f19f6` | Selected scan change |
| --- | ---: | ---: |
| Decoded storage bytes | 235,520 | 1,840 |
| Allocations | 2,070 | 34 |
| Allocated bytes, median | 848,438 | 11,719 |
| Read time, median | 1.030 ms | 0.043 ms |

This isolates page reading; it is **not** a 24-fold consumer throughput claim.
The existing broker group lock still covers durable append, and cooperative
workers still wait for each batch's durable commit. These remain costs that
eager's asynchronous interval commits do not pay in the same way.

[All observations and limitations](../measurements/consumer-cooperative-broker-2026-10-24.json)
include the four-variant DEBUG comparison: five repetitions, both 960-record
join and 9,600-record single-member workloads, eager and cooperative modes
(80 passing cases). The selected change's cooperative medians were 921.4 versus
812.5 records/s for join, and 1,204.7 versus 883.4 records/s for the larger
backlog. However, eager controls also varied substantially, ranges overlapped,
and these results do not establish that the end-to-end eager gap narrowed.
Cooperative duplicates were 0–24 at join and zero in the larger backlog; the
existing assertions were unchanged. Long-backlog metadata replay payload
medians were 139,193 versus 140,172 bytes (408 versus 411 records), not a material
recovery-format saving. Both fetched exactly 9,600 records / 1,228,800 payload
bytes in the large workload.

An INFO confirmation completed 27 cases, then its fourth selected-version
large-backlog cooperative case stalled after repeated localhost TCP timeouts
and failed the existing 60-second delivery assertion. Twelve planned cases
were not run. A previous three-variant INFO attempt failed without retained
logs because the initial harness exported only on success; it is excluded from
numeric evidence. After adding failure diagnostics without changing deadlines,
five baseline and five selected-version large-backlog rechecks passed. The
earlier stall's root cause is not established. No passing subset is presented
as a completed INFO performance comparison or proof of a narrower eager gap.

### TCP stall and commit contention follow-up

The historical INFO failure contained 51 heartbeat retry lines and 210 response
write errors; requests timed out and late responses found closed connections.
It did not capture disk latency or goroutine stacks during the stall. Eight
instrumented repetitions on `6e78a23` (32 cases) passed with periodic stack
samples, CPU, mutex and blocking profiles. They accumulated 48.52 seconds of
`GetStableOffset` blocking across concurrent goroutines; this is not elapsed
time. The original trigger remains unconfirmed, rather than being attributed
to TCP, disk or host load without evidence.

A deterministic paused-writer test exposed an amplification mechanism:
ordinary commits retain `c.mu.RLock` and `group.mu` during durable append.
`checkSingleGroupTimeout` requested `c.mu.Lock` even for a read-only scan with no
expired member. A queued Go RWMutex writer then blocked subsequent readers,
including readers of an otherwise available committed-offset window. The first
window-only slow-writer test failed one of three repetitions through this path.
Heartbeat requests also timed out during the injected append pause and produced
the same late-response/closed-connection symptom after release. This establishes
a causal lock-convoy mechanism, **not the cause of the historical append delay**.

The selected design follows Kafka's committed-state/durable-response distinction
described above while preserving Cursus's serialization and persisted formats:

- Session timeout inspection and an already-established owner grace-period
  lookup use shared locks. Installing/removing the owner clock and actual
  expiration retain the mutation lock; cooperative expiration rechecks current
  session identities and heartbeat timestamps.
- During an ordinary cooperative offset append, a temporary read window borrows
  the last acknowledged offsets only if the entire group has no transaction
  reservations. Readers hold the window through the transaction resolver lookup.
  Closing it drains readers before publishing offsets or allowing a reservation
  mutation. It is not a durable cache, does not expose the candidate offset and
  does not release the commit's membership or group mutation locks.
- Go workers still wait for durable success before handling their next batch.
  The two-millisecond collection deadline ends early when every running worker
  has queued its completed batch, or the protocol batch cap is reached. Worker
  counts only guide batching; every original token is still validated. Partial
  batches retain the deadline, so idle partitions cannot block progress.

Regression checks cover append success/failure, lost response and replay,
transaction-resolver drain, reservation fencing, established distributed owner
grace, unexpired scans during append, early/full and timed/partial batches,
cancelled requests and revocation waiting for in-flight commits. The real TCP
slow-writer test checks old-offset visibility, no premature acknowledgement,
heartbeat timeout while mutation remains fenced, and recovery after release.
Recovery checks count serialized metadata records and payload bytes. No new
record type, protocol number, ownership grant or recovery state is introduced.

Availability remains bounded by real storage delays: membership mutation and
fresh ownership observations can still wait for a durable append. This change
removes unnecessary read-side contention, not the durability boundary. A future
separation of membership clocks or append sequencing needs a different design
and its own fencing/recovery checks.

Eight rotated repetitions of four variants (baseline `6e78a23`, broker only,
collection only, and both), each with eager/cooperative and short/long workloads,
passed all 128 cases. Go 1.26.2 ran in one Linux container limited to two CPUs
with GOMAXPROCS=2; broker logging used the same INFO overlay and SDK logging was
unchanged. The host was shared, not dedicated benchmark hardware. All cases
waited for every unique delivery and all six durable offsets at log end.
[Raw observations](../measurements/consumer-cooperative-contention-2026-10-24.json)
retain outliers, variant controls, recovery work and the differing commit policies.

| Measurement, median unless a range | Baseline | Combined change |
| --- | ---: | ---: |
| Join / 960-record durable records/s | 768.7 | 991.5 |
| Long / 9,600-record durable records/s | 1,362.45 | 1,648.85 |
| Join cooperative/eager per-run ratio | 62.56% | 71.45% |
| Long cooperative/eager per-run ratio | 46.56% | 51.57% |
| Long cumulative worker commit wait | 26,220.5 ms | 19,994.5 ms |
| Long metadata replay records | 382 | 310 |
| Long metadata replay payload | 130,235 B | 106,352 B |
| Join duplicate deliveries, range | 0–24 | 0–12 |
| Long duplicate deliveries | 0 | 0 |

The combined long-backlog median was 21.0% higher, but the 772.7–2,113.6
records/s range overlapped baseline's 1,135.2–2,017.7. Eager controls also varied;
the ratio is the median of each run's cooperative/eager rates, not the ratio of
medians. These are observed local improvements, not a guaranteed performance
margin. Every long cooperative case fetched exactly 9,600 records / 1,228,800
payload bytes. Replay-byte reductions reflect fewer existing-format snapshots,
not a smaller snapshot format; payload counts exclude headers and replication.

A separate three-run blocking profile reduced cumulative `GetStableOffset`
wait from 21.210 s to 0.088 s and worker commit wait from 78.713 s to 54.988 s.
Append wait was 13.679 versus 10.687 s. These overlapping goroutine totals must
not be added together or interpreted as wall-clock percentages.

The combined variant's fifth long run had one in-run heartbeat I/O timeout,
then recovered with a 1.880 s maximum partition-0 delivery gap and zero
duplicates. It remains in the reported results. A subsequent 20-run diagnostic
with 250 ms goroutine snapshots passed with no I/O timeout; cancellation-time
closed-connection messages are not counted as stalls. Samples showed metadata
append waiting for the disk flush loop's fsync, but cannot establish that a
particular fsync caused the earlier timeout. The original long stall's initiating
cause therefore remains unconfirmed. Diagnostic runs with stack/profiling
overhead are excluded from throughput comparisons.

### Runtime regression checks

- Prove write failure and response loss do not publish an uncommitted grant;
  restore pending revocations and transaction barriers from log and snapshot.
- Delay/duplicate ACKs; return an expired member; change topic lifecycle; resolve
  transactions by commit and abort. Reject old fetch, commit and reservations.
- Run continuous traffic during join, leave, crash and rolling restart, including
  coordinator failover and a data leader on another broker.
- Compare eager/sticky with cooperative using identical workloads. Record moved
  partitions, retained worker identity, processing gap, duplicates/replay records
  and payload bytes, worker rebuilds, metadata recovery bytes and decoded grants,
  and convergence duration. Report measured results separately from assertions.
- Exercise old/new clients, activation failure in a mixed cluster, and persisted
  downgrade rejection. Run existing eager regression and race suites.

Availability trades a moving partition's progress for exclusive ownership while
waiting for quiescence or transaction resolution. Unaffected partitions should
continue. Recovery cost includes full persisted ownership state and bytes; lower
request counts alone do not establish cheaper recovery.

## Enabling the broker and using Go

Set `consumer_cooperative_enabled: true` (`--consumer-cooperative-enabled` or
`CONSUMER_COOPERATIVE_ENABLED=true`) on all brokers after upgrading every
registered broker and Raft voter. Activation requires lifecycle protocol 11 and
bootstrap recovery protocol 2. It is durable and rejects incompatible later
registrations. In distributed mode the durable activation state remains the gate
even when a restarted broker has its local trigger flag disabled; standalone
mode continues to use the local flag. Cooperative groups cannot switch back to
eager membership.

The Raft directory marker advances to 14 when this binary starts, including when
cooperative activation is disabled. Activated controller snapshots use version
13; existing cooperative ownership snapshots without activation remain version
12. Do not point an older binary at these directories. Draining clients or
turning off the feature flag does not make persisted state downgrade-compatible.
History compaction already uses lifecycle protocol 10 and Raft marker 13 on main.
It does not implement cooperative runtime commands. Keeping those numbers for
this runtime would incorrectly admit history-only brokers and lose the startup
downgrade barrier. Markers 9–13 remain readable for upgrade; validation leaves
the marker unchanged until the existing upgrade stage. Both history and
cooperative activation flags are retained and validated independently on restore.

```go
cfg := sdk.NewDefaultConsumerConfig()
cfg.BrokerAddrs = []string{"localhost:9000"}
cfg.Topic = "orders"
cfg.GroupID = "order-workers"
consumer, err := sdk.NewCooperativeConsumer(cfg)
if err != nil { return err }
return consumer.Run(ctx, func(ctx context.Context, record sdk.CooperativeMessage) error {
    return process(ctx, record.Payload)
})
```

The additive API accepts one explicit topic per group. Polling and streaming
retain workers with unchanged grants. A handler must finish all work it owns
before returning; cancellation alone does not establish quiescence. Handler
errors retry according to `HandlerMaxRetries` and its backoff settings, then
stop the consumer without committing that record. Session fencing stops `Run`;
create a new consumer to explicitly rejoin after diagnosing the interruption.

`EnableAutoCommit` commits the last successfully processed offset of each batch
synchronously. Concurrent completed partitions share a commit, collected for up to
two milliseconds from the oldest queued request and capped at 64 partitions per request.
Collection ends early when every running worker has submitted its batch.
Time already spent behind an in-flight append counts toward this window. Each worker waits for
the durable response before processing its next batch. This API does not use
the eager consumer's `AutoCommitInterval`
background batching. With auto-commit disabled, call
`consumer.Commit(ctx, record, record.Offset+1)` or
`producer.SendCooperativeOffsets(record, record.Offset+1)` inside a transaction.
Retain the delivered record's token for deferred work; never build a token from
current assignments. External side effects still require application idempotency.
A cancelled partially processed batch can be delivered again.

`BATCH_COMMIT topic=... group=... member=... offsets=P0:8,P1:8 ownerships=...`
carries an ordered array of original ownership tokens encoded by
`protocol.EncodeCooperative`. It supports one topic/session/registration and
rejects duplicate or mismatched partitions. The coordinator validates every
grant, reservation and monotonic offset before writing one existing-format
offset snapshot; a failed append publishes none. Cancelled queued commits are
discarded. An in-flight commit settles before its worker can acknowledge revoke.
A stale grant or reserved partition is isolated with individual commits, so
retained partitions can progress. Retryable transport/coordinator failures retain
the existing bounded retry policy. A repeated acknowledged batch is a no-op.

### Commit batching comparison and regression contract

[Kafka 4.1.0 CommitRequestManager](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CommitRequestManager.java)
accepts maps of topic-partition offsets, and
[OffsetMetadataManager](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java)
validates commits and produces records for the coordinator runtime. Cursus adopts
multi-partition submission but retains message-bound grant epochs, synchronous
per-batch completion and full topic offset snapshots. It does not substitute
current membership authority for an old record or adopt interval auto-commit.
The persisted format and O(P) snapshot size remain unchanged; fewer appends also
mean fewer replay records/bytes before compaction. One slow partition does not
hold up collection. A one-worker workload can flush immediately instead of
paying a collection delay without a batching benefit.

Checks defined for this optimization: all-or-nothing stale-token and reservation
rejection; failed append leaves every offset unchanged; replay-safe duplicate
submission; independent progress after a rejected mixed batch; no revoke ACK
overtaking an in-flight commit; cancellation before submission; mixed history-only
broker refusal; combined activation restore; fault recovery with all committed
offsets draining to log end. The paired TCP measurement records both metadata
replay bytes and handler duplicate bytes, not just request reduction.

Membership and offset operations reuse separate coordinator connections, keeping
queued commits out of the heartbeat lane. Per-request deadlines and reconnects
apply to both. Failed partition requests and commits use exponential backoff
with jitter, starting at least at 100 ms and capped at about one second (or the
configured minimum if larger). Healthy polling retains its configured cadence.
Partition fetch connections retain their cursors until reconnect.

An ambiguous lifecycle append closes ownership admission rather than reusing a
possibly persisted ownership revision. In distributed mode, successful full strict
metadata replay atomically clears lifecycle fences; failed or scoped replay cannot
clear them. Transient routing and append errors remain retryable, but retrying a
request does not itself remove an ambiguous lifecycle fence.

In standalone mode, a failed cooperative lifecycle append requires a broker
process restart. The running process has no automatic unfence path: joins report
`group lifecycle update in progress`, and cooperative reads/membership report
`coordinator_not_available`; reconnecting the SDK will not restore availability.
After correcting the underlying storage/write failure, restart the broker using
its existing data directory and wait for startup consumer-metadata replay to
succeed before resuming consumers. `NewCoordinatorWithRecovery` reconstructs the
group from the local committed metadata log, so it can resolve whether the
ambiguous write persisted. If that replay fails, recovery is not complete. This
is an explicit standalone availability cost of retaining the ownership fence;
no in-place automatic recovery is implemented for this case.

## Reproducing validation

```sh
go test -race ./pkg/server ./pkg/coordinator ./pkg/cluster/replication/fsm -run '^TestCooperative' -count=1
go test ./pkg/server -run '^TestCooperativeGoEagerComparison$' -count=1 -v
go test ./pkg/coordinator -run '^$' -bench 'Benchmark(CooperativeRecovery|StickyAssignmentJoin)' -benchmem
RUN_E2E_COOPERATIVE=1 go test ./test/e2e-cluster -run '^TestCooperativeBrokerGoFaultRecovery$' -count=1 -timeout 20m -v
```

The cluster test creates an isolated three-broker compose project on ports
33001–33003. It covers member join/leave, an actual client process kill,
coordinator stop/start and rolling broker restart under continuous traffic.
The dedicated CI job uses the existing main/release pull-request triggers.


## Bounded fetch pipeline (2026-10-24)

The remaining gap includes a client execution difference: eager separates fetch
and handler work, while the initial cooperative worker performed fetch, handling,
synchronous commit and the polling delay serially. A fresh five-run profile of
coalesced cooperative consumption accumulated 5.19 s of worker commit waits,
0.85 s in stable-offset read lock waits, and 0.95 s in durable snapshot appends.
These overlapping goroutine totals are not elapsed time; a read can wait behind
another partition's append. The asynchronous eager commit policy still provides
a throughput advantage with a larger processed-but-uncommitted window.

Kafka 4.1.0 separates network responses in
[AbstractFetch](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractFetch.java)
from record delivery in
[FetchCollector](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/FetchCollector.java).
The latter checks assignment/fetchability before returning buffered records.
Cursus adopts this stage separation with a stricter, one-batch read-ahead limit.
It retains its original message-bound grant and synchronous auto-commit boundary;
this is not adoption of Kafka's interval auto-commit policy. KIP-848's ownership
handoff remains unchanged.

Each partition has a fetch goroutine and an unbuffered batch handoff. While one
batch is being handled or committed, the reader may decode one additional batch
and then must wait. Handling the next batch still requires durable success for
the previous batch. Cancellation closes the fetch connection and discards queued
work. Worker completion waits for the reader, handler and any submitted commit,
so a revocation acknowledgement cannot overtake them. Polling and streaming use
the same processing boundary. Manual/transactional commit users retain their
existing responsibility for deciding when to commit.

This overlaps reads and polling delays with useful work without releasing the
broker's group lock or weakening transaction reservation checks. The tradeoff is
one additional decoded batch per partition, bounded by the existing batch/frame
limits, plus one goroutine. On cancellation that batch may be fetched again by
the next owner; fetched payload bytes therefore include discarded work as well
as handler replay. Metadata encoding and recovery format are unchanged.

Regression checks cover a blocked first handler with exactly two batches fetched
and only one record handled; a blocked append with exactly one batch handled;
cancellation, handler failure and rejected commit without delivery of queued
records; polling/streaming revocation and stale-token rejection. The comparison
also requires every committed offset to reach log end and asserts at most one
batch of duplicate handler deliveries per transferred partition (zero without
membership changes). Existing lost-response and transaction barrier checks still
apply. The performance comparison includes both a 960-record join and a
9,600-record single-member backlog, with matched baseline test code and alternating
execution order. Neither is a production capacity claim.

Five final alternating runs compared published commit `cac136c`, read-ahead
with a 1 ms collection window, and the selected 2 ms window. Collection now
starts when the oldest request is queued, so requests waiting behind an append
do not pay another full collection delay afterward. The 2 ms window gave more
partitions time to share a snapshot; it retains the same per-partition processed
work limit. This adds up to 1 ms of intentional collection delay for an otherwise
idle queue. A slow or blocked partition is never a prerequisite for flushing.

| Median / range | Serial coalesced baseline | Selected pipeline | Eager control with selected pipeline |
| --- | ---: | ---: | ---: |
| 9,600 records, durable records/s | 1,634.9 | 2,041.1 | 3,838.3 |
| 9,600 records, durable records/s range | 1,396.6–1,722.2 | 1,917.2–2,110.9 | 3,602.2–4,015.5 |
| 9,600 records, metadata records | 429 | 410 | 218 |
| 9,600 records, metadata replay payload bytes | 146,153 | 140,012 | 72,840 |
| 9,600 records, handler duplicates | 0 | 0 | 0 |
| 960 records + join, durable records/s | 1,129.5 | 1,568.9 | 2,073.5 |
| 960 records + join, handler duplicates (range) | 0–18 | 9–15 | 144–240 |
| 960 records + join, metadata records | 78 | 44 | 34 |
| 960 records + join, metadata replay payload bytes | 34,839 | 23,728 | 11,212 |
| 960 records + join, fetched payload bytes | 125,952 | 129,024 | not instrumented |
| 960 records + join, maximum retained-partition gap | 93 ms | 43 ms | 57 ms |

Durable throughput improved by 24.8% for the longer backlog and 38.9% with a
join. The baseline's longer-backlog eager control was 3,716.3 records/s versus
3,838.3 in the selected runs (+3.3%); host variation remains visible. The 1 ms
pipeline candidate reached 1,892.3 records/s on the longer backlog, below the
selected window's 2,041.1. Initial read-ahead-only probes did not consistently
improve the short join workload. The final change combines read-ahead with
queue-age-aware collection; the final comparison does not isolate each change's
causal contribution. Absolute rates are not directly comparable with the earlier
host runs below.

Replay metadata bytes fell 4.2% on the longer backlog and 31.9% with a join.
The read-ahead tradeoff is visible: median fetched payload during the join rose
by 3,072 bytes (24 records), even though handler replay stayed within the original
three-partition/eight-record bound. The no-join run fetched exactly 9,600 records
/ 1,228,800 bytes in both cooperative variants. Eager still leads throughput;
the retained synchronous commit and authoritative ownership/stable-offset checks
remain costs. No asynchronous processing past an unacknowledged commit was added.

[Raw runs, controls and candidate results](../measurements/consumer-cooperative-pipeline-2026-10-24.json)
include all 60 workload/mode observations. Both binaries used the same updated
test fixture; baseline source was archived from `cac136c`, with only the test
file overlaid. Tests ran without race instrumentation in Linux/Go 1.26.2 with
two CPUs and GOMAXPROCS=2. Logs were buffered on the container filesystem and
copied out after execution. To reproduce a current run:

```sh
RUN_COOPERATIVE_THROUGHPUT=1 go test ./pkg/server -run '^TestCooperativeGo(EagerComparison|BacklogThroughput)$/(eager|cooperative-batched)$' -count=5 -v
```

The larger diagnostic workload is opt-in; the short join comparison and all
prefetch/commit/revocation safety tests remain in ordinary race and coverage runs.
The durable metric waits for every partition's committed offset to reach its log
end, unlike the older handler-completion metric. Metadata counts include lifecycle
and shutdown records, but exclude headers, compaction and broker replication
traffic. All control and candidate runs were kept; no throughput threshold is
encoded as a correctness test.

Final local validation passed SDK, coordinator and controller race suites;
the server suite including the large diagnostic passed in 149.656 s after its
inherited 20 s workload deadline was adjusted. Its initial timeout also affected
the unchanged eager control under concurrent validation load. The large
throughput diagnostic is now explicitly requested for measurements; the same
prefetch, failed-commit, duplicate-bound and revocation checks remain ordinary CI
tests. `golangci-lint run ./...` reported zero issues.

The selected runtime passed a fresh isolated three-broker fault run (289.72 s),
including all offsets draining to log end. Join took 7.350 s, process-kill recovery
12.413 s, coordinator stop/start progress 11.104/14.572 s, and rolling restarts
26.347/33.121/51.741 s. The run overlapped race/static checks; these phase times
include readiness and are not recovery SLOs. The main consumer created/stopped
12 workers; broker restarts added none. It fetched/delivered 309 records and
41,361 payload bytes. Parent/graceful-peer duplicates were two records / 268
bytes, and the maximum retained-partition gap was 13.269 s. Killed-child delivery
counts and broker replication bytes remain outside these counters.

## Commit throughput analysis and measurements (2026-10-24)

The original runtime serialized partition commits through one operation lane,
persisting a full topic offset snapshot for each eight-record batch. The
coordinator holds the group lock through the durable write, so stable-offset
reads also wait behind these appends. A five-run block profile accumulated
7.15 seconds in `GetStableOffset` and 3.98 seconds in `writeOffsetSnapshot`.
These are overlapping goroutine wait totals, not elapsed time or CPU percentages.
Eager consumption amortizes writes with asynchronous interval commits, so its
throughput advantage cannot be attributed to the rebalance algorithm alone.

The extended TCP test compares eager, single synchronous cooperative commits
and coalesced synchronous cooperative commits in the same build on main
`9c43d39`. Single mode disables auto-commit and calls the existing public
`Commit` after every eight successful records. Both cooperative modes wait for
commit completion before the next batch. The final five non-race repetitions
use the same six-partition/960-record workload and two-CPU limit described below.
Raw per-run results are in
[the measurement artifact](../measurements/consumer-cooperative-commit-2026-10-24.json).

| Measurement | Cooperative single | Cooperative coalesced | Eager |
| --- | ---: | ---: | ---: |
| Median handler records/s | 1,533.5 | 1,935.9 | 3,488.0 |
| Handler records/s range | 786.5–1,557.9 | 1,845.3–2,037.1 | 2,308.1–3,614.6 |
| Median drain after join | 580 ms | 450 ms | 240 ms |
| Duplicate handler records | 7–16 | 0–16 | 96–144 |
| Duplicate payload bytes | 896–2,048 | 0–2,048 | 12,288–18,432 |
| Median metadata records | 126 | 52 | 42 |
| Median metadata replay payload bytes | 50,284 | 26,368 | 13,836 |
| Median maximum retained-partition gap | 39 ms | 29 ms | 13 ms |

Coalescing improved median throughput by 26.2%, reduced metadata record count
by 58.7% and replay payload bytes by 47.6%. The original synchronous replay
window and token fences remain; duplicate reduction relative to eager is
preserved. It still trails eager throughput. Absolute rates vary with host
load, and the first single-commit run was slower; this small backlog test is
not a production capacity claim. Metadata measurements include membership and
shutdown records and exclude compaction, record headers and broker replication
bytes. The per-record snapshot remains O(P).

Linux race checks passed for the affected SDK/runtime and recovery paths,
including failed/duplicate commits, stale grant isolation, transaction barriers,
cancel-before-send, revoke during commit, and a lost response retried with the
same token/offset. Current main's existing cooperative/history/index/strict
recovery and disk tests passed before integration. Combined activation restore
and marker 13-to-14 upgrade checks passed after integration. Native Windows
filesystem tests require the Linux fixture for atomic replacement, and local
TCP tests require permission beyond the desktop sandbox.

The three-broker fault run passed join, graceful leave, client process kill,
coordinator stop/start and all three rolling restarts. All six committed offsets
reached log end. Phase times were 13.358 s for join, 13.405 s after process kill,
17.804/30.868 s for coordinator stop/start, and 10.033/8.914/8.142 s for rolling
restarts. These include readiness checks and overlapped static/race validation.
The main consumer constructed 12 workers total; broker restarts added none.
It fetched/delivered 194 records / 25,951 payload bytes; the parent and graceful
peer observed zero duplicates. The killed child's deliveries are excluded, so
this is not an exactly-once claim. Maximum retained-partition gap was 10.916 s.

## Original measurements before commit batching

The paired TCP test uses six partitions, 160 records per partition, 128-byte
payloads, batches of eight, a one-millisecond handler, and a second consumer
joining after partition 0 processes 16 records. Both sides use the sticky
assignor. It runs in a Linux container limited to two CPUs, with GOMAXPROCS=2.
The following three runs have race instrumentation disabled. These are small
backlog/handler-completion measurements, not steady-state capacity or a fully
controlled comparison of commit policies: eager commits asynchronously every
10 ms; cooperative commits each processed batch synchronously.

| Measurement | Eager/sticky | Cooperative |
| --- | ---: | ---: |
| Unique records per run | 960 | 960 |
| Handler throughput, records/s | 2,551–3,356 | 1,360–1,406 |
| Backlog drain after join | 250–331 ms | 632–660 ms |
| Partition 0 maximum processing gap | 13–64 ms | 38–51 ms |
| Duplicate handler deliveries | 96–144 | 5–15 |
| Duplicate payload bytes | 12,288–18,432 | 640–1,920 |

Cooperative reduced replay in these runs, but did **not** improve throughput or
consistently improve the processing gap. It constructed six initial workers and
three workers on the joining member, with no replacement workers for retained
grants. Total fetched work was 968–976 records / 123,904–124,928 payload bytes;
fetched but cancelled records are counted even if never delivered to a handler.
The benchmark stops after all unique records reach handlers; the cluster test
separately waits for every committed consumer offset to reach the log end.

The metadata recovery test at 120 partitions replayed four records, 61,168
payload bytes and 240 grants, while preserving 60 retained grants and completing
60 transfers. Twenty unchanged heartbeats appended no checkpoint. The isolated
recovery benchmark measured 29,944 recovery bytes at 120 grants (0.489 ms/op,
159,976 allocated bytes/op) and 3,050,824 recovery bytes at 12,000 grants
(54.96 ms/op, 21,406,353 allocated bytes/op). Ownership checkpoints remain O(P);
cooperative membership alone is not a recovery-byte optimization.

Local Linux race validation passed for server, coordinator, controller,
transaction, cluster controller, replication, FSM and Go SDK packages. Fault
coverage also includes blocked-handler revocation, delayed/duplicate ACKs,
stale fetch/commit/transaction tokens, commit/abort reservation barriers,
mixed-broker activation refusal, versioned restore and downgrade refusal.
Java and Python are deliberately outside this acceptance run.


The final three-broker run used six replicated partitions and continuous
idempotent publishing (`acks=all`). It passed all phases and drained all six
consumer offsets to their log ends before shutdown. Phase times include test
readiness checks and waiting for three further partition-0 deliveries, not just
broker election time; this local run overlapped static/race validation and is
not a production recovery SLO.

| Fault/transition | Observed time to phase completion |
| --- | ---: |
| Second consumer join / three partitions moved | 7.551 s |
| Client process killed / three workers rebuilt after expiration | 13.531 s |
| Coordinator stopped / surviving cluster resumes processing | 13.201 s |
| Coordinator started / processing resumes | 27.844 s |
| Rolling restart of brokers 1 / 2 / 3 | 32.291 / 27.692 / 8.678 s |

Across the run, the main consumer constructed 12 workers: six initially, three
after graceful leave, and three after the crashed member expired. Coordinator
and broker restarts added none. Its maximum partition-0 processing gap was
10.552 s; it fetched/delivered 258 records and 34,526 payload bytes. The main
consumer plus graceful peer observed one duplicate / 133 replay payload bytes.
The killed subprocess's deliveries are excluded from those duplicate counters;
no end-to-end exactly-once claim is made. Broker replication/recovery traffic is
not included in fetched payload bytes.

## Reconnect duplicate investigation (2026-10-24)

The historical CI counts, zero on `6e78a23` and 16 / 2,144 replay payload
bytes on `9f04d42`, did not contain per-offset attribution. They cannot establish
that all 16 duplicates had the same cause. The instrumented three-broker test
now records phase, partition, offset, payload bytes, original/new member and
grant, rejects duplicates within an unchanged grant, and bounds replay on each
transferred partition to one configured batch. Delivered/fetched totals now
include the graceful peer as their scope label promises; the old totals only
included the parent. Duplicate counters already included both. Killed-child
deliveries remain excluded.

A same-harness local run reproduced one same-grant duplicate / 134 bytes on
`6e78a23`, and four / 536 bytes on `9f04d42`, all during coordinator restart.
Both failed the new assertion despite draining every committed offset to log
end. The counts are timing-sensitive; the bug is not harmless timing variation.
Both polling's fresh connection and streaming's initialization ignored an
explicit cooperative fetch position whenever a committed offset existed.
Reconnecting between handling a batch and completing its commit could therefore
read that batch again. The committed-read window introduced an additional
reproducible exposure while a write was already in flight.

The controlled regression holds commit 16 at the durable writer while a new
connection requests position 16 under the original grant and committed offset
8. Three repetitions in each mode showed `6e78a23` waiting for commit completion
then returning 16, but `9f04d42` returning eight records starting at 8 before the
commit completed. This distinguishes a specific behavioral regression from the
pre-existing reconnect bug. The test records replay work and payload bytes,
and also asserts that the held commit cannot acknowledge early.

### Kafka comparison and selected changes

[Kafka 4.1.0 AbstractFetch](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractFetch.java)
constructs partition fetch requests from the current `FetchPosition`, separate
from committed group offsets. Cursus adopts that separation for cooperative
resume positions. It retains its existing committed-offset lower bound,
connection cursor, transaction-reservation lookup on every read, HWM/LSO and
retention behavior, and final original-grant validation. Eager cursor semantics
are unchanged. Reading ahead never publishes a commit; the Go worker still
waits for synchronous durable success before handling the next batch. A new
owner still starts from the committed position and may replay uncommitted work.

[Kafka 4.1.0 OffsetMetadataManager](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java)
validates the group for a commit request before generating its partition records.
Cursus resolves the single batch topic's lifecycle once and leaves original
registration/session/grant validation for every partition in
`CommitCooperativeBatch`, under the same coordinator fence held through durable
append. This removes repeated preliminary group lock acquisitions and topic
lookups; it does not remove the authoritative ownership validation or change
snapshot formats, all-or-nothing rejection, reservations or synchronous ACKs.

Regression checks cover polling/streaming reconnect during a held commit,
explicit and cached cursor precedence, unchanged eager behavior, reads beyond
log end without advancing a commit, transaction reservations, and rejection of
an invalid second token without publishing the first partition. Existing stale
grant, delayed revocation ACK, failed append and response-loss/replay tests remain
required. Availability improves because a reconnect can read its correct next
position while the prior commit is pending; recovery avoids replaying already
handled batches and their bytes. Membership transfer and hard-crash replay are
still bounded at-least-once behavior, not exactly-once delivery.

An initialized Go worker also reuses its fetch position on reconnect rather
than sending another `FETCH_OFFSET` through the operations connection occupied
by a synchronous commit. The broker still consults the authoritative stable
offset/reservation on every CONSUME/STREAM read and validates ownership before
delivery. A new grant creates a new worker and performs the initial offset
lookup. A real-socket SDK regression holds the operations gate across a forced
data-connection loss and requires the next page at offset 16, with just one
initial offset lookup; polling and streaming each pass five race repetitions.
The SDK and server full race suites and `go vet` also pass with this change.

The broker cursor/validation version passed the full controller, server,
coordinator and SDK race suites and vet. Its instrumented local three-broker
fault run drained all partitions with zero duplicates/replay bytes and 12 parent
worker starts. Local restart timings were slow and variable; those are not used
as throughput evidence. The original same-grant failures are retained as failed
measurements rather than counted as passes.

A 96-case, six-rotation exploration compared baseline, cursor/batch-validation
with the existing 2 ms collection limit, and 4/8 ms limits. All passed the
original safety bounds. Longer collection did not give a consistent throughput
gain: 4 ms increased join duplicates to 3–24 (median 16.5), while 8 ms increased
the long-workload median partition-0 gap from 52.5 to 109 ms. Both candidates
are rejected. The production collection limit remains 2 ms, with early flush
when all workers are waiting. The selected implementation and eager control
are compared separately after the SDK reconnect change.

The final 48-case confirmation alternated baseline `9f04d42` and the selected
code six times, each with eager/cooperative and 960-record join/9,600-record
backlog workloads. All passed, keeping the original one-batch transfer bound
and zero duplicates without membership changes. Durable backlog throughput
median was **2,657.8 → 2,744.4 records/s (+3.26%)**; the median of per-run
cooperative/eager ratios was **67.51% → 69.93%**. Join throughput was
2,169.45 → 2,193.65 records/s (+1.12%), with its paired ratio essentially
unchanged at 69.87%. These are modest local observations: ranges overlap and the
host is shared, so they do not establish a guaranteed capacity increase.

Long-workload commit requests fell from a median 254.5 to 212, cumulative worker
commit wait from 8,398.5 to 7,916 ms, and metadata replay payload from 88,861.5
to 74,512 bytes (257.5 → 215 records). All long cases fetched exactly 1,228,800
payload bytes with zero duplicates. Join duplicates were 3–22 for the selected
code, 3–24 for baseline, and 144 for its eager control; normal membership
handoff can still replay one uncommitted batch per moved partition. A held
operations-connection regression fails on the baseline and passes on the new
SDK, directly proving that reconnect no longer waits on that extra offset query.

[Raw observations and fault attribution](../measurements/consumer-cooperative-resume-2026-10-24.json)
include all 96 exploratory and 48 confirmation cases, rejected settings, the
older/current/fixed cluster outcomes and controlled regressions. As before,
cooperative commits are synchronous while eager uses a 10 ms asynchronous
interval. Test completion waits for every unique handler delivery and all
committed offsets to reach log end. Own race/fault jobs finished before capacity
measurement; other sessions' jobs were present on the shared Windows/WSL host.
