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
same token. Reads consult the authoritative coordinator, including when the
data leader differs. SDK delivery checks cancellation, and commit submission checks the worker
identity. Delayed operations keep their original grant; they never acquire the
replacement worker's authority. An already authorized operation may complete
while revocation is pending.

Existing eager commands keep their behavior. Activation is explicit and requires
capable registered processes and voters; admission must reject later incompatible
processes. Persist the activation barrier in a versioned controller snapshot.
An old SDK cannot silently downgrade a cooperative group. Binary downgrade is
not achieved by draining clients: retained new-format records/snapshots still
require capable readers. Java/Python clients and their measurements are separate
work; this change's end-to-end acceptance applies to the broker and Go SDK.

## Validation defined before implementation

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
registered broker and Raft voter. Activation requires lifecycle protocol 10 and
bootstrap recovery protocol 2. It is durable and rejects incompatible later
registrations. Cooperative groups cannot switch back to eager membership.

The Raft directory marker advances to 13 when this binary starts, including when
cooperative activation is disabled. Activated controller snapshots use version
13; existing cooperative ownership snapshots without activation remain version
12. Do not point an older binary at these directories. Draining clients or
turning off the feature flag does not make persisted state downgrade-compatible.

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
synchronously. This API does not use the eager consumer's `AutoCommitInterval`
background batching. With auto-commit disabled, call
`consumer.Commit(ctx, record, record.Offset+1)` or
`producer.SendCooperativeOffsets(record, record.Offset+1)` inside a transaction.
Retain the delivered record's token for deferred work; never build a token from
current assignments. External side effects still require application idempotency.
A cancelled partially processed batch can be delivered again.

Membership and offset operations reuse separate coordinator connections, keeping
queued commits out of the heartbeat lane. Per-request deadlines and reconnects
apply to both. Failed partition requests and commits use exponential backoff
with jitter, starting at least at 100 ms and capped at about one second (or the
configured minimum if larger). Healthy polling retains its configured cadence.
Partition fetch connections retain their cursors until reconnect.

An ambiguous lifecycle append closes ownership admission until strict metadata
recovery resolves it. Standalone mode requires restart for that recovery; it
does not reuse a possibly persisted ownership revision. Transient coordinator
routing and metadata append outages remain retryable without weakening grant fences.

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


## Measured results

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
| Handler throughput, records/s | 2,201–2,225 | 670–765 |
| Backlog drain after join | 380–390 ms | 1,170–1,343 ms |
| Partition 0 maximum processing gap | 43–55 ms | 106–628 ms |
| Duplicate handler deliveries | 144 | 0–17 |
| Duplicate payload bytes | 18,432 | 0–2,176 |

Cooperative reduced replay in these runs, but did **not** improve throughput or
consistently improve the processing gap. It constructed six initial workers and
three workers on the joining member, with no replacement workers for retained
grants. Total fetched work was 960–984 records / 122,880–125,952 payload bytes;
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
