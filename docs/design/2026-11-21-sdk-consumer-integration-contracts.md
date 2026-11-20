# Go SDK Consumer Integration Contracts

## Status

Approved in chat on 2026-11-21. This document records the design before
implementation. It does not describe behavior that is already available unless
the section explicitly says so.

The implementation starts from `4a43c70d9602e03f2a5413f37445e2cbe71624e1`,
the current `cursus-io/cursus` `main` commit when the work began.

## Problem

The Go consumer already discovers and caches its coordinator, follows bounded
redirect and rediscovery paths, resumes from broker-committed offsets, processes
each partition serially, and attempts a graceful `LEAVE_GROUP` during close.
Those mechanisms are necessary but not sufficient for a supervised production
application that owns its Prometheus endpoint and its durable processing
boundary.

The missing public contracts are:

1. register SDK collectors in a caller-owned Prometheus registry;
2. distinguish active group membership from the coarse `running` lifecycle;
3. deliver ordered partition batches whose durable next offset is committed by
   the application and fenced by the assignment that produced the batch;
4. reject a missing catalog before the ordinary first-join registration path;
5. provide bounded shutdown and document the consumer's one-shot lifecycle.

The design is deliberately application-neutral. Database transactions,
dead-letter storage, HTTP readiness routes, process retry policy, and supervisor
types remain application responsibilities.

## Non-goals and compatibility constraints

- Do not change Wire v2 commands, encodings, or broker behavior.
- Do not modify an application repository or cluster configuration.
- Do not introduce infinite retries, unbounded background catalog polling, or
  retries outside the existing attempt and deadline budgets.
- Do not add topic, group, member, or other unbounded labels.
- Preserve the coordinator cache and rediscovery behavior and these metric
  families exactly:
  - `cursus_consumer_coordinator_commands_total`
  - `cursus_consumer_coordinator_redirects_total`
  - `cursus_consumer_coordinator_resolution_total`
  - `cursus_consumer_coordinator_cache_refresh_total`
  - `cursus_consumer_coordinator_retry_total`
  - `cursus_consumer_coordinator_resolution_duration_seconds`
- Preserve `EnableMetrics`, `MetricsHandler`, `Start`, automatic commit, and the
  existing two-argument `CommitOffset` API.
- Do not promise exactly-once processing across an external store and Cursus.
  A process failure after the external side effect and before the offset commit
  produces an intentional redelivery.

## Apache Kafka comparison

The comparison uses Kafka 4.1.0 source and accepted Kafka Improvement Proposals
as the primary references:

- [`KafkaConsumer.java` at 4.1.0](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java)
  distinguishes the consumed position from the durable committed position,
  exposes synchronous commits, and documents that the committed value is the
  next record to process.
- [`ConsumerRecords.java` at 4.1.0](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerRecords.java)
  groups records by partition and exposes next offsets.
- [KIP-429](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/103090108/KIP-429%2BKafka%2BConsumer%2BIncremental%2BRebalance%2BProtocol)
  distinguishes assigned, revoked, and lost partitions. Lost ownership is not
  a safe commit boundary.
- [`ClassicKafkaConsumer.java` at 4.1.0](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ClassicKafkaConsumer.java)
  applies an overall duration to close and attempts group departure within the
  remaining budget.

### Adopt

- Treat the durable offset as the next record to process.
- Expose the current membership and assignment independently of the object
  lifecycle.
- Reject a commit that was created by ownership lost during a rebalance.
- Deliver partition-ordered records and serialize a handler per partition.
- Bound synchronous commit and close operations by caller or configured
  deadlines.

### Differ because of Cursus architecture

- Continue to use the existing Cursus `JOIN_GROUP`, `SYNC_GROUP`,
  `FETCH_OFFSET`, `BATCH_COMMIT`, and `LEAVE_GROUP` commands. Do not reproduce
  Kafka protocol objects or broker transactions.
- Keep Cursus coordinator redirect, address cache, and rediscovery behavior.
- Use a snapshot instead of a rebalance callback. A snapshot cannot block the
  consumer hot path and does not require a notification goroutine or channel
  delivery policy.
- Use the Cursus broker generation together with an SDK assignment generation.
  The latter fences a batch across local worker replacement even if a later
  broker assignment includes the same partition.
- Make existing-catalog validation opt-in. Existing development and bootstrap
  workflows retain the current first-join behavior.

### Safety, availability, and recovery cost

- A membership snapshot becomes ready only after join, sync, durable offset
  reads, and assignment installation. This delays readiness but prevents an
  application from accepting work while it owns no partition.
- A stale commit fails instead of acknowledging work under a replacement
  assignment. Availability is traded for at-least-once safety.
- A handler or external-store failure leaves the durable offset unchanged. A
  restart reads every encoded message byte from the last committed next offset
  through the failed batch again, and repeats the corresponding application
  work. Tests must report or assert offsets and payload bytes where practical,
  not only request counts.
- Existing-catalog validation adds one bounded `METADATA`, coordinator
  resolution, and `GROUP_STATUS` exchange at startup when enabled. It adds no
  steady-state polling.
- The broker currently permits a first `JOIN_GROUP` to register a missing
  group. A client-side `GROUP_STATUS` preflight prevents ordinary accidental
  creation but cannot atomically prevent an administrator from deleting the
  group between validation and join. Eliminating that time-of-check/time-of-use
  race would require a protocol or broker change and is outside this change.

## Public API

### Caller-owned metrics

```go
type Metrics struct {
    // unexported collector and registry fields
}

func NewMetrics(
    registerer prometheus.Registerer,
    gatherer prometheus.Gatherer,
) (*Metrics, error)

func (m *Metrics) Handler() http.Handler
```

`ConsumerConfig` gains:

```go
Metrics *Metrics `yaml:"-" json:"-"`
```

A non-nil `Metrics` enables metrics for that consumer. `EnableMetrics=true`
with a nil `Metrics` selects the existing SDK default metrics object. A caller
may therefore migrate without changing the existing configuration field.

`NewMetrics` registers the complete existing SDK collector set. It uses
`prometheus.AlreadyRegisteredError` to adopt a compatible collector already in
the same registry. An incompatible existing collector returns an error; the API
never calls `MustRegister` on a caller registry. Two `Metrics` values created
for the same registry and multiple consumers sharing either value use the same
collector vectors, so a label set has exactly one series.

`Metrics.Handler` serves the supplied gatherer. Existing application collectors
and SDK collectors can therefore share one endpoint. `MetricsHandler()` keeps
serving the SDK default registry.

No metric name or label set changes in this work.

### Membership snapshot

```go
type ConsumerMembership struct {
    Ready                bool
    Generation           int64
    AssignmentGeneration uint64
    MemberID             string
    Partitions           []int
}

func (c *Consumer) Membership() ConsumerMembership
```

The returned partition slice is a sorted defensive copy. The snapshot is safe
to call concurrently with start, rebalance, commit, failure, and close.

State transitions are:

```text
new                                      -> ready=false
JOIN_GROUP success only                  -> ready=false
SYNC_GROUP + non-empty assignment
  + committed offsets installed          -> ready=true
rebalance/member loss/assignment loss    -> ready=false, empty active fields
rejoin + recovered assignment            -> ready=true with a new token
Close/CloseContext begins                -> ready=false, empty active fields
```

The consumer always confirms the join through `SYNC_GROUP`, including when
`JOIN_GROUP` includes assignments. The sync result is authoritative for the
public readiness transition. This changes only SDK command sequencing, not the
wire contract or broker behavior.

`Ready` means that this consumer can process at least one currently owned
partition. It is not a process health, dependency health, or HTTP readiness
policy.

### Ordered manual batches

```go
type ConsumerBatch struct {
    Partition            int
    Messages             []Message
    NextOffset           uint64
    Generation           int64
    AssignmentGeneration uint64

    // unexported consumer identity and commit token
}

type BatchHandler func(context.Context, ConsumerBatch) error

func (c *Consumer) StartBatch(handler BatchHandler) error
func (c *Consumer) CommitBatch(ctx context.Context, batch ConsumerBatch) error
```

`StartBatch` requires `EnableAutoCommit=false` and a non-nil handler. It is a
one-shot alternative to `Start`; the lifecycle guard prevents calling both.
Messages are ordered by offset, all belong to `Partition`, and `NextOffset` is
one past the last message. The SDK invokes at most one batch handler at a time
for a partition; different partitions may run concurrently.

The SDK never commits a `StartBatch` batch automatically. Handler success,
handler failure, and handler-context cancellation all leave commit ownership
with the application. The application calls `CommitBatch` only after its
durable side effect completes. A batch-handler error stops the consumer with a
typed handler error and leaves the durable offset unchanged; the supervisor may
create a new consumer and receive the batch again. `HandlerMaxRetries` remains
the compatibility behavior of the record-oriented `Start` path and is not
silently applied to an external side effect in `StartBatch`.

The unexported token prevents callers from constructing a commit-capable batch.
`CommitBatch` checks consumer identity, broker generation, member identity,
assignment generation, current partition ownership, lifecycle state, and the
batch next offset before sending the existing commit command. The supplied
context is additionally bounded by `RequestTimeoutMS`.

The error contract is:

```go
var ErrConsumerManualCommitRequired = errors.New("consumer batch mode requires auto commit to be disabled")
var ErrConsumerStaleGeneration = errors.New("consumer batch generation is stale")

type ConsumerCommitError struct {
    Partition         int
    NextOffset        uint64
    BatchGeneration   int64
    CurrentGeneration int64
    Cause             error
}

func (e *ConsumerCommitError) Error() string
func (e *ConsumerCommitError) Unwrap() error
```

`errors.Is(err, ErrConsumerRebalancing)` is true when a commit races an active
rebalance. `errors.Is(err, ErrConsumerStaleGeneration)` is true after ownership
has moved to a different assignment or a broker rejects stale membership. The
typed error preserves the safe, bounded diagnostic fields and does not expose
topic, group, or member labels.

Existing `Start(func(Message) error)` retains record-level retry and automatic
commit behavior. Existing `CommitOffset(partition int, offset uint64) error`
retains its signature and behavior for callers that manage their own record
loop. New batch consumers use `CommitBatch` because its assignment token closes
the stale-work race that the legacy two-argument method cannot represent.

### Existing-catalog validation

`ConsumerConfig` gains:

```go
RequireExistingCatalog bool `yaml:"require_existing_catalog" json:"require_existing_catalog"`
```

When true, `Start` and `StartBatch` perform one bounded preflight before
`JOIN_GROUP`:

1. `METADATA topic=<topic>` must find the configured topic.
2. The SDK resolves the existing coordinator through the normal cache and
   rediscovery path.
3. `GROUP_STATUS group=<group>` must find the durable group and must report the
   configured topic.

A missing or mismatched catalog returns a typed error and sends no
`JOIN_GROUP`. When false, current bootstrap compatibility is unchanged. The
preflight does not create topics or groups and does not add a background poll.

The normal post-membership `FETCH_OFFSET` remains the source of truth, so an
existing group resumes from its broker-committed next offsets.

### Bounded lifecycle

```go
func (c *Consumer) CloseContext(ctx context.Context) error
func (c *Consumer) Close() error
```

`CloseContext` marks membership not ready before cancellation, stops assignment
work, attempts `LEAVE_GROUP`, closes active connections, drains commit and
lifecycle workers, and returns within the caller's deadline. `Close` supplies a
default overall deadline derived from `RequestTimeoutMS`. Concurrent close
callers observe the same terminal result.

All SDK-owned loops and connections must terminate. Go cannot forcibly stop an
arbitrary user callback. A `StartBatch` handler receives the assignment context
and must return on cancellation; the legacy `Start` handler retains its existing
obligation not to block forever. If user code ignores cancellation,
`CloseContext` returns its deadline error rather than blocking indefinitely.

A `Consumer` is one-shot. After `Start` or `StartBatch` returns, a supervisor
creates a new `Consumer` for another attempt. The SDK does not run an unbounded
internal restart loop.

## Internal structure

- Replace package-global consumer metric access with a `*Metrics` selected at
  construction. Retain a lazy default object for producer and compatibility
  paths.
- Add a dedicated membership mutex and central transition helpers. Do not send
  readiness events from fetch or processing hot paths.
- Generalize the existing per-partition private batch worker to select either a
  record handler or batch handler. Do not start a second worker for the same
  partition.
- Add an error-returning synchronous commit path. Automatic commit may adapt
  its existing boolean/retry behavior around that path; manual batch commit
  returns the underlying typed error and never enters the background retry
  queue after ownership is stale.
- Perform the optional catalog preflight through existing connection,
  coordinator-routing, TLS, authentication, and timeout helpers.
- Make close stages consume one overall context budget. Never replace the
  existing finite request attempts with a retry-until-close loop.

## Test plan

### Metrics

- Create two `Metrics` values against one registry and attach multiple
  consumers; assert registration does not panic and each descriptor/label set
  appears once.
- Register an application collector and SDK collectors in one registry, gather
  both through `Metrics.Handler`, and assert both families are exposed.
- Assert `EnableMetrics=true` plus `MetricsHandler()` retains the default
  compatibility behavior.
- Assert the six coordinator metric names and their existing label names do not
  change.

### Membership

- Drive initial, join-only, sync/assignment, rebalance/loss, recovered
  assignment, and close transitions.
- Read snapshots concurrently with transitions under `go test -race`.
- Assert returned partition slices cannot mutate internal state.
- Assert no-assignment membership is not ready.

### Batch and commit

- Successful ordered batch and explicit next-offset commit.
- Handler error and context cancellation produce no commit.
- Simulated external-store failure produces no commit; a replacement consumer
  starts from the prior durable offset and receives the same encoded payload
  bytes.
- A stale generation and active rebalance return errors matching the documented
  sentinels.
- Two batches for one partition never overlap; batches for separate partitions
  may execute concurrently.
- Legacy `Start`, automatic commit, and two-argument `CommitOffset` tests remain
  unchanged and pass.

### Catalog and lifecycle

- Missing topic, missing group, and topic/group mismatch send no `JOIN_GROUP`
  when `RequireExistingCatalog` is true.
- Existing durable group resumes from the committed next offset.
- Close before start, during join, during assignment, during a cooperative
  batch handler, and after failure stays within its deadline and attempts leave
  when membership exists.
- Run goroutine leak checks for every SDK-owned worker and blocked network test
  fixture. Run the focused SDK suite with the race detector.

### Three-broker integration

Use the existing cluster fixture and preserve its retry budgets while covering:

- bootstrap address already owns the coordinator;
- redirect to a different advertised address;
- redirect that resolves to the same usable address;
- redirect without an address, requiring rediscovery;
- stale cache refresh followed by a successful join, sync, offset fetch, and
  commit;
- readiness loss and recovery across coordinator movement;
- durable manual-batch redelivery after a stopped consumer.

The integration assertions include command bounds, final cache source,
committed and redelivered offsets, message counts, and redelivered payload
bytes.

### Regression commands

The implementation plan will select exact repository scripts after inspecting
their prerequisites. At minimum it will run:

```text
go test ./sdk/...
go test -race ./sdk/...
go test ./...
```

It will also run the existing SDK integration and repository E2E suites that
the local Docker environment supports, followed by all required PR checks.

## Documentation plan

Update the Go SDK overview and API reference with:

- caller-owned registry construction and a shared application `/metrics`
  endpoint;
- membership snapshot semantics and the difference from `Consumer.State()`;
- ordered manual batches, explicit commit, redelivery, and idempotent external
  side effects;
- existing-catalog validation and its client-side race limitation;
- one-shot consumer lifecycle and a generic supervisor example that creates a
  new consumer for each bounded attempt;
- exact compatibility behavior for `EnableMetrics`, `MetricsHandler`, `Start`,
  automatic commit, and legacy `CommitOffset`.

The example may describe a Commerce-style workload, but no Commerce package,
metric, database interface, health route, or retry policy becomes SDK API.

## Release handoff

After review and CI, report:

- the exact public signatures shipped;
- readiness and commit semantics;
- compatibility behavior;
- local tests and CI results;
- the ready-for-review PR URL;
- the final commit SHA and Go pseudo-version calculated from its UTC commit
  timestamp and 12-character SHA suffix;
- application integration cautions, especially at-least-once redelivery,
  handler cancellation, caller-owned supervisor retries, and the catalog
  preflight race.

The PR remains unmerged for the user.
