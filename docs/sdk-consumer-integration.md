# Go SDK consumer integration contracts

This document describes the application-facing contracts for running the Go
consumer under a service supervisor. The contracts are generic: database
transactions, dead-letter storage, HTTP policy, retry classification, and
process supervision remain application responsibilities.

The implementation preserves Wire v2 commands, broker behavior, coordinator
cache and rediscovery behavior, and the existing retry and request-deadline
budgets.

## Public API

```go
func NewMetrics(
    registerer prometheus.Registerer,
    gatherer prometheus.Gatherer,
) (*Metrics, error)

func (m *Metrics) Handler() http.Handler

type ConsumerMembership struct {
    Ready                bool
    Generation           int64
    AssignmentGeneration uint64
    MemberID             string
    Partitions           []int
}

func (c *Consumer) Membership() ConsumerMembership

type ConsumerBatch struct {
    Partition            int
    Messages             []Message
    NextOffset           uint64
    Generation           int64
    AssignmentGeneration uint64
}

type BatchHandler func(context.Context, ConsumerBatch) error

func (c *Consumer) StartBatch(handler BatchHandler) error
func (c *Consumer) CommitBatch(ctx context.Context, batch ConsumerBatch) error
func (c *Consumer) CloseContext(ctx context.Context) error
```

`ConsumerConfig` adds these fields:

```go
Metrics                *Metrics
RequireExistingCatalog bool
```

## Caller-owned Prometheus registry

Use one application registry for application and SDK collectors:

```go
registry := prometheus.NewRegistry()
registry.MustRegister(applicationCollector)

metrics, err := sdk.NewMetrics(registry, registry)
if err != nil {
    return err
}

cfg := sdk.NewDefaultConsumerConfig()
cfg.Metrics = metrics
consumer, err := sdk.NewConsumer(cfg)

http.Handle("/metrics", metrics.Handler())
```

Multiple `Metrics` values and multiple consumers may use the same registry.
The SDK adopts an already registered compatible collector instead of calling
`MustRegister`, so this does not panic and does not create duplicate series.
The existing coordinator metric names and bounded labels remain unchanged:

- `cursus_consumer_coordinator_commands_total`
- `cursus_consumer_coordinator_redirects_total`
- `cursus_consumer_coordinator_resolution_total`
- `cursus_consumer_coordinator_cache_refresh_total`
- `cursus_consumer_coordinator_retry_total`
- `cursus_consumer_coordinator_resolution_duration_seconds`

No topic, group, member, or other unbounded coordinator label is added.

Compatibility behavior is unchanged when `Metrics` is nil:

- `EnableMetrics=true` selects the SDK default registry.
- `sdk.MetricsHandler()` serves that default registry.
- `EnableMetrics=false` leaves consumer instrumentation disabled.

## Membership readiness

`Membership` returns a concurrency-safe snapshot. Its `Partitions` slice is a
copy and may be inspected or modified by the caller without racing the SDK.

`Ready` has a narrower meaning than `Consumer.State() == ConsumerStateRunning`:

1. A new consumer reports `Ready=false`.
2. A successful `JOIN_GROUP` alone is not ready.
3. A successful `SYNC_GROUP` plus installation of at least one assigned
   partition publishes `Ready=true`.
4. Rebalance start, coordinator/member fencing, assignment loss, fatal
   failure, or shutdown start publishes `Ready=false`.
5. A later join, sync, and non-empty assignment publishes `Ready=true` with a
   new generation and assignment generation.

The snapshot is lock-protected and does not add an event send to polling,
streaming, or message-handler hot paths. It is a group-ownership signal, not a
complete service health policy. An application may combine it with database,
dead-letter, and HTTP-server health:

```go
snapshot := consumer.Membership()
if !snapshot.Ready {
    return errors.New("consumer has no active assignment")
}
log.Printf("generation=%d assignment=%d partitions=%v",
    snapshot.Generation, snapshot.AssignmentGeneration, snapshot.Partitions)
```

## Ordered batch and manual commit

Select batch mode only with automatic commit disabled:

```go
cfg.EnableAutoCommit = false

err := consumer.StartBatch(func(ctx context.Context, batch sdk.ConsumerBatch) error {
    if err := databaseTransaction(ctx, batch.Messages); err != nil {
        return err
    }
    if err := storeDeadLetters(ctx, batch.Messages); err != nil {
        return err
    }
    return consumer.CommitBatch(ctx, batch)
})
```

Each batch contains ordered records from exactly one partition and the next
offset after its final record. A partition has one batch handler at a time;
different assigned partitions may run concurrently. The handler context is
canceled when the assignment is lost or shutdown starts.

The SDK does not commit when the handler returns an error or its context is
canceled. The next consumer therefore resumes from the last broker-committed
offset and may redeliver the batch. Applications must make database and
dead-letter effects idempotent or transactionally deduplicate them.

`CommitBatch` sends `batch.NextOffset` synchronously and advances the local
committed offset only after broker acknowledgement. It validates the private
batch provenance, broker generation, member identity, local assignment
generation, and partition ownership. Inspect failures with `errors.Is` and
`errors.As`:

```go
err := consumer.CommitBatch(ctx, batch)
switch {
case errors.Is(err, sdk.ErrConsumerRebalancing):
    // Ownership is being replaced; do not retry this batch token.
case errors.Is(err, sdk.ErrConsumerStaleGeneration):
    // This batch belongs to an old generation; do not commit it.
}

var commitErr *sdk.ConsumerCommitError
if errors.As(err, &commitErr) {
    log.Printf("partition=%d next=%d batch-generation=%d current=%d",
        commitErr.Partition,
        commitErr.NextOffset,
        commitErr.BatchGeneration,
        commitErr.CurrentGeneration,
    )
}
```

`CommitOffset(partition, nextOffset)` remains available for existing message
handlers. `CommitBatch` is the safer batch API because the SDK-generated batch
token fences delayed commits from a previous assignment.

## Existing catalog and durable resume

Set `RequireExistingCatalog=true` when an application must not create a topic
or durable group implicitly. Before `JOIN_GROUP`, the SDK performs bounded
`METADATA` and `GROUP_STATUS` checks. A missing topic returns the existing
topic error, a missing group matches `ErrConsumerGroupNotFound`, and a group
whose durable topic selector does not include the configured topic matches
`ErrConsumerCatalogMismatch`.

The preflight issues no create command. A successful start joins the existing
group and fetches its committed next offsets before installing assignments, so
restart resumes from durable broker state.

This is a fail-closed client preflight, not an atomic catalog lease. A topic or
group can still be deleted after validation and before join. Closing that race
would require a broker or wire-protocol change and is outside this contract.

## Lifecycle and supervisor

A `Consumer` is one-shot. Do not call `Start` or `StartBatch` again after it
returns. A supervisor must construct a new consumer for each attempt while
reusing the caller-owned `Metrics` object:

```go
for ctx.Err() == nil {
    attemptCfg := *baseConfig
    consumer, err := sdk.NewConsumerWithContext(ctx, &attemptCfg)
    if err == nil {
        err = consumer.StartBatch(handlerFor(consumer))
    }
    if ctx.Err() != nil {
        break
    }
    log.Printf("consumer stopped: %v", err)
    if !waitForRetry(ctx) {
        break
    }
}
```

`Close()` uses `RequestTimeoutMS` as its shutdown bound. `CloseContext` accepts
an earlier application deadline. Shutdown immediately clears membership
readiness, cancels assignment work, closes active sockets, drains cooperative
SDK workers, flushes automatic offsets when enabled, and attempts
`LEAVE_GROUP` within the remaining deadline. Batch handlers must observe their
context and return; Go cannot forcibly terminate application callback code.

See the complete, compilable
[consumer supervisor example](../examples/consumer-supervisor/main.go) for a
shared `/metrics` endpoint, readiness handler, manual DB/offset boundary, and
fresh-consumer retry loop.

## Compatibility summary

| Existing API | Behavior |
| --- | --- |
| `Start(func(Message) error)` | Unchanged message-at-a-time handler contract. |
| `EnableAutoCommit=true` | Existing automatic commit behavior remains the default. |
| `CommitOffset(partition, nextOffset)` | Existing two-argument manual commit API remains available. |
| `EnableMetrics=true` | Uses the existing default SDK registry. |
| `MetricsHandler()` | Continues to serve the existing default registry. |
| Coordinator cache, rediscovery, metrics | Existing names, labels, redirect handling, retry counts, and deadlines are preserved. |
| Wire v2 and broker commands | No command shape or broker behavior changes. |

`Metrics`, `Membership`, `StartBatch`, `CommitBatch`,
`RequireExistingCatalog`, and `CloseContext` are additive opt-in contracts.
