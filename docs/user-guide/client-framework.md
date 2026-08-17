# Cursus Client Framework

The Cursus broker provides the event log, aggregate stream version checks, snapshots, partition ordering, and consumer offsets. The client framework adds the application-facing event-sourcing API on top of those broker primitives.

## What belongs where

```text
Application client
  EventEnvelope, AggregateRepository, projections, Saga Manager
  inbox/outbox adapters and application database transactions

Cursus broker
  APPEND_STREAM, READ_STREAM, STREAM_VERSION, snapshots, consumer offsets
```

Cursus does not create or own a domain database. A service owns its `sagas`, `inbox`, `outbox`, and projection tables.

## Event envelope

Use `sdk.NewEventEnvelope` instead of constructing event metadata by hand.

```go
event, err := sdk.NewEventEnvelope(
    "game", "game-123", "GameFinished",
    map[string]any{"winner": "player-1"},
)
event.CorrelationID = "match-command-456"
```

The framework envelope contains `event_id`, `event_type`, `schema_version`, aggregate identity and version, `occurred_at`, correlation/causation IDs, and the JSON payload. The envelope is stored in the Cursus event stream payload, so older broker clients can continue to read the stream.

## Aggregate repository

An aggregate implements four small methods:

```go
type Aggregate interface {
    ID() string
    Type() string
    Version() uint64
    Apply(EventEnvelope) error
}
```

Create a repository with an `EventStore` and a factory:

```go
store := sdk.NewEventStore("localhost:9000", "games", "game-service")
repo, err := sdk.NewAggregateRepository(store, func(id string) sdk.Aggregate {
    return NewGame(id)
})
```

`Load` replays committed events. `Save` assigns the next aggregate version and uses the broker's optimistic concurrency check. A stale writer receives a version conflict and must reload, reapply its command, and retry.

## Client API layers

Use the lowest layer only when needed:

```text
EventStore          raw event stream and snapshot operations
EventEnvelope       common event metadata and serialization
AggregateRepository aggregate load/save and version handling
Saga Manager        command coordination, retries, and compensation (PR 2/3)
```

## Failure and retry rules

- Generate a new `event_id` for a new event and reuse it when retrying the same publish attempt.
- Do not commit a consumer offset before the projection database transaction succeeds.
- Record `event_id` in a projection inbox table to make handler retries safe.
- Use an outbox table when a database change and a Cursus publish must be coordinated.
- Broker transactions cover Cursus records and offsets, not external database side effects.

## Running the example

Start the standalone broker with the repository's Docker Compose environment, then run:

```bash
go run ./examples/event-sourcing-framework
```

The example creates an event-sourcing topic, saves `GameCreated` and `GameFinished`, reloads the aggregate, and prints the reconstructed state.
