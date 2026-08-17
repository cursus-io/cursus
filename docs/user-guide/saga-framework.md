# Saga Framework

The Cursus Saga Manager is a client-side coordinator. It uses Cursus events and consumer offsets, but it does not add a database to the broker.

## Service-owned stores

Implement these SDK interfaces with the service's existing database:

- `sdk.InboxStore` claims `(consumer, event_id)` exactly once
- `sdk.SagaStore` persists saga state and retry information
- `sdk.OutboxStore` stores commands in the same local transaction as state changes

The broker remains responsible for event storage and delivery. The service database remains responsible for local business consistency.

## Register a Saga

```go
manager, err := sdk.NewSagaManager(sdk.SagaDefinition{
    Type: "finish-game",
    Handlers: map[string]sdk.SagaHandler{
        "GameFinished": func(ctx context.Context, state *sdk.SagaState, event sdk.EventEnvelope) ([]sdk.Command, error) {
            state.Step = "update-elo"
            state.Status = sdk.SagaWaiting
            return []sdk.Command{
                {Type: "UpdatePlayerElo", Payload: `{"game_id":"game-1"}`},
            }, nil
        },
        "PlayerEloUpdated": func(ctx context.Context, state *sdk.SagaState, event sdk.EventEnvelope) ([]sdk.Command, error) {
            state.Status = sdk.SagaCompleted
            return nil, nil
        },
    },
}, inboxStore, sagaStore, outboxStore)
```

The manager claims the event in the inbox, loads the saga state, invokes the matching handler, saves the state, and enqueues returned commands. A duplicate `event_id` is ignored.

## Transaction boundary

For a database-backed implementation, save saga state, claim/complete inbox, and enqueue outbox commands in one local database transaction. A publisher worker then sends pending outbox commands to Cursus and marks them published.

Do not commit the Cursus consumer offset before the local transaction succeeds. Cursus broker transactions cannot atomically include an external service database.

## Failure and compensation

Handler errors increment `RetryCount` and are recorded through `InboxStore.Fail`. A retry worker can redeliver the event. After a policy limit, set the saga to `FAILED` or `COMPENSATING` and emit a compensating command such as `RollbackPlayerElo`.

Run the example with:

```bash
go run ./examples/saga-framework
```
