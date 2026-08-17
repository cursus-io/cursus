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


## Durable effects, association, and compensation

Each logical command-producing step should use a stable effect identity. Store effect state with the Saga state and enforce a database uniqueness constraint on (saga type, association key, effect ID).

The effect lifecycle is:

~~~text
PENDING -> SUCCEEDED
        \-> FAILED -> retry or compensation
~~~

When a redelivery produces an effect already marked SUCCEEDED, the Saga Manager skips the outbox enqueue. The outbox adapter must also deduplicate by effect ID because a process may crash after publishing and before persisting SUCCEEDED.

Set EventEnvelope.AssociationKey when the Saga instance is not identified by the aggregate:

~~~go
event, err := sdk.NewEventEnvelope("game", "game-123", "GameFinished", payload)
event.AssociationKey = "membership:player-1"
~~~

Saga association resolution is AssociationKey, then CorrelationID, then AggregateID.

Compensation is durable and resumable:

~~~go
state, err := manager.StartCompensation(ctx, associationKey, "rollback-elo", cause)
if err != nil {
    return err
}
if err := rollback(ctx); err != nil {
    return manager.FailCompensation(ctx, associationKey, err)
}
return manager.CompleteCompensation(ctx, associationKey)
~~~

StartCompensation, CompleteCompensation, and FailCompensation persist the compensation step, status, attempt count, and last error. A restarted worker can load SagaState and resume the recorded step.

The in-memory example is instructional only. Production adapters must make inbox claim, saga state, effect state, and outbox enqueue part of one local transaction.
