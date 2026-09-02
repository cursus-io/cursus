# Saga Framework

The Cursus Saga Manager is a client-side coordinator. It uses Cursus events and consumer offsets, but it does not add a database to the broker.

## Service-owned repository

Implement `sdk.SagaRepository` with the service's existing database. Its
`Transact` callback is the only durability boundary and receives an
`sdk.SagaTransaction` that must atomically:

- claim `(consumer, event_id)` exactly once
- compare-and-swap `SagaState.Version`
- enqueue commands with a unique command ID
- complete or fail the inbox item

If the callback returns an error, none of those writes may commit. Use a
serializable database transaction and unique constraints for inbox and outbox
identities.

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
}, sagaRepository)
```

The manager claims the event, loads the saga state, invokes the matching
handler, saves the state with CAS, enqueues returned commands, and completes
the inbox item in one transaction. A duplicate `event_id` is ignored.

## Transaction boundary

`SagaRepository.Transact` must commit state, inbox, and outbox together. A
publisher worker then sends pending outbox commands to Cursus. After the
command is durably acknowledged, call `AcknowledgeEffect` with the saga,
effect, and command identities.

Do not commit the Cursus consumer offset before the local transaction succeeds. Cursus broker transactions cannot atomically include an external service database.

## Failure and compensation

Handler errors increment `RetryCount`; the failed state and inbox failure
record commit together. A retry worker can redeliver the event. After a policy
limit, set the saga to `FAILED` or `COMPENSATING` and emit a compensating
command such as `RollbackPlayerElo`.

Run the example with:

```bash
go run ./examples/saga-framework
```


## Durable effects, association, and compensation

Each logical command-producing step should use a stable effect identity. Store effect state with the Saga state and enforce a database uniqueness constraint on (saga type, association key, effect ID).

The effect lifecycle is:

~~~text
ENQUEUED -> SUCCEEDED
        \-> FAILED -> retry or compensation
~~~

`ENQUEUED` means the command and saga state committed atomically; it does not
claim that the external effect succeeded. When a redelivery produces an effect
already marked ENQUEUED or SUCCEEDED, the Saga Manager skips the outbox insert.
The stable command ID is derived from saga type, saga ID, and effect ID.

~~~go
err := manager.AcknowledgeEffect(ctx, associationKey, effectID, commandID)
~~~

The command ID fences stale acknowledgements from older attempts.

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

The in-memory example is instructional only. Production repositories must
provide a real rollback-capable transaction, CAS on `SagaState.Version`, and
unique inbox and outbox constraints.
