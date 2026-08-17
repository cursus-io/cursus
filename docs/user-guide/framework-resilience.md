# Replay, Retry, Compensation, and Deadlines

PR 3 adds client-side resilience helpers. They are deliberately application-owned and do not add broker tables.

## Replay and upcasting

```go
registry := sdk.NewUpcasterRegistry()
registry.Register("GameFinished", 1, func(event sdk.EventEnvelope) (sdk.EventEnvelope, error) {
    event.SchemaVersion = 2
    return event, nil
})

err := sdk.Replay(store, "game-123", 1, registry, func(event sdk.EventEnvelope) error {
    return projection.Apply(event)
})
```

Replay reads committed stream events. Upcasters transform old immutable payloads at read time; the original event is never rewritten.

## Retry and compensation

```go
policy := sdk.RetryPolicy{
    MaxAttempts: 5,
    InitialDelay: time.Second,
    MaxDelay: 30 * time.Second,
    Multiplier: 2,
}

if !policy.ShouldRetry(state.RetryCount) {
    command := sdk.CompensationCommand("RollbackPlayerElo", state, event.EventID, `{}`)
    outbox.Enqueue(ctx, command)
}
```

The outbox publisher owns delivery retries. The Saga Manager records handler failures and retry counts; a worker decides when to retry or move to compensation.

## Deadlines

```go
deadlines := sdk.NewDeadlineManager()
deadlines.Schedule("saga-1:elo-timeout", time.Now().Add(time.Minute), func() {
    // Load state and enqueue a compensation command.
})
```

Call `RunDue` from a service worker. Persist deadlines in the service database when restart recovery is required.

## Client help

`sdk.FrameworkHelp()` provides a compact list of the framework entry points. Go uses explicit registration APIs rather than Java-style annotations; a future code generator can produce those registrations from project metadata without changing the broker protocol.
