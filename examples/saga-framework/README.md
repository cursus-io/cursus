# Saga framework example

This is a small in-memory demonstration of the client-side Saga API. Production applications should implement `InboxStore`, `SagaStore`, and `OutboxStore` with their own database.

```bash
go run ./examples/saga-framework
```

The broker does not own these tables. Cursus owns the event log and offsets; the application owns Saga state and local database transactions.


## Reliability features shown

The example also demonstrates the API concepts used by production adapters:

- EventEnvelope.AssociationKey selects the Saga instance explicitly.
- Command.EffectID identifies one logical side effect across retries.
- SagaState.Effects records effect status and attempts.
- StartCompensation, CompleteCompensation, and FailCompensation persist rollback progress.

The example store is still in memory. Replace it with a transactional database and unique keys before using this pattern in production.
