# Saga framework example

This is a small in-memory demonstration of the client-side Saga API. Production applications should implement `InboxStore`, `SagaStore`, and `OutboxStore` with their own database.

```bash
go run ./examples/saga-framework
```

The broker does not own these tables. Cursus owns the event log and offsets; the application owns Saga state and local database transactions.
