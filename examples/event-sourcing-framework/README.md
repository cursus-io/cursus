# Event-sourcing framework example

This example demonstrates the client-side framework API. It does not add a database to Cursus.

## Start the broker

From the repository root:

```bash
docker compose -f test/e2e/docker-compose.yml up -d --build
```

## Run

```bash
go run ./examples/event-sourcing-framework
```

Expected output:

```text
aggregate=game-example-1 version=2 status=finished
```

The example uses:

- `sdk.EventEnvelope` for event identity and correlation metadata
- `sdk.EventStore` for Cursus stream access
- `sdk.AggregateRepository` for replay and optimistic versioned saves

The database-side inbox, outbox, projection, and Saga state store are intentionally left to the service that owns the aggregate. Those adapters are introduced in the next framework PRs.
