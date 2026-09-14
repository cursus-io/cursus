package sagapg

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

func TestPostgresSagaHistoryIntegration(t *testing.T) {
	dsn := os.Getenv("CURSUS_SAGA_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CURSUS_SAGA_POSTGRES_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, Migrate(context.Background(), db))
	verifySagaHistory(t, Transaction{DB: db, HistoryTopic: "observability.saga-history.v1"}, db)
}

func verifySagaHistory(t *testing.T, transaction sdk.SagaTransaction, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	id := "go-integration-" + time.Now().UTC().Format("20060102150405.000000000")
	manager, err := sdk.NewTransactionalSagaManager(sdk.SagaDefinition{
		Type: "go-integration",
		Handlers: map[string]sdk.SagaHandler{
			"OrderCreated": func(_ context.Context, state *sdk.SagaState, _ sdk.EventEnvelope) ([]sdk.Command, error) {
				state.Status, state.Step = sdk.SagaWaiting, "reserve"
				return []sdk.Command{{Type: "Reserve", Payload: "{}"}}, nil
			},
		},
	}, transaction, sdk.SagaHistoryOptions{EnvironmentID: "test", ServiceName: "orders"})
	require.NoError(t, err)
	err = manager.Handle(ctx, sdk.EventEnvelope{EventID: "event-" + id, EventType: "OrderCreated", AssociationKey: id, CorrelationID: id, AggregateType: "order", AggregateID: id, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: []byte("{}")})
	require.NoError(t, err)
	var history, outbox int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM cursus_saga_history WHERE saga_id=$1`, id).Scan(&history))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM cursus_saga_history_outbox WHERE topic_name=$1`, "observability.saga-history.v1").Scan(&outbox))
	require.Greater(t, history, 0)
	require.Greater(t, outbox, 0)
}
