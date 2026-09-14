package sagamysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestMySQLSagaHistoryIntegration(t *testing.T) {
	dsn := os.Getenv("CURSUS_SAGA_MYSQL_DSN")
	if dsn == "" {
		t.Skip("CURSUS_SAGA_MYSQL_DSN is not set")
	}
	db, err := sql.Open("mysql", mysqlDSN(dsn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, Migrate(context.Background(), db))
	verifySagaHistory(t, Transaction{DB: db, HistoryTopic: "observability.saga-history.v1"}, db)
}

func mysqlDSN(dsn string) string {
	if !strings.HasPrefix(dsn, "mysql://") {
		return dsn
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	password, _ := u.User.Password()
	query := u.Query()
	query.Set("parseTime", "true")
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", u.User.Username(), password, u.Host, strings.TrimPrefix(u.Path, "/"), query.Encode())
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
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM cursus_saga_history WHERE saga_id=?`, id).Scan(&history))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM cursus_saga_history_outbox WHERE topic_name=?`, "observability.saga-history.v1").Scan(&outbox))
	require.Greater(t, history, 0)
	require.Greater(t, outbox, 0)
}
