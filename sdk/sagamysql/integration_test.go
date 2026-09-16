package sagamysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
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
	sagaID := verifySagaHistory(t, Transaction{DB: db, HistoryTopic: "observability.saga-history.v1"}, db)
	publisher := &recordingHistoryPublisher{failures: 1}
	worker := HistoryOutboxPublisher{DB: db, Publisher: publisher}
	_, err = worker.PublishPending(context.Background(), 1)
	require.ErrorIs(t, err, errHistoryBrokerUnavailable)
	require.Len(t, publisher.payloads, 1)
	firstID := historyEventID(t, publisher.payloads[0])
	_, err = db.Exec(`UPDATE cursus_saga_history_outbox SET status='PUBLISHED' WHERE history_event_id IN (SELECT history_event_id FROM cursus_saga_history WHERE saga_id=?) AND history_event_id<>?`, sagaID, firstID)
	require.NoError(t, err)
	count, err := worker.PublishPending(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Len(t, publisher.payloads, 2)
	require.Equal(t, firstID, historyEventID(t, publisher.payloads[1]))
	var status string
	var attempts int
	require.NoError(t, db.QueryRow(`SELECT status,attempts FROM cursus_saga_history_outbox WHERE history_event_id=?`, firstID).Scan(&status, &attempts))
	require.Equal(t, "PUBLISHED", status)
	require.Equal(t, 2, attempts)
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

var errHistoryBrokerUnavailable = errors.New("history broker unavailable")

type recordingHistoryPublisher struct {
	failures int
	payloads []string
}

func (p *recordingHistoryPublisher) PublishSagaHistory(_ context.Context, _ string, payload string) error {
	p.payloads = append(p.payloads, payload)
	if p.failures > 0 {
		p.failures--
		return errHistoryBrokerUnavailable
	}
	return nil
}

func historyEventID(t *testing.T, payload string) string {
	t.Helper()
	var event struct {
		HistoryEventID string `json:"history_event_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(payload), &event))
	require.NotEmpty(t, event.HistoryEventID)
	return event.HistoryEventID
}

func verifySagaHistory(t *testing.T, transaction sdk.SagaTransaction, db *sql.DB) string {
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
	var runID string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT run_id FROM cursus_saga_history WHERE saga_id=? ORDER BY sequence LIMIT 1`, id).Scan(&runID))
	_, err = db.ExecContext(ctx, `INSERT INTO cursus_saga_history (history_event_id,history_schema_version,environment_id,service_name,saga_type,saga_id,run_id,sequence,event_type,occurred_at,recorded_at) VALUES (?,1,'test','orders','go-integration',?,?,1,'run.started',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, uuid.NewString(), id, runID)
	require.Error(t, err, "the per-run sequence unique constraint must reject collisions")
	return id
}
