//go:build legacy_sql_saga

package sagamysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cursus-io/cursus/sdk"
)

const maxSagaTransactionAttempts = 3

// Transaction is a MySQL 8+ implementation of the database-neutral Saga
// transaction interface. HistoryTopic is service configuration, never a
// Cursus internal/reserved-topic default.
type Transaction struct {
	DB           *sql.DB
	HistoryTopic string
}

func (t Transaction) WithinSagaTransaction(ctx context.Context, fn func(context.Context, sdk.SagaTransactionStores) error) error {
	if t.DB == nil {
		return fmt.Errorf("mysql saga database is required")
	}
	if t.HistoryTopic == "" {
		return fmt.Errorf("saga history topic is required and must be service configured")
	}
	var lastErr error
	for attempt := 1; attempt <= maxSagaTransactionAttempts; attempt++ {
		tx, err := t.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return fmt.Errorf("begin saga transaction: %w", err)
		}
		adapter := store{tx: tx, historyTopic: t.HistoryTopic}
		err = fn(ctx, sdk.SagaTransactionStores{Inbox: adapter, State: adapter, Outbox: adapter, History: adapter})
		// GET_LOCK is connection-scoped rather than transaction-scoped. Release
		// every named lock before returning this pooled database/sql connection.
		_, _ = tx.ExecContext(ctx, `SELECT RELEASE_ALL_LOCKS()`)
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
		if err == nil {
			return nil
		}
		if !isRetryableTransactionError(err) || attempt == maxSagaTransactionAttempts {
			return fmt.Errorf("complete saga transaction: %w", err)
		}
		lastErr = err
	}
	return fmt.Errorf("complete saga transaction after retry: %w", lastErr)
}

func isRetryableTransactionError(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "deadlock") || strings.Contains(text, "lock wait timeout") || strings.Contains(text, "sqlstate 40001")
}

type store struct {
	tx           *sql.Tx
	historyTopic string
}

func (s store) Claim(ctx context.Context, consumer, eventID string) (bool, error) {
	r, err := s.tx.ExecContext(ctx, `INSERT IGNORE INTO cursus_saga_inbox (consumer_name,event_id,status,last_error,updated_at) VALUES (?,?,'CLAIMED','',UTC_TIMESTAMP(6))`, consumer, eventID)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}
func (s store) Complete(ctx context.Context, consumer, eventID string) error {
	_, err := s.tx.ExecContext(ctx, `UPDATE cursus_saga_inbox SET status='COMPLETED',updated_at=UTC_TIMESTAMP(6) WHERE consumer_name=? AND event_id=?`, consumer, eventID)
	return err
}
func (s store) Fail(ctx context.Context, consumer, eventID string, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	_, err := s.tx.ExecContext(ctx, `INSERT INTO cursus_saga_inbox (consumer_name,event_id,status,last_error,updated_at) VALUES (?,?,'FAILED',?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE status='FAILED',last_error=VALUES(last_error),updated_at=VALUES(updated_at)`, consumer, eventID, message)
	return err
}
func (s store) Load(ctx context.Context, typ, id string) (*sdk.SagaState, error) {
	var locked int
	if err := s.tx.QueryRowContext(ctx, `SELECT GET_LOCK(SHA2(CONCAT(?,':',?),256),10)`, typ, id).Scan(&locked); err != nil {
		return nil, err
	}
	if locked != 1 {
		return nil, fmt.Errorf("acquire saga creation lock")
	}
	row := s.tx.QueryRowContext(ctx, `SELECT association_key,correlation_id,run_id,next_sequence,status,outcome,step_id,data,retry_count,last_error,effects,compensation,updated_at FROM cursus_saga_state WHERE saga_type=? AND saga_id=? FOR UPDATE`, typ, id)
	var v sdk.SagaState
	var effects, comp []byte
	if err := row.Scan(&v.AssociationKey, &v.CorrelationID, &v.RunID, &v.NextSequence, &v.Status, &v.Outcome, &v.Step, &v.Data, &v.RetryCount, &v.LastError, &effects, &comp, &v.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	v.ID, v.Type = id, typ
	if err := json.Unmarshal(effects, &v.Effects); err != nil {
		return nil, fmt.Errorf("decode saga effects: %w", err)
	}
	if len(comp) > 0 && string(comp) != "null" {
		var compensation sdk.CompensationState
		if err := json.Unmarshal(comp, &compensation); err != nil {
			return nil, fmt.Errorf("decode compensation: %w", err)
		}
		v.Compensation = &compensation
	}
	return &v, nil
}
func (s store) Save(ctx context.Context, v *sdk.SagaState) error {
	if v == nil {
		return fmt.Errorf("saga state is required")
	}
	effects, err := json.Marshal(v.Effects)
	if err != nil {
		return err
	}
	compensation, err := json.Marshal(v.Compensation)
	if err != nil {
		return err
	}
	if v.UpdatedAt.IsZero() {
		v.UpdatedAt = time.Now().UTC()
	}
	_, err = s.tx.ExecContext(ctx, `INSERT INTO cursus_saga_state (saga_type,saga_id,association_key,correlation_id,run_id,next_sequence,status,outcome,step_id,data,retry_count,last_error,effects,compensation,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE association_key=VALUES(association_key),correlation_id=VALUES(correlation_id),run_id=VALUES(run_id),next_sequence=VALUES(next_sequence),status=VALUES(status),outcome=VALUES(outcome),step_id=VALUES(step_id),data=VALUES(data),retry_count=VALUES(retry_count),last_error=VALUES(last_error),effects=VALUES(effects),compensation=VALUES(compensation),updated_at=VALUES(updated_at)`, v.Type, v.ID, v.AssociationKey, v.CorrelationID, v.RunID, v.NextSequence, v.Status, v.Outcome, v.Step, objectJSON(v.Data), v.RetryCount, v.LastError, string(effects), string(compensation), v.UpdatedAt)
	return err
}
func (s store) Enqueue(ctx context.Context, c sdk.Command) error {
	_, err := s.tx.ExecContext(ctx, `INSERT IGNORE INTO cursus_saga_outbox (command_id,saga_type,saga_id,effect_id,command_type,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,UTC_TIMESTAMP(6))`, c.ID, c.SagaType, c.SagaID, c.EffectID, c.Type, c.CorrelationID, c.CausationID, objectJSON(c.Payload))
	return err
}
func (s store) AppendSagaHistory(ctx context.Context, v sdk.SagaHistoryEvent) error {
	_, err := s.tx.ExecContext(ctx, `INSERT INTO cursus_saga_history (history_event_id,history_schema_version,environment_id,service_name,saga_type,saga_id,run_id,sequence,event_type,occurred_at,recorded_at,step_id,attempt,command_id,effect_id,source_event_id,correlation_id,causation_id,source_topic,source_partition,source_offset,aggregate_type,aggregate_id,aggregate_version,payload,error) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, v.HistoryEventID, v.HistorySchemaVersion, v.EnvironmentID, v.ServiceName, v.SagaType, v.SagaID, v.RunID, v.Sequence, v.EventType, v.OccurredAt, v.RecordedAt, v.StepID, v.Attempt, v.CommandID, v.EffectID, v.SourceEventID, v.CorrelationID, v.CausationID, v.SourceTopic, v.SourcePartition, v.SourceOffset, v.AggregateType, v.AggregateID, v.AggregateVersion, v.Payload, v.Error)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal saga history outbox payload: %w", err)
	}
	_, err = s.tx.ExecContext(ctx, `INSERT INTO cursus_saga_history_outbox (history_event_id,topic_name,payload,status,attempts,last_error,created_at) VALUES (?,?,?,'PENDING',0,'',UTC_TIMESTAMP(6))`, v.HistoryEventID, s.historyTopic, string(payload))
	return err
}
func objectJSON(v string) string {
	if v == "" {
		return "{}"
	}
	return v
}

var _ sdk.SagaTransaction = Transaction{}
