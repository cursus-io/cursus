// Package sagapg contains the service-owned PostgreSQL transaction adapter.
package sagapg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/jackc/pgx/v5/pgconn"
)

const DefaultHistoryTopic = "cursus.saga-history.v1"

// PostgreSQL reports both serializable snapshot conflicts and deadlocks as
// retryable transaction failures. Retrying the complete local transaction
// keeps a concurrently handled Saga on one durable sequence instead of
// turning a transient conflict into a spurious failed inbox record.
const maxSagaTransactionAttempts = 3

type Transaction struct {
	DB           *sql.DB
	HistoryTopic string
}

func (t Transaction) WithinSagaTransaction(ctx context.Context, fn func(context.Context, sdk.SagaTransactionStores) error) error {
	if t.DB == nil {
		return fmt.Errorf("postgres saga database is required")
	}
	var lastErr error
	for attempt := 1; attempt <= maxSagaTransactionAttempts; attempt++ {
		tx, err := t.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return fmt.Errorf("begin saga transaction: %w", err)
		}
		adapter := store{tx: tx, historyTopic: t.HistoryTopic}
		if adapter.historyTopic == "" {
			adapter.historyTopic = DefaultHistoryTopic
		}
		err = fn(ctx, sdk.SagaTransactionStores{Inbox: adapter, State: adapter, Outbox: adapter, History: adapter})
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
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "40001" || pgErr.Code == "40P01"
}

type store struct {
	tx           *sql.Tx
	historyTopic string
}

func (s store) Claim(ctx context.Context, consumer, eventID string) (bool, error) {
	r, e := s.tx.ExecContext(ctx, `INSERT INTO cursus_saga_inbox (consumer_name,event_id,status,updated_at) VALUES ($1,$2,'CLAIMED',NOW()) ON CONFLICT DO NOTHING`, consumer, eventID)
	if e != nil {
		return false, e
	}
	n, e := r.RowsAffected()
	return n == 1, e
}
func (s store) Complete(ctx context.Context, consumer, eventID string) error {
	_, e := s.tx.ExecContext(ctx, `UPDATE cursus_saga_inbox SET status='COMPLETED',updated_at=NOW() WHERE consumer_name=$1 AND event_id=$2`, consumer, eventID)
	return e
}
func (s store) Fail(ctx context.Context, consumer, eventID string, cause error) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	_, e := s.tx.ExecContext(ctx, `INSERT INTO cursus_saga_inbox (consumer_name,event_id,status,last_error,updated_at) VALUES ($1,$2,'FAILED',$3,NOW()) ON CONFLICT (consumer_name,event_id) DO UPDATE SET status='FAILED',last_error=EXCLUDED.last_error,updated_at=EXCLUDED.updated_at`, consumer, eventID, msg)
	return e
}
func (s store) Load(ctx context.Context, typ, id string) (*sdk.SagaState, error) {
	row := s.tx.QueryRowContext(ctx, `SELECT association_key,correlation_id,run_id::text,next_sequence,status,outcome,step_id,data,retry_count,last_error,effects,compensation,updated_at FROM cursus_saga_state WHERE saga_type=$1 AND saga_id=$2 FOR UPDATE`, typ, id)
	var v sdk.SagaState
	var effects, comp []byte
	if e := row.Scan(&v.AssociationKey, &v.CorrelationID, &v.RunID, &v.NextSequence, &v.Status, &v.Outcome, &v.Step, &v.Data, &v.RetryCount, &v.LastError, &effects, &comp, &v.UpdatedAt); e != nil {
		if e == sql.ErrNoRows {
			return nil, nil
		}
		return nil, e
	}
	v.ID, v.Type = id, typ
	if e := json.Unmarshal(effects, &v.Effects); e != nil {
		return nil, fmt.Errorf("decode saga effects: %w", e)
	}
	if len(comp) > 0 && string(comp) != "null" {
		var c sdk.CompensationState
		if e := json.Unmarshal(comp, &c); e != nil {
			return nil, fmt.Errorf("decode compensation: %w", e)
		}
		v.Compensation = &c
	}
	return &v, nil
}
func (s store) Save(ctx context.Context, v *sdk.SagaState) error {
	if v == nil {
		return fmt.Errorf("saga state is required")
	}
	effects, e := json.Marshal(v.Effects)
	if e != nil {
		return e
	}
	comp, e := json.Marshal(v.Compensation)
	if e != nil {
		return e
	}
	if v.UpdatedAt.IsZero() {
		v.UpdatedAt = time.Now().UTC()
	}
	_, e = s.tx.ExecContext(ctx, `INSERT INTO cursus_saga_state (saga_type,saga_id,association_key,correlation_id,run_id,next_sequence,status,outcome,step_id,data,retry_count,last_error,effects,compensation,updated_at) VALUES ($1,$2,$3,$4,$5::uuid,$6,$7,$8,$9,$10::jsonb,$11,$12,$13::jsonb,$14::jsonb,$15) ON CONFLICT (saga_type,saga_id) DO UPDATE SET association_key=EXCLUDED.association_key,correlation_id=EXCLUDED.correlation_id,run_id=EXCLUDED.run_id,next_sequence=EXCLUDED.next_sequence,status=EXCLUDED.status,outcome=EXCLUDED.outcome,step_id=EXCLUDED.step_id,data=EXCLUDED.data,retry_count=EXCLUDED.retry_count,last_error=EXCLUDED.last_error,effects=EXCLUDED.effects,compensation=EXCLUDED.compensation,updated_at=EXCLUDED.updated_at`, v.Type, v.ID, v.AssociationKey, v.CorrelationID, v.RunID, v.NextSequence, v.Status, v.Outcome, v.Step, objectJSON(v.Data), v.RetryCount, v.LastError, effects, comp, v.UpdatedAt)
	return e
}
func (s store) Enqueue(ctx context.Context, c sdk.Command) error {
	_, e := s.tx.ExecContext(ctx, `INSERT INTO cursus_saga_outbox (command_id,saga_type,saga_id,effect_id,command_type,correlation_id,causation_id,payload,created_at) VALUES ($1,'',$2,$3,$4,$5,$6,$7::jsonb,NOW()) ON CONFLICT (command_id) DO NOTHING`, c.ID, c.SagaID, c.EffectID, c.Type, c.CorrelationID, c.CausationID, objectJSON(c.Payload))
	return e
}
func (s store) AppendSagaHistory(ctx context.Context, v sdk.SagaHistoryEvent) error {
	_, err := s.tx.ExecContext(ctx, `INSERT INTO cursus_saga_history (history_event_id,history_schema_version,environment_id,service_name,saga_type,saga_id,run_id,sequence,event_type,occurred_at,recorded_at,step_id,attempt,command_id,effect_id,source_event_id,correlation_id,causation_id,payload,error) VALUES ($1::uuid,$2,$3,$4,$5,$6,$7::uuid,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`, v.HistoryEventID, v.HistorySchemaVersion, v.EnvironmentID, v.ServiceName, v.SagaType, v.SagaID, v.RunID, v.Sequence, v.EventType, v.OccurredAt, v.RecordedAt, v.StepID, v.Attempt, v.CommandID, v.EffectID, v.SourceEventID, v.CorrelationID, v.CausationID, v.Payload, v.Error)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal saga history outbox payload: %w", err)
	}
	_, err = s.tx.ExecContext(ctx, `INSERT INTO cursus_saga_history_outbox (history_event_id,topic_name,payload) VALUES ($1::uuid,$2,$3::jsonb)`, v.HistoryEventID, s.historyTopic, string(payload))
	return err
}
func objectJSON(v string) string {
	if v == "" {
		return "{}"
	}
	return v
}

var _ sdk.SagaTransaction = Transaction{}
