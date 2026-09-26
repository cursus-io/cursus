//go:build legacy_sql_saga

package sagapg

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/sdk"
)

// HistoryPublisher must return nil only when the history record has received a
// broker acknowledgement. A failed attempt remains pending for redelivery.
type HistoryPublisher interface {
	PublishSagaHistory(context.Context, string, string) error
}

// SDKHistoryPublisher adapts the SDK producer to the outbox publisher. Flush
// waits for broker acknowledgement before the outbox row is marked published.
type SDKHistoryPublisher struct{ Producer *sdk.Producer }

func (p SDKHistoryPublisher) PublishSagaHistory(ctx context.Context, _ string, payload string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.Producer == nil {
		return fmt.Errorf("saga history producer is required")
	}
	if _, err := p.Producer.PublishMessage(payload); err != nil {
		return err
	}
	return p.Producer.Flush()
}

// HistoryOutboxPublisher delivers immutable history using at-least-once
// semantics. A process crash after broker acknowledgement and before the row
// is marked published can redeliver the same history_event_id; collectors must
// deduplicate that stable ID.
type HistoryOutboxPublisher struct {
	DB        *sql.DB
	Publisher HistoryPublisher
	Now       func() time.Time
}

type historyOutboxEntry struct {
	ID, Topic, Payload string
}

func (p HistoryOutboxPublisher) PublishPending(ctx context.Context, limit int) (int, error) {
	if p.DB == nil || p.Publisher == nil {
		return 0, fmt.Errorf("history outbox requires database and publisher")
	}
	if limit <= 0 {
		limit = 100
	}
	published := 0
	for published < limit {
		entry, found, err := p.claim(ctx)
		if err != nil {
			return published, err
		}
		if !found {
			return published, nil
		}
		if err := p.Publisher.PublishSagaHistory(ctx, entry.Topic, entry.Payload); err != nil {
			if releaseErr := p.release(ctx, entry.ID, err); releaseErr != nil {
				return published, fmt.Errorf("publish saga history: %w (release outbox row: %v)", err, releaseErr)
			}
			return published, fmt.Errorf("publish saga history: %w", err)
		}
		if err := p.markPublished(ctx, entry.ID); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}

func (p HistoryOutboxPublisher) claim(ctx context.Context) (historyOutboxEntry, bool, error) {
	tx, err := p.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return historyOutboxEntry{}, false, err
	}
	var entry historyOutboxEntry
	err = tx.QueryRowContext(ctx, `SELECT history_event_id::text,topic_name,payload::text FROM cursus_saga_history_outbox WHERE status='PENDING' OR (status='PUBLISHING' AND lease_expires_at < NOW()) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&entry.ID, &entry.Topic, &entry.Payload)
	if err == sql.ErrNoRows {
		_ = tx.Rollback()
		return historyOutboxEntry{}, false, nil
	}
	if err != nil {
		_ = tx.Rollback()
		return historyOutboxEntry{}, false, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE cursus_saga_history_outbox SET status='PUBLISHING',attempts=attempts+1,last_error='',lease_expires_at=NOW() + INTERVAL '2 minutes' WHERE history_event_id=$1::uuid`, entry.ID)
	if err != nil {
		_ = tx.Rollback()
		return historyOutboxEntry{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return historyOutboxEntry{}, false, err
	}
	return entry, true, nil
}

func (p HistoryOutboxPublisher) release(ctx context.Context, id string, cause error) error {
	_, err := p.DB.ExecContext(ctx, `UPDATE cursus_saga_history_outbox SET status='PENDING',last_error=$2,lease_expires_at=NULL WHERE history_event_id=$1::uuid`, id, cause.Error())
	return err
}

func (p HistoryOutboxPublisher) markPublished(ctx context.Context, id string) error {
	now := time.Now().UTC()
	if p.Now != nil {
		now = p.Now().UTC()
	}
	_, err := p.DB.ExecContext(ctx, `UPDATE cursus_saga_history_outbox SET status='PUBLISHED',published_at=$2,last_error='',lease_expires_at=NULL WHERE history_event_id=$1::uuid`, id, now)
	return err
}
