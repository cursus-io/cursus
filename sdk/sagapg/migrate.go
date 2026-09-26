//go:build legacy_sql_saga

package sagapg

import (
	"context"
	"database/sql"
	_ "embed"
)

//go:embed migrations/001_saga_history_v1.sql
var schemaV1 string

// Migrate creates the service-owned Saga, inbox, command outbox, immutable
// history, and history outbox tables. The schema is idempotent so a service
// can invoke it during controlled startup or an integration test setup.
func Migrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, schemaV1)
	return err
}
