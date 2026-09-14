// Package sagamysql contains the service-owned MySQL 8+ Saga transaction adapter.
package sagamysql

import (
	"context"
	"database/sql"
	_ "embed"
	"strings"
)

//go:embed migrations/001_saga_history_v1.sql
var schemaV1 string

// Migrate creates the service-owned Saga tables. MySQL 8+ is required for the
// JSON, CHECK, and SKIP LOCKED features used by this adapter.
func Migrate(ctx context.Context, db *sql.DB) error {
	for _, statement := range strings.Split(schemaV1, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
