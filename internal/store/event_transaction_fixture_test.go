package store

import (
	"context"
	"database/sql"
)

// runEventTransaction is a test-only fixture boundary for legacy semantic
// fixtures that need to assemble several private facts atomically. Production
// code has no generic event transaction operation.
func (s *SQLiteRuntimeStore) runEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runAuthorActivityMutation(ctx, "sqlite event fixture transaction", fn)
}

func (s *PostgresStore) runEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runAuthorActivityMutation(ctx, "postgres event fixture transaction", fn)
}
