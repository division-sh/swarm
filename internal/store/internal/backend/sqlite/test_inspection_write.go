package sqlite

import (
	"context"
	"database/sql"
)

func (b *Backend) AttemptInspectionTableCreationForTest(ctx context.Context) error {
	return b.RunTransaction(ctx, "read-only observer refusal", func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE snapshot_forbidden_writer (id TEXT)`)
		return err
	})
}
