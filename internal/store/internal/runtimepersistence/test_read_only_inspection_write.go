package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

// This closed construction negative control attempts the original literal
// CREATE on the observer's own backend, without exporting that backend or SQL.
func AttemptSQLiteInspectionTableCreationForTest(ctx context.Context, selected any) error {
	owner, ok := selected.(*SQLiteRuntimeStore)
	if !ok || owner == nil || owner.backend == nil || !owner.backend.Valid() {
		return fmt.Errorf("inspection write control requires an initialized native sqlite observer")
	}
	return owner.backend.RunTransaction(ctx, "read-only observer refusal", func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE snapshot_forbidden_writer (id TEXT)`)
		return err
	})
}
