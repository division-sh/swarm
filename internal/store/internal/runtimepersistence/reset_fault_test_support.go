package runtimepersistence

import (
	"context"
	"fmt"
)

// SetResetFinalReceiptFaultForTest installs one named persistence fault through
// the selected writer. It exposes no raw SQL or transaction callback to callers.
func SetResetFinalReceiptFaultForTest(ctx context.Context, selected any, enabled bool) error {
	return setResetReceiptFaultForTest(ctx, selected, "completed", enabled)
}

func SetResetPlanReceiptFaultForTest(ctx context.Context, selected any, enabled bool) error {
	return setResetReceiptFaultForTest(ctx, selected, "planned", enabled)
}

func setResetReceiptFaultForTest(ctx context.Context, selected any, phase string, enabled bool) error {
	if phase != "planned" && phase != "completed" {
		return fmt.Errorf("unsupported reset receipt fault phase %s", phase)
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("reset receipt fault requires a PostgreSQL fixture store")
		}
		return store.destructiveResetPostgresOwner.SetResetReceiptFaultForTest(ctx, phase, enabled)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("reset receipt fault requires a SQLite fixture store")
		}
		return store.destructiveResetSQLiteOwner.SetResetReceiptFaultForTest(ctx, phase, enabled)
	default:
		return fmt.Errorf("reset receipt fault store %T is unsupported", selected)
	}
}
