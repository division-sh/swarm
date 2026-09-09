package runtimepersistence

import (
	"context"
	"database/sql"
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
		return store.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			statement := `DROP TRIGGER reset_final_receipt_fault ON runtime_reset_operations; DROP FUNCTION reset_final_receipt_fault()`
			if enabled {
				statement = fmt.Sprintf(`CREATE FUNCTION reset_final_receipt_fault() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN IF NEW.phase = '%s' THEN RAISE EXCEPTION 'reset receipt fault'; END IF; RETURN NEW; END $$;
				CREATE TRIGGER reset_final_receipt_fault BEFORE UPDATE ON runtime_reset_operations FOR EACH ROW EXECUTE FUNCTION reset_final_receipt_fault()`, phase)
			}
			_, err := tx.ExecContext(txctx, statement)
			return err
		})
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("reset receipt fault requires a SQLite fixture store")
		}
		return store.backend.RunTransaction(ctx, "reset final receipt fault fixture", func(txctx context.Context, tx *sql.Tx) error {
			statement := `DROP TRIGGER reset_final_receipt_fault`
			if enabled {
				statement = fmt.Sprintf(`CREATE TRIGGER reset_final_receipt_fault BEFORE UPDATE ON runtime_reset_operations
				WHEN NEW.phase = '%s' BEGIN SELECT RAISE(ABORT, 'reset receipt fault'); END`, phase)
			}
			_, err := tx.ExecContext(txctx, statement)
			return err
		})
	default:
		return fmt.Errorf("reset receipt fault store %T is unsupported", selected)
	}
}
