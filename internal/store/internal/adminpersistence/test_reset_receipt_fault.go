package adminpersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *DestructiveResetPostgresOwner) SetResetReceiptFaultForTest(ctx context.Context, phase string, enabled bool) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("reset receipt fault requires the original postgres owner")
	}
	if !enabled {
		_, err := s.backend.ConstructionHandle().ExecContext(ctx, `DROP TRIGGER reset_final_receipt_fault ON runtime_reset_operations; DROP FUNCTION reset_final_receipt_fault()`)
		return err
	}
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		statement := fmt.Sprintf(`CREATE FUNCTION reset_final_receipt_fault() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN IF NEW.phase = '%s' THEN RAISE EXCEPTION 'reset receipt fault'; END IF; RETURN NEW; END $$;
			CREATE TRIGGER reset_final_receipt_fault BEFORE UPDATE ON runtime_reset_operations FOR EACH ROW EXECUTE FUNCTION reset_final_receipt_fault()`, phase)
		_, err := tx.ExecContext(txctx, statement)
		return err
	})
}

func (s *DestructiveResetSQLiteOwner) SetResetReceiptFaultForTest(ctx context.Context, phase string, enabled bool) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("reset receipt fault requires the original sqlite owner")
	}
	if !enabled {
		_, err := s.backend.ConstructionHandle().ExecContext(ctx, `DROP TRIGGER reset_final_receipt_fault`)
		return err
	}
	return s.backend.RunTransaction(ctx, "reset final receipt fault fixture", func(txctx context.Context, tx *sql.Tx) error {
		statement := fmt.Sprintf(`CREATE TRIGGER reset_final_receipt_fault BEFORE UPDATE ON runtime_reset_operations
			WHEN NEW.phase = '%s' BEGIN SELECT RAISE(ABORT, 'reset receipt fault'); END`, phase)
		_, err := tx.ExecContext(txctx, statement)
		return err
	})
}
