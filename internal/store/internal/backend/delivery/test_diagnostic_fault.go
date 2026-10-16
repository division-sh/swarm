package delivery

import (
	"context"
	"database/sql"
)

func InstallConformanceDiagnosticFaultTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	query := `CREATE TRIGGER fail_delivery_dead_letter_insert BEFORE INSERT ON dead_letters WHEN NEW.delivery_id IS NOT NULL BEGIN SELECT RAISE(ABORT, 'forced delivery diagnostic failure'); END`
	if postgres {
		if err := InstallConformanceDiagnosticFunctionFaultTx(ctx, tx); err != nil {
			return err
		}
		query = `CREATE TRIGGER fail_delivery_dead_letter_insert BEFORE INSERT ON dead_letters FOR EACH ROW EXECUTE FUNCTION fail_delivery_dead_letter_insert()`
	}
	_, err := tx.ExecContext(ctx, query)
	return err
}

func InstallConformanceDiagnosticFunctionFaultTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE OR REPLACE FUNCTION fail_delivery_dead_letter_insert() RETURNS trigger AS $$ BEGIN IF NEW.delivery_id IS NOT NULL THEN RAISE EXCEPTION 'forced delivery diagnostic failure'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql`)
	return err
}

func RemoveConformanceDiagnosticFaultTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	query := `DROP TRIGGER IF EXISTS fail_delivery_dead_letter_insert`
	if postgres {
		query += ` ON dead_letters`
	}
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return err
	}
	if postgres {
		_, err := tx.ExecContext(ctx, `DROP FUNCTION IF EXISTS fail_delivery_dead_letter_insert()`)
		return err
	}
	return nil
}
