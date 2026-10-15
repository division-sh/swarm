package eventrecord

import (
	"context"
	"database/sql"
)

func SetA2AccumulatorPublicationFailureTx(ctx context.Context, tx *sql.Tx, postgres, enabled bool) error {
	if !enabled {
		query := "DROP TRIGGER a2_accumulator_publication_failure"
		if postgres {
			query += " ON events"
		}
		_, err := tx.ExecContext(ctx, query)
		return err
	}
	query := `CREATE TRIGGER a2_accumulator_publication_failure BEFORE INSERT ON events WHEN NEW.event_name='item.recorded' BEGIN SELECT RAISE(ABORT,'a2_accumulator_publication_fault'); END`
	if postgres {
		if _, err := tx.ExecContext(ctx, `CREATE FUNCTION a2_accumulator_publication_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_name='item.recorded' THEN RAISE EXCEPTION 'a2_accumulator_publication_fault'; END IF; RETURN NEW; END $$`); err != nil {
			return err
		}
		query = `CREATE TRIGGER a2_accumulator_publication_failure BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION a2_accumulator_publication_fail()`
	}
	_, err := tx.ExecContext(ctx, query)
	return err
}
