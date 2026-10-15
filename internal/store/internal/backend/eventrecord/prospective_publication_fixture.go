package eventrecord

import (
	"context"
	"database/sql"
)

func InstallProspectivePublicationCreationFailureTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	fault := `CREATE TRIGGER prospective_publication_failure BEFORE INSERT ON events WHEN NEW.event_name='work.ready' BEGIN SELECT RAISE(ABORT,'prospective_publication_fault_cut'); END`
	if postgres {
		if _, err := tx.ExecContext(ctx, `CREATE FUNCTION prospective_publication_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_name='work.ready' THEN RAISE EXCEPTION 'prospective_publication_fault_cut'; END IF; RETURN NEW; END $$`); err != nil {
			return err
		}
		fault = `CREATE TRIGGER prospective_publication_failure BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION prospective_publication_fail()`
	}
	_, err := tx.ExecContext(ctx, fault)
	return err
}

func RemoveProspectivePublicationCreationFailureTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	statements := []string{`DROP TRIGGER IF EXISTS prospective_publication_failure`}
	if postgres {
		statements = []string{`DROP TRIGGER IF EXISTS prospective_publication_failure ON events`, `DROP FUNCTION IF EXISTS prospective_publication_fail()`}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
