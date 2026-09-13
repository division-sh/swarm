package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SetMailboxCompletionInsertFaultForTest owns the named response-insert fault's
// DDL through the selected writer, not an unfenced fixture connection.
func SetMailboxCompletionInsertFaultForTest(ctx context.Context, selected any, key, witness string, enabled bool) error {
	if enabled && (key == "" || witness == "") {
		return fmt.Errorf("mailbox completion fault requires a key and witness")
	}
	literal := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	apply := func(ctx context.Context, tx *sql.Tx, postgres bool) error {
		statements := []string{`DROP TRIGGER mailbox_completion_cut`}
		if enabled {
			statements = []string{`CREATE TRIGGER mailbox_completion_cut BEFORE INSERT ON api_idempotency WHEN NEW.idempotency_key=` + literal(key) + ` BEGIN SELECT swarm_test_mailbox_completion_cut(` + literal(witness) + `); SELECT RAISE(ABORT,'mailbox_completion_exact_insert_cut'); END`}
		}
		if postgres {
			statements = []string{`DROP TRIGGER mailbox_completion_cut ON api_idempotency`, `DROP FUNCTION mailbox_completion_cut()`, `DROP SEQUENCE mailbox_completion_cut_seen`}
			if enabled {
				// Sequence increments survive the deliberately rolled-back mutation.
				statements = []string{
					`CREATE SEQUENCE mailbox_completion_cut_seen`,
					`CREATE FUNCTION mailbox_completion_cut() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.idempotency_key=` + literal(key) + ` THEN PERFORM nextval('mailbox_completion_cut_seen'); RAISE EXCEPTION 'mailbox_completion_exact_insert_cut'; END IF; RETURN NEW; END $$`,
					`CREATE TRIGGER mailbox_completion_cut BEFORE INSERT ON api_idempotency FOR EACH ROW EXECUTE FUNCTION mailbox_completion_cut()`,
				}
			}
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres fixture store is required")
		}
		return store.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return apply(ctx, tx, true) })
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite fixture store is required")
		}
		return store.backend.RunTransaction(ctx, "mailbox completion insert fault fixture", func(ctx context.Context, tx *sql.Tx) error { return apply(ctx, tx, false) })
	default:
		return fmt.Errorf("unsupported mailbox completion fault store %T", selected)
	}
}
