package apiidempotency

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func applyMailboxCompletionInsertFault(ctx context.Context, tx *sql.Tx, key, witness string, enabled, postgres bool) error {
	literal := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
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

func (o *PostgresOwner) SetMailboxCompletionInsertFaultForTest(ctx context.Context, key, witness string, enabled bool) error {
	if o == nil || o.backend == nil {
		return fmt.Errorf("postgres fixture store is required")
	}
	return o.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return applyMailboxCompletionInsertFault(ctx, tx, key, witness, enabled, true)
	})
}

func (o *SQLiteOwner) SetMailboxCompletionInsertFaultForTest(ctx context.Context, key, witness string, enabled bool) error {
	if o == nil || o.backend == nil {
		return fmt.Errorf("sqlite fixture store is required")
	}
	return o.backend.RunTransaction(ctx, "mailbox completion insert fault fixture", func(ctx context.Context, tx *sql.Tx) error {
		return applyMailboxCompletionInsertFault(ctx, tx, key, witness, enabled, false)
	})
}
