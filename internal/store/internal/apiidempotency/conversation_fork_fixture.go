package apiidempotency

import (
	"context"
	"database/sql"
	"time"
)

type ConversationForkCompletionExpiryStorage struct {
	SQLiteText   string
	PostgresTime time.Time
}

func CountConversationForkCompletionStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT count(*) FROM api_idempotency`).Scan(&count)
	return count, err
}

// Keep the original native scan representation; interpretation stays with the
// TTL witness. There is no actor, key, method or expiry-eligibility filter.
func ReadConversationForkCompletionExpiryStorageTx(ctx context.Context, tx *sql.Tx, postgres bool) (ConversationForkCompletionExpiryStorage, error) {
	var out ConversationForkCompletionExpiryStorage
	row := tx.QueryRowContext(ctx, `SELECT expires_at FROM api_idempotency ORDER BY expires_at DESC LIMIT 1`)
	var err error
	if postgres {
		err = row.Scan(&out.PostgresTime)
	} else {
		err = row.Scan(&out.SQLiteText)
	}
	return out, err
}

func InstallConversationForkCompletionInsertFailureTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	statements := []string{`CREATE TRIGGER fail_fork_completion BEFORE INSERT ON api_idempotency BEGIN SELECT RAISE(ABORT, 'fork completion fault'); END`}
	if postgres {
		statements = []string{
			`CREATE FUNCTION fail_fork_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fork completion fault'; END $$`,
			`CREATE TRIGGER fail_fork_completion BEFORE INSERT ON api_idempotency FOR EACH ROW EXECUTE FUNCTION fail_fork_completion()`,
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func RemoveConversationForkCompletionInsertFailureTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	statements := []string{`DROP TRIGGER fail_fork_completion`}
	if postgres {
		statements = []string{`DROP TRIGGER fail_fork_completion ON api_idempotency`, `DROP FUNCTION fail_fork_completion()`}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
