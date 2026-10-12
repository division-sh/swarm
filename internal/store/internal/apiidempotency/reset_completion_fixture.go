package apiidempotency

import (
	"context"
	"database/sql"
)

func InstallPostgresResetAPICompletionLossTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE FUNCTION lose_reset_api_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'API completion lost'; END $$;
	CREATE TRIGGER lose_reset_api_completion BEFORE INSERT ON api_idempotency FOR EACH ROW EXECUTE FUNCTION lose_reset_api_completion()`)
	return err
}

func RemovePostgresResetAPICompletionLossTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP TRIGGER lose_reset_api_completion ON api_idempotency; DROP FUNCTION lose_reset_api_completion()`)
	return err
}
