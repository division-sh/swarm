package pipelinepersistence

import (
	"context"
	"database/sql"
)

func CountGlobalPipelineReceiptStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts`).Scan(&count)
	return count, err
}

func SetSelectedReplayScopeCollisionFaultTx(ctx context.Context, tx *sql.Tx, enabled bool) error {
	statement := `DROP TRIGGER IF EXISTS seed_conflicting_replay_scope_after_event_insert ON events;
DROP FUNCTION IF EXISTS seed_conflicting_replay_scope_after_event_insert();`
	if enabled {
		statement = `CREATE OR REPLACE FUNCTION seed_conflicting_replay_scope_after_event_insert()
RETURNS trigger AS $$
BEGIN
    INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
    VALUES (NEW.event_id, NEW.run_id, 'direct', NEW.created_at, NEW.created_at)
    ON CONFLICT (event_id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER seed_conflicting_replay_scope_after_event_insert
AFTER INSERT ON events
FOR EACH ROW EXECUTE FUNCTION seed_conflicting_replay_scope_after_event_insert();`
	}
	_, err := tx.ExecContext(ctx, statement)
	return err
}
