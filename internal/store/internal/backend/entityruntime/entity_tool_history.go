package entitystore

import (
	"context"
	"database/sql"
	"time"
)

func InsertEntityToolHistoricalReadOnlyStateTx(ctx context.Context, tx *sql.Tx, runID, entityID string, at time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO entity_state
		(run_id,entity_id,flow_instance,entity_type,current_state,gates,fields,accumulator,revision,entered_state_at,created_at,updated_at)
		VALUES ($1::uuid,$2::uuid,'review/inst-1','accounts','queued','{}'::jsonb,'{"status":"open"}'::jsonb,'{}'::jsonb,1,$3,$3,$3)`, runID, entityID, at)
	return err
}
