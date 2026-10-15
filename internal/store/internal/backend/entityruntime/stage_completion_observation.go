package entitystore

import (
	"context"
	"database/sql"
)

func ReadStageCompletionDiscoveryStateTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (string, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT current_state FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance='discovery'`, runID, entityID).Scan(&state)
	return state, err
}
