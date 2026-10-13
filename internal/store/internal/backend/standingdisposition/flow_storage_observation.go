package standingdisposition

import (
	"context"
	"database/sql"
)

// This tuple is physical storage evidence, not a classified restart disposition.
type StandingFlowStorage struct {
	ServiceID, RunID string
	Generation       int64
}

func ReadStandingFlowStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, flowID string) (StandingFlowStorage, error) {
	query := `SELECT service_id, current_run_id, current_generation FROM standing_services WHERE flow_path = ?`
	if postgres {
		query = `SELECT service_id::text, current_run_id::text, current_generation FROM standing_services WHERE flow_path = $1`
	}
	var result StandingFlowStorage
	err := tx.QueryRowContext(ctx, query, flowID).Scan(&result.ServiceID, &result.RunID, &result.Generation)
	return result, err
}
