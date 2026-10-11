package mutationlog

import (
	"context"
	"database/sql"
)

func CountScenarioSetupMutationsTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND writer_type='platform' AND writer_id='test.setup_entities'`, runID, entityID).Scan(&count)
	return count, err
}
