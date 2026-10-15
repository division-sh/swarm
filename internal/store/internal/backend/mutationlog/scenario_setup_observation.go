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

func ReadFanOutAccountIDsMutationSourceStorageTx(ctx context.Context, tx *sql.Tx, runID, mutationID, entityID string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT caused_by_event FROM entity_mutations WHERE run_id=$1 AND mutation_id=$2 AND entity_id=$3 AND domain='authored_field' AND path='account_ids'`, runID, mutationID, entityID).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}
