package pipelinepersistence

import (
	"context"
	"database/sql"
)

func ReadWorkflowStateEntityStorageTx(ctx context.Context, tx *sql.Tx, runID, instance, state string) (string, error) {
	var entityID string
	err := tx.QueryRowContext(ctx, `SELECT entity_id FROM flow_instances WHERE run_id=$1 AND instance_path=$2 AND current_state=$3`, runID, instance, state).Scan(&entityID)
	return entityID, err
}
