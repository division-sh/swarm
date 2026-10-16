package managedcapabilitystore

import (
	"context"
	"database/sql"
)

func ReadRunlessSelectedPreparationSurfaceStorageTx(ctx context.Context, tx *sql.Tx, surfaceID, preparationID, actorID string) ([]byte, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT surface FROM managed_agent_capability_surfaces WHERE surface_id=$1 AND authority_kind='startup_probe' AND execution_kind='selected_fork_preparation' AND execution_authority_id=$2 AND run_id IS NULL AND actor_id=$3`, surfaceID, preparationID, actorID).Scan(&raw)
	if err != nil {
		return nil, err
	}
	return raw, nil
}
