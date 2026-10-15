package entitystore

import (
	"context"
	"database/sql"
)

type ServedSetupEntityStorage struct {
	FlowInstance, EntityType, State string
	Score                           any
}

func ReadServedSetupEntityStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID string) (ServedSetupEntityStorage, error) {
	query := `SELECT flow_instance, entity_type, current_state, json_extract(fields, '$.score') FROM entity_state WHERE run_id = ? AND entity_id = ?`
	if postgres {
		query = `SELECT flow_instance, entity_type, current_state, fields->>'score' FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
	}
	var out ServedSetupEntityStorage
	err := tx.QueryRowContext(ctx, query, runID, entityID).Scan(&out.FlowInstance, &out.EntityType, &out.State, &out.Score)
	return out, err
}
