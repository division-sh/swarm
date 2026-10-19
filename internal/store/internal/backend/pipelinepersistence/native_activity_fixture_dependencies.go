package pipelinepersistence

import (
	"context"
	"database/sql"
)

func ReadActivityForkEntityStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (ActivityForkEntityStorage, error) {
	var out ActivityForkEntityStorage
	err := tx.QueryRowContext(ctx, `SELECT es.flow_instance,es.entity_type,fi.current_state,es.fields,fi.accumulator
		FROM entity_state es JOIN flow_instances fi
		ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance AND fi.entity_id=es.entity_id
		WHERE es.run_id=$1 AND es.entity_id=$2`, runID, entityID).
		Scan(&out.Flow, &out.EntityType, &out.Stage, &out.Fields, &out.Accumulator)
	if err != nil {
		return ActivityForkEntityStorage{}, err
	}
	return out, nil
}

type ActivityForkEntityStorage struct {
	Flow, EntityType, Stage string
	Fields, Accumulator     []byte
}
