package entitystore

import (
	"context"
	"database/sql"
)

func SeedPostgresPublicationIdentityLookalikesTx(ctx context.Context, tx *sql.Tx, runID, entityID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE entity_state
		SET fields='{"entity_id":"authored-lookalike","flow_instance":"authored/lookalike"}'::jsonb,
		bookkeeping='{"entity_id":"bookkeeping-lookalike","flow_instance":"bookkeeping/lookalike"}'::jsonb
		WHERE run_id=$1::uuid AND entity_id=$2::uuid`, runID, entityID)
	return err
}

// The state-only hostile input deliberately has no construction or attachment.
func SeedPostgresPublicationUnroutableEntityTx(ctx context.Context, tx *sql.Tx, runID, entityID, flowInstance string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO entity_state
		(run_id,entity_id,flow_instance,entity_type,current_state,gates,fields,accumulator,revision,entered_state_at,created_at,updated_at)
		VALUES($1::uuid,$2::uuid,$3,'widget','pending','{}'::jsonb,'{}'::jsonb,'{}'::jsonb,1,now(),now(),now())`, runID, entityID, flowInstance)
	return err
}
