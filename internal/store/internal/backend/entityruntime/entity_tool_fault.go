package entitystore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// This contradictory sibling is negative read evidence, not a constructed
// entity or a source-history fact. The live header retains its real owner.
func InsertEntityToolSiblingAliasFaultTx(ctx context.Context, tx *sql.Tx, runID, ownerID, siblingID string, fields map[string]any, at time.Time) error {
	var owners int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances
		WHERE run_id=$1 AND instance_path='validation/inst-1' AND entity_id=$2 AND entity_type='validation_case'`, runID, ownerID).Scan(&owners); err != nil {
		return err
	}
	if owners != 1 {
		return fmt.Errorf("entity-tool sibling alias fault requires its exact constructed owner")
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO entity_state
		(run_id,entity_id,flow_instance,entity_type,name,current_state,gates,fields,accumulator,revision,entered_state_at,created_at,updated_at)
		VALUES ($1,$2,'validation/inst-1','validation_case','','queued','{}',$3::jsonb,'{}',1,$4,$4,$4)`, runID, siblingID, string(raw), at)
	return err
}

func SetEntityToolPrivateBookkeepingFaultTx(ctx context.Context, tx *sql.Tx, entityID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE entity_state SET bookkeeping='{"private_fact":"must-not-leak"}'::jsonb WHERE entity_id=$1::uuid`, entityID)
	return err
}

func SetEntityToolUndeclaredFieldFaultTx(ctx context.Context, tx *sql.Tx, entityID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE entity_state
		SET fields=jsonb_set(COALESCE(fields,'{}'::jsonb),'{legacy_flag}','true'::jsonb,true),
		bookkeeping='{"private_fact":"must-not-leak"}'::jsonb WHERE entity_id=$1::uuid`, entityID)
	return err
}

func SetEntityToolMarginalStateFaultTx(ctx context.Context, tx *sql.Tx, entityID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE entity_state SET current_state='marginal_review' WHERE entity_id=$1::uuid`, entityID)
	return err
}

func AdvanceEntityToolForkSourceFaultTx(ctx context.Context, tx *sql.Tx, runID, entityID string, at time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE entity_state SET current_state='done', fields='{"status":"closed"}'::jsonb,
		revision=7, updated_at=$3 WHERE run_id=$1::uuid AND entity_id=$2::uuid`, runID, entityID, at)
	return err
}
