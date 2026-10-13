package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// SuspendedForkReceiverConstruction retains one exact physical receipt while a
// paused-child fixture removes or corrupts its companion. It lends no SQL or
// ordinary construction authority and cannot be minted by a serialized value.
type SuspendedForkReceiverConstruction struct {
	runID, entityID, instance, workflowName, occurredAt, createdAt string
	projectionVersion                                              int
	projection                                                     []byte
}

func SuspendForkReceiverConstructionTxForTest(ctx context.Context, tx *sql.Tx, runID, entityID string) (SuspendedForkReceiverConstruction, error) {
	var saved SuspendedForkReceiverConstruction
	saved.runID, saved.entityID = runID, entityID
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT m.instance_path,f.flow_template,m.projection_version,CAST(m.projection AS TEXT),CAST(m.occurred_at AS TEXT),CAST(m.created_at AS TEXT)
		FROM workflow_instance_initial_materializations m JOIN flow_instances f ON f.run_id=m.run_id AND f.entity_id=m.entity_id AND f.instance_path=m.instance_path
		WHERE m.run_id=$1 AND m.entity_id=$2 AND EXISTS (SELECT 1 FROM runs WHERE run_id=$1 AND status='paused')`, runID, entityID).
		Scan(&saved.instance, &saved.workflowName, &saved.projectionVersion, &raw, &saved.occurredAt, &saved.createdAt); err != nil {
		return SuspendedForkReceiverConstruction{}, err
	}
	if saved.projectionVersion != pipeline.FlowConstructionReceiptVersion {
		return SuspendedForkReceiverConstruction{}, fmt.Errorf("receiver construction suspension requires its exact receipt version")
	}
	saved.projection = []byte(raw)
	if _, err := pipeline.DecodeStoredFlowConstructionReceipt(saved.projection, runID, entityID, saved.instance, saved.workflowName); err != nil {
		return SuspendedForkReceiverConstruction{}, err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3`, runID, saved.instance, entityID)
	if err != nil {
		return SuspendedForkReceiverConstruction{}, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return SuspendedForkReceiverConstruction{}, err
	}
	if count != 1 {
		return SuspendedForkReceiverConstruction{}, fmt.Errorf("receiver construction suspension requires one exact receipt")
	}
	return saved, nil
}

func RestoreForkReceiverConstructionTxForTest(ctx context.Context, tx *sql.Tx, saved SuspendedForkReceiverConstruction) error {
	if saved.runID == "" || saved.entityID == "" || saved.instance == "" || saved.workflowName == "" || saved.occurredAt == "" || saved.createdAt == "" || saved.projectionVersion != pipeline.FlowConstructionReceiptVersion || len(saved.projection) == 0 {
		return fmt.Errorf("receiver construction restore requires its exact private witness")
	}
	var paused bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs r JOIN flow_instances f ON f.run_id=r.run_id
		WHERE r.run_id=$1 AND r.status='paused' AND f.entity_id=$2 AND f.instance_path=$3 AND f.flow_template=$4)`,
		saved.runID, saved.entityID, saved.instance, saved.workflowName).Scan(&paused); err != nil {
		return err
	}
	if !paused {
		return fmt.Errorf("receiver construction restore requires its exact paused child header")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO workflow_instance_initial_materializations (run_id,instance_path,entity_id,projection_version,projection,occurred_at,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		saved.runID, saved.instance, saved.entityID, saved.projectionVersion, string(saved.projection), saved.occurredAt, saved.createdAt)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("receiver construction restore requires one exact receipt")
	}
	return nil
}
