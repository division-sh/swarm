package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

func SetWorkflowProjectionFieldsArrayForTest(ctx context.Context, tx *sql.Tx, run, key string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE entity_state SET fields = '[]' WHERE run_id = $2 AND entity_id = $1`, key, run))
}

func SetWorkflowProjectionNumericGateForTest(ctx context.Context, tx *sql.Tx, run, key string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE flow_instances SET gates = '{"g_ready":1}' WHERE run_id = $2 AND instance_path = $1`, key, run))
}

func SetWorkflowProjectionAccumulatorArrayForTest(ctx context.Context, tx *sql.Tx, run, key string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE flow_instances SET accumulator = '[]' WHERE run_id = $2 AND instance_path = $1`, key, run))
}

func SetWorkflowProjectionMalformedTransitionHistoryForTest(ctx context.Context, tx *sql.Tx, run, key string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE flow_instances SET config = '{"workflow_version":"1.0.0","instance_id":"inst-1","storage_ref":"storage-ref","transition_history":"bad"}' WHERE run_id = $2 AND instance_path = $1`, key, run))
}

func SetWorkflowProjectionConflictingInstanceIDForTest(ctx context.Context, tx *sql.Tx, run, key string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE flow_instances SET config = '{"workflow_version":"1.0.0","instance_id":"inst-2","storage_ref":"storage-ref","flow_path":"storage-ref"}' WHERE run_id = $2 AND instance_path = $1`, key, run))
}

func SetWorkflowProjectionSlashOnlyFlowPathForTest(ctx context.Context, tx *sql.Tx, run, key string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE flow_instances SET config = '{"workflow_version":"1.0.0","instance_id":"inst-1","storage_ref":"storage-ref","flow_path":"/"}' WHERE run_id = $2 AND instance_path = $1`, key, run))
}

func workflowProjectionFaultResult(result sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if changed != 1 {
		return 0, fmt.Errorf("workflow projection fault requires exactly one existing row, got %d", changed)
	}
	return changed, nil
}

func SetWorkflowProjectionObsoleteFieldRowsForTest(ctx context.Context, tx *sql.Tx, run string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE entity_state SET current_state = 'obsolete', gates = '{}', bookkeeping = '{}', accumulator = '{}' WHERE run_id = $1`, run))
}

func SetWorkflowProjectionPlatformBookkeepingForTest(ctx context.Context, tx *sql.Tx, run, path string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE flow_instances SET bookkeeping = '{"platform_fact":"preserve"}' WHERE run_id = $2 AND instance_path = $1`, path, run))
}

func SetEntityProjectionPrivateBookkeepingForTest(ctx context.Context, tx *sql.Tx, run, entity string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE entity_state SET bookkeeping = '{"private_fact":"must-not-leak"}' WHERE run_id = $1 AND entity_id = $2`, run, entity))
}

func SetWorkflowProjectionConflictingEntityTypeForTest(ctx context.Context, tx *sql.Tx, run, entity string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE entity_state SET entity_type = 'wrong_entity_type' WHERE run_id = $1 AND entity_id = $2`, run, entity))
}

func RemoveWorkflowProjectionHeaderForTest(ctx context.Context, tx *sql.Tx, run, path string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `DELETE FROM flow_instances WHERE run_id = $1 AND instance_path = $2`, run, path))
}

func RemoveWorkflowProjectionFieldsForTest(ctx context.Context, tx *sql.Tx, run, entity string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `DELETE FROM entity_state WHERE run_id = $1 AND entity_id = $2`, run, entity))
}

func SetWorkflowProjectionDrainingForTest(ctx context.Context, tx *sql.Tx, run, path string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE flow_instances SET status = 'draining', terminated_at = NULL WHERE run_id = $1 AND instance_path = $2`, run, path))
}

func SetWorkflowProjectionTerminatedForTest(ctx context.Context, tx *sql.Tx, run, path string, at time.Time) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE flow_instances SET status = 'terminated', terminated_at = $1 WHERE run_id = $2 AND instance_path = $3`, at, run, path))
}

func SetWorkflowProjectionActiveTerminatedTimestampForTest(ctx context.Context, tx *sql.Tx, run, path string, at time.Time) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `UPDATE flow_instances SET terminated_at=$1 WHERE run_id=$2 AND instance_path=$3 AND status='active'`, at, run, path))
}

func SetWorkflowProjectionNumericFlowPathForTest(ctx context.Context, tx *sql.Tx, postgres bool, run, path string) (int64, error) {
	query := `UPDATE flow_instances SET config=json_set(config,'$.flow_path',7) WHERE run_id=$1 AND instance_path=$2`
	if postgres {
		query = `UPDATE flow_instances SET config=jsonb_set(config,'{flow_path}','7'::jsonb) WHERE run_id=$1 AND instance_path=$2`
	}
	return workflowProjectionFaultResult(tx.ExecContext(ctx, query, run, path))
}

func AddAmbiguousWorkflowProjectionFieldsForTest(ctx context.Context, tx *sql.Tx, run, entity string) (int64, error) {
	return workflowProjectionFaultResult(tx.ExecContext(ctx, `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,current_state) SELECT run_id,$1,flow_instance,entity_type,current_state FROM entity_state WHERE run_id=$2 AND entity_id=$3`, uuid.NewString(), run, entity))
}

// Only the evidence of the single existing transition may change. This cut
// cannot seed a header or change its identity, stage, fields or other history.
func SetWorkflowTransitionEvidenceWireForTest(ctx context.Context, tx *sql.Tx, postgres bool, run, path string, wire json.RawMessage) (int64, error) {
	current, err := ReadWorkflowTransitionEvidenceWireForTest(ctx, tx, run, path)
	if err != nil {
		return 0, err
	}
	var before, after map[string]any
	if err := json.Unmarshal(current, &before); err != nil {
		return 0, err
	}
	if err := json.Unmarshal(wire, &after); err != nil {
		return 0, err
	}
	strip := func(config map[string]any) error {
		history, ok := config["transition_history"].([]any)
		if !ok || len(history) != 1 {
			return fmt.Errorf("transition evidence fault requires one exact existing transition")
		}
		transition, ok := history[0].(map[string]any)
		if !ok {
			return fmt.Errorf("transition evidence fault requires an existing transition record")
		}
		// Fault only the evidence and its redundant projections; preserve the
		// causal event/time and every other header coordinate for restoration.
		for _, key := range []string{"evidence", "transition_id", "from", "to", "guards_evaluated"} {
			delete(transition, key)
		}
		return nil
	}
	if err := strip(before); err != nil {
		return 0, err
	}
	if err := strip(after); err != nil {
		return 0, err
	}
	if !reflect.DeepEqual(before, after) {
		return 0, fmt.Errorf("transition evidence fault may not change other projection coordinates")
	}
	query := `UPDATE flow_instances SET config=$1 WHERE run_id=$2 AND instance_path=$3`
	if postgres {
		query = `UPDATE flow_instances SET config=$1::jsonb WHERE run_id=$2 AND instance_path=$3`
	}
	return workflowProjectionFaultResult(tx.ExecContext(ctx, query, string(wire), run, path))
}
