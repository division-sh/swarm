package sourceadmission

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// ObserveMaterializedSelectedGrant classifies the private pre-ACK phase. Its
// callers must independently prove the current grant; this is not admission.
func ObserveMaterializedSelectedGrant(ctx context.Context, q RowQueryer, evidence startupownership.GrantEvidence) (bool, error) {
	if err := evidence.Validate(); err != nil {
		return false, err
	}
	if evidence.State != startupownership.GrantAdmitted || evidence.SelectedFork == nil {
		return false, nil
	}
	selected := evidence.SelectedFork
	var operationID string
	err := q.QueryRowContext(ctx, `SELECT operation.operation_id
		FROM run_fork_operations AS operation
		JOIN runs AS child ON child.run_id=operation.fork_run_id AND child.status='paused' AND child.bundle_hash=$4
		JOIN run_fork_operation_live_parents AS parents ON parents.operation_id=operation.operation_id
			AND parents.selected_binding_id=operation.selected_binding_id AND parents.fork_run_id=operation.fork_run_id
			AND parents.source_run_id=operation.source_run_id AND parents.fork_point_kind=operation.fork_point_kind
			AND parents.fork_revision=operation.fork_revision
			AND (parents.fork_event_id=operation.fork_event_id OR (parents.fork_event_id IS NULL AND operation.fork_event_id IS NULL))
		JOIN run_fork_selected_contract_bindings AS binding ON binding.binding_id=operation.selected_binding_id
			AND binding.fork_run_id=operation.fork_run_id AND binding.source_run_id=operation.source_run_id
			AND binding.fork_point_kind=operation.fork_point_kind AND binding.fork_revision=operation.fork_revision
			AND (binding.fork_event_id=operation.fork_event_id OR (binding.fork_event_id IS NULL AND operation.fork_event_id IS NULL))
		JOIN run_fork_selected_contract_runtime_executions AS execution ON execution.binding_id=binding.binding_id
			AND execution.fork_run_id=operation.fork_run_id AND execution.source_run_id=operation.source_run_id
			AND execution.fork_point_kind=operation.fork_point_kind AND execution.fork_revision=operation.fork_revision
			AND (execution.fork_event_id=operation.fork_event_id OR (execution.fork_event_id IS NULL AND operation.fork_event_id IS NULL))
		WHERE operation.fork_run_id=$1 AND operation.selected_binding_id=$2 AND operation.state='materialized'
			AND operation.result_json IS NULL AND operation.failure_json IS NULL
			AND execution.execution_id=$3 AND execution.generation=$5
			AND execution.state='running' AND execution.failure IS NULL`,
		selected.ForkRunID, selected.BindingID, selected.ExecutionID, evidence.BundleHash, selected.ExecutionGeneration).Scan(&operationID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read exact materialized selected execution: %w", err)
	}
	return operationID != "", nil
}
