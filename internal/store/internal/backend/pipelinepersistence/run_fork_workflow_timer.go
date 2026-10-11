package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type runForkWorkflowTimerSourceOwner interface {
	RequireActiveSourceTx(context.Context, *sql.Tx, string) (correlation.SourceArtifactFact, error)
}

func (s *PipelinePostgresOwner) ReadRunForkWorkflowTimerInventoryTx(ctx context.Context, attempt *mutationprotocol.Attempt, childRunID string) ([]pipeline.WorkflowTimerActivation, error) {
	if s == nil || s.RunLifecyclePostgresOwner == nil {
		return nil, fmt.Errorf("fork workflow timer inventory requires the postgres lifecycle owner")
	}
	return readRunForkWorkflowTimerInventory(ctx, attempt, s.RunLifecyclePostgresOwner, true, childRunID)
}

func (s *PipelineSQLiteOwner) ReadRunForkWorkflowTimerInventoryTx(ctx context.Context, attempt *mutationprotocol.Attempt, childRunID string) ([]pipeline.WorkflowTimerActivation, error) {
	if s == nil || s.RunLifecycleSQLiteOwner == nil {
		return nil, fmt.Errorf("fork workflow timer inventory requires the sqlite lifecycle owner")
	}
	return readRunForkWorkflowTimerInventory(ctx, attempt, s.RunLifecycleSQLiteOwner, false, childRunID)
}

// Readback carries persisted facts only; callers own expected-set comparison
// and whether lifecycle progress is lawful for their materialization phase.
func readRunForkWorkflowTimerInventory(ctx context.Context, attempt *mutationprotocol.Attempt, source runForkWorkflowTimerSourceOwner, postgres bool, childRunID string) ([]pipeline.WorkflowTimerActivation, error) {
	if attempt == nil {
		return nil, fmt.Errorf("fork workflow timer inventory requires its native attempt")
	}
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return nil, err
	}
	if childRunID == "" || childRunID != strings.TrimSpace(childRunID) || correlation.RunIDFromContext(ctx) != childRunID {
		return nil, fmt.Errorf("fork workflow timer inventory frame belongs to another child run")
	}
	var inventory []pipeline.WorkflowTimerActivation
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := admitRunForkWorkflowTimerSource(ctx, tx, source, childRunID); err != nil {
			return err
		}
		refs, err := loadRunForkWorkflowTimerInventoryRefs(ctx, tx, postgres, childRunID)
		if err != nil {
			return err
		}
		inventory = make([]pipeline.WorkflowTimerActivation, 0, len(refs))
		for _, ref := range refs {
			actual, found, err := loadWorkflowEngineTimerActivation(ctx, tx, postgres, ref)
			if err != nil {
				return err
			}
			if !found || actual.RunID != childRunID || actual.SourceTimerID == "" {
				return fmt.Errorf("fork workflow timer inventory row %s is missing or lacks exact inherited child ownership", ref.ActivationID)
			}
			inventory = append(inventory, actual)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return nil, err
	}
	return inventory, nil
}

func loadRunForkWorkflowTimerInventoryRefs(ctx context.Context, tx *sql.Tx, postgres bool, childRunID string) ([]timeridentity.WorkflowTimerActivationRef, error) {
	query := `SELECT CAST(timer_id AS TEXT), timer_name FROM timers
		WHERE run_id = ? AND task_type = 'workflow_timer'
		AND (source_timer_id IS NOT NULL OR forked_from_run_id IS NOT NULL
		     OR reconstruction_owner IS NOT NULL OR forked_from_event_id IS NOT NULL
		     OR forked_from_point_kind IS NOT NULL OR forked_from_point_revision IS NOT NULL
		     OR source_armed_at IS NOT NULL)
		ORDER BY timer_id`
	if postgres {
		query = strings.Replace(query, "run_id = ?", "run_id = $1::uuid", 1)
	}
	rows, err := tx.QueryContext(ctx, query, childRunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refs []timeridentity.WorkflowTimerActivationRef
	previous := ""
	for rows.Next() {
		var id, taskID string
		if err := rows.Scan(&id, &taskID); err != nil {
			return nil, err
		}
		ref, valid := timeridentity.ParseWorkflowTimerActivationTaskID(taskID)
		if !valid || ref.ActivationID != id || ref.TaskID() != taskID || id <= previous {
			return nil, fmt.Errorf("fork workflow timer inventory row %s has invalid or repeated exact task identity", id)
		}
		previous = id
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return refs, nil
}

func (s *PipelinePostgresOwner) MaterializeRunForkWorkflowTimerTx(ctx context.Context, attempt *mutationprotocol.Attempt, activation pipeline.WorkflowTimerActivation, removed bool) error {
	if s == nil || s.RunLifecyclePostgresOwner == nil {
		return fmt.Errorf("fork workflow timer requires the postgres lifecycle owner")
	}
	return materializeRunForkWorkflowTimer(ctx, attempt, s.RunLifecyclePostgresOwner, true, activation, removed)
}

func (s *PipelineSQLiteOwner) MaterializeRunForkWorkflowTimerTx(ctx context.Context, attempt *mutationprotocol.Attempt, activation pipeline.WorkflowTimerActivation, removed bool) error {
	if s == nil || s.RunLifecycleSQLiteOwner == nil {
		return fmt.Errorf("fork workflow timer requires the sqlite lifecycle owner")
	}
	return materializeRunForkWorkflowTimer(ctx, attempt, s.RunLifecycleSQLiteOwner, false, activation, removed)
}

func materializeRunForkWorkflowTimer(ctx context.Context, attempt *mutationprotocol.Attempt, source runForkWorkflowTimerSourceOwner, postgres bool, activation pipeline.WorkflowTimerActivation, removed bool) error {
	if err := requireRunForkWorkflowTimerFrame(ctx, attempt, activation); err != nil {
		return err
	}
	if removed {
		if err := activation.ValidateCancellation(pipeline.WorkflowTimerCancelCauseRuleRemoved, activation.CreatedAt); err != nil {
			return err
		}
	}
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return admitRunForkWorkflowTimerSource(ctx, tx, source, activation.RunID)
	}); err != nil {
		return err
	}
	if _, _, err := commitWorkflowEngineTimerMutation(ctx, attempt, postgres, pipeline.WorkflowTimerMutation{
		Kind: pipeline.WorkflowTimerMutationInsert, Activation: activation,
	}); err != nil {
		return err
	}
	if !removed {
		return nil
	}
	_, _, err := commitWorkflowEngineTimerMutation(ctx, attempt, postgres, pipeline.WorkflowTimerMutation{
		Kind: pipeline.WorkflowTimerMutationCancel, Activation: activation,
		CancelCause: pipeline.WorkflowTimerCancelCauseRuleRemoved, CancelledAt: activation.CreatedAt,
	})
	return err
}

func requireRunForkWorkflowTimerFrame(ctx context.Context, attempt *mutationprotocol.Attempt, activation pipeline.WorkflowTimerActivation) error {
	if attempt == nil {
		return fmt.Errorf("fork workflow timer requires its native materialization attempt")
	}
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return err
	}
	if err := activation.Validate(); err != nil {
		return err
	}
	if activation.SourceTimerID == "" || activation.ForkedFromRunID == activation.RunID || activation.Status != "active" {
		return fmt.Errorf("fork workflow timer requires an exact active inherited obligation")
	}
	if correlation.RunIDFromContext(ctx) != activation.RunID {
		return fmt.Errorf("fork workflow timer frame belongs to another child run")
	}
	return nil
}

func admitRunForkWorkflowTimerSource(ctx context.Context, tx *sql.Tx, owner runForkWorkflowTimerSourceOwner, runID string) error {
	if owner == nil {
		return fmt.Errorf("fork workflow timer source owner is required")
	}
	fact, err := owner.RequireActiveSourceTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	admitted, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present || !admitted.Matches(fact) {
		return fmt.Errorf("fork workflow timer source differs from its materialization context")
	}
	_, err = authoractivity.BundleScopeForSource(ctx, fact.BundleHash())
	return err
}
