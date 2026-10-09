package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type runForkWorkflowTimerSourceOwner interface {
	RequireActiveSourceTx(context.Context, *sql.Tx, string) (correlation.SourceArtifactFact, error)
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

func (s *PipelinePostgresOwner) RequireRunForkWorkflowTimerTx(ctx context.Context, attempt *mutationprotocol.Attempt, activation pipeline.WorkflowTimerActivation, removed bool) error {
	if s == nil || s.RunLifecyclePostgresOwner == nil {
		return fmt.Errorf("fork workflow timer requires the postgres lifecycle owner")
	}
	return requireRunForkWorkflowTimer(ctx, attempt, s.RunLifecyclePostgresOwner, true, activation, removed)
}

func (s *PipelineSQLiteOwner) RequireRunForkWorkflowTimerTx(ctx context.Context, attempt *mutationprotocol.Attempt, activation pipeline.WorkflowTimerActivation, removed bool) error {
	if s == nil || s.RunLifecycleSQLiteOwner == nil {
		return fmt.Errorf("fork workflow timer requires the sqlite lifecycle owner")
	}
	return requireRunForkWorkflowTimer(ctx, attempt, s.RunLifecycleSQLiteOwner, false, activation, removed)
}

func requireRunForkWorkflowTimer(ctx context.Context, attempt *mutationprotocol.Attempt, source runForkWorkflowTimerSourceOwner, postgres bool, activation pipeline.WorkflowTimerActivation, removed bool) error {
	if err := requireRunForkWorkflowTimerFrame(ctx, attempt, activation); err != nil {
		return err
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := admitRunForkWorkflowTimerSource(ctx, tx, source, activation.RunID); err != nil {
			return err
		}
		actual, found, err := loadWorkflowEngineTimerActivation(ctx, tx, postgres, activation.Ref)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("materialized fork workflow timer %s is missing", activation.Ref.ActivationID)
		}
		if err := actual.ValidateCauseReplay(activation); err != nil {
			return err
		}
		if removed && (actual.Status != "cancelled" || actual.CancelCause != pipeline.WorkflowTimerCancelCauseRuleRemoved || !actual.CancelledAt.Equal(activation.CreatedAt)) {
			return fmt.Errorf("materialized fork workflow timer lost its exact rule-removal disposition")
		}
		return nil
	})
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
