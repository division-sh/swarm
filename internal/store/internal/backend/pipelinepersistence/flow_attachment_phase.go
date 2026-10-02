package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *PipelinePostgresOwner) AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return pipeline.FlowAttachmentAdvanceResult{}, err
	}
	return advanceFlowAttachment(ctx, true, attempt, previous, at, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (pipeline.FlowAttachmentAdvanceResult, error)) mutationprotocol.Result[pipeline.FlowAttachmentAdvanceResult] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	})
}

func (s *PipelineSQLiteOwner) AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return pipeline.FlowAttachmentAdvanceResult{}, err
	}
	return advanceFlowAttachment(ctx, false, attempt, previous, at, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (pipeline.FlowAttachmentAdvanceResult, error)) mutationprotocol.Result[pipeline.FlowAttachmentAdvanceResult] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite flow attachment progress", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	})
}

func advanceFlowAttachment(ctx context.Context, postgres bool, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time, run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (pipeline.FlowAttachmentAdvanceResult, error)) mutationprotocol.Result[pipeline.FlowAttachmentAdvanceResult]) (pipeline.FlowAttachmentAdvanceResult, error) {
	if err := attempt.Validate(); err != nil {
		return pipeline.FlowAttachmentAdvanceResult{}, err
	}
	next, err := previous.Next()
	if err != nil {
		return pipeline.FlowAttachmentAdvanceResult{}, err
	}
	if at.IsZero() {
		return pipeline.FlowAttachmentAdvanceResult{}, errors.New("attachment progress requires an occurrence time")
	}
	outcome := run(ctx, func(txctx context.Context, mutation *mutationprotocol.Attempt) (pipeline.FlowAttachmentAdvanceResult, error) {
		var progress pipeline.FlowAttachmentAdvanceResult
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			state, err := authorizeCurrentFlowActivationAttemptTx(txctx, tx, postgres, attempt)
			if errors.Is(err, pipeline.ErrFlowAttachmentStale) {
				progress.Progress = pipeline.FlowAttachmentStale
				return nil
			}
			if err != nil {
				return err
			}
			if state != "accepted" {
				progress.Progress = pipeline.FlowAttachmentStale
				return nil
			}
			current, found, err := loadDynamicFlowRuntimeReadiness(txctx, tx, postgres, attempt.RunID(), flowidentity.RouteForInstancePath(attempt.InstancePath()), true)
			if err != nil {
				return err
			}
			if !found || !current.Eligible() {
				progress.Progress = pipeline.FlowAttachmentIneligible
				progress.Terminal = found && current.Terminal()
				return nil
			}
			if current.AttemptOrdinal != attempt.Ordinal() || current.Plan.BundleHash != attempt.ProcessBinding().BundleHash {
				progress.Progress = pipeline.FlowAttachmentStale
				return nil
			}
			progress.Phase = current.Phase
			if progress.Phase.Includes(next) {
				progress.Progress = pipeline.FlowAttachmentAlreadyAdvanced
				return nil
			}
			if progress.Phase != previous {
				return fmt.Errorf("attachment progress cannot skip from %s through %s", progress.Phase, previous)
			}
			query := `UPDATE flow_instance_runtime_readiness SET phase=$1,updated_at=$2 WHERE run_id=$3::uuid AND instance_path=$4 AND activation_attempt_id=$5 AND phase=$6 AND plan_hash=$7`
			if !postgres {
				query = `UPDATE flow_instance_runtime_readiness SET phase=?,updated_at=? WHERE run_id=? AND instance_path=? AND activation_attempt_id=? AND phase=? AND plan_hash=?`
			}
			result, err := tx.ExecContext(txctx, query, string(next), at.UTC(), attempt.RunID(), attempt.InstancePath(), attempt.ID(), string(previous), current.PlanHash)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows != 1 {
				return errors.New("attachment progress lost its exact attempt/phase/plan")
			}
			progress.Progress, progress.Phase = pipeline.FlowAttachmentAdvanced, next
			return nil
		})
		return progress, err
	})
	result, acknowledged := outcome.Value()
	result.Acknowledged = acknowledged
	return result, standalonePipelineMutationError(outcome)
}
