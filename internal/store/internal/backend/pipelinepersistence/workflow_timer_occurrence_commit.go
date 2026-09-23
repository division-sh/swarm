package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func commitWorkflowTimerOccurrence(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedWorkflowTimerOccurrence, error)) mutationprotocol.Result[runtimepipeline.CommittedWorkflowTimerOccurrence],
	candidateWriter mutationprotocol.CandidateWriter,
	command runtimepipeline.WorkflowTimerOccurrenceCommand,
) (runtimepipeline.CommittedWorkflowTimerOccurrence, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, fmt.Errorf("workflow timer publication has unexpected type %T", command.Publication)
	}
	outcome := run(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.CommittedWorkflowTimerOccurrence, error) {
		result := runtimepipeline.CommittedWorkflowTimerOccurrence{}
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			activation, found, err := loadWorkflowEngineTimerActivation(txctx, tx, postgres, command.Activation.Ref)
			if err != nil {
				return err
			}
			if !found || activation.Status != "active" || !activation.FireAt.Equal(command.Occurrence.DueAt) {
				result.Outcome = runtimepipeline.WorkflowTimerOccurrenceTerminal
				return nil
			}
			if !sameWorkflowEngineTimerActivation(activation, command.Activation) || activation.Ref != command.Occurrence.Activation {
				return fmt.Errorf("workflow timer occurrence persisted coordinate changed before commit")
			}
			if postgres {
				err = requirePostgresRunActive(txctx, tx, activation.RunID)
			} else {
				err = requireSQLiteRunActive(txctx, tx, activation.RunID)
			}
			if errors.Is(err, runtimerunlifecycle.ErrRunNotActive) {
				result.Outcome = runtimepipeline.WorkflowTimerOccurrenceTerminal
				return nil
			}
			if err != nil {
				return err
			}

			committed, err := store.commitPublicationTx(txctx, attempt, plan.PublicationCommand())
			if err != nil {
				return fmt.Errorf("commit workflow timer publication: %w", err)
			}
			if committed.AppendOutcome == runtimebus.EventAppendExactDuplicate {
				return fmt.Errorf("active workflow timer occurrence already has a persisted event")
			}
			if committed.AppendOutcome != runtimebus.EventAppendInserted {
				return fmt.Errorf("workflow timer publication returned invalid append outcome")
			}

			next, err := advanceWorkflowEngineTimerOccurrence(txctx, tx, postgres, attempt, activation, command.FiredAt)
			if err != nil {
				return err
			}
			if !activation.Recurring {
				if _, err := attempt.RequestCompletion(txctx, candidateWriter, activation.RunID, nil); err != nil {
					return err
				}
			}
			evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
			if err != nil {
				return err
			}
			result = runtimepipeline.CommittedWorkflowTimerOccurrence{
				Outcome:     runtimepipeline.WorkflowTimerOccurrenceCommitted,
				Next:        next,
				Publication: evidence,
			}
			return nil
		})
		return result, err
	})
	result, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, outcome.Err()
	}
	return result, errors.Join(outcome.Err(), result.Validate())
}

func advanceWorkflowEngineTimerOccurrence(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	facts workflowTimerFactSink,
	activation runtimepipeline.WorkflowTimerActivation,
	firedAt time.Time,
) (runtimepipeline.WorkflowTimerActivation, error) {
	next := activation.Canonical()
	next.FiredAt = firedAt.UTC()
	nextStatus := "fired"
	if activation.Recurring {
		nextStatus = "active"
		next.FireAt = activation.FireAt.Add(activation.RecurrenceInterval).UTC()
	}
	query := `
		UPDATE timers SET status = ?, fired_at = ?, fire_at = ?
		WHERE timer_id = ? AND task_type = 'workflow_timer' AND status = 'active' AND fire_at = ?
	`
	args := []any{nextStatus, next.FiredAt, next.FireAt, activation.Ref.ActivationID, activation.FireAt}
	storedRunID, storedTimerID := activation.RunID, activation.Ref.ActivationID
	var rows int64
	var err error
	if postgres {
		query = `
			UPDATE timers SET status = $1, fired_at = $2, fire_at = $3
			WHERE timer_id = $4::uuid AND task_type = 'workflow_timer' AND status = 'active' AND fire_at = $5
			RETURNING CAST(run_id AS TEXT), CAST(timer_id AS TEXT)
		`
		err = tx.QueryRowContext(ctx, query, args...).Scan(&storedRunID, &storedTimerID)
		if err == nil {
			rows = 1
		}
	} else {
		var updated sql.Result
		updated, err = tx.ExecContext(ctx, query, args...)
		if err == nil {
			rows, err = updated.RowsAffected()
		}
	}
	if err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, fmt.Errorf("advance workflow timer occurrence: %w", err)
	}
	if rows != 1 {
		return runtimepipeline.WorkflowTimerActivation{}, fmt.Errorf("workflow timer occurrence advanced %d rows", rows)
	}
	if err := facts.AddFact(storedRunID, privaterunforkrevision.FamilyTimers, storedTimerID); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, err
	}
	next.Status = nextStatus
	next = next.Canonical()
	if err := next.Validate(); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, err
	}
	return next, nil
}

func (s *PipelinePostgresOwner) CommitWorkflowTimerOccurrence(ctx context.Context, command runtimepipeline.WorkflowTimerOccurrenceCommand) (runtimepipeline.CommittedWorkflowTimerOccurrence, error) {
	return commitWorkflowTimerOccurrence(ctx, s, true, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedWorkflowTimerOccurrence, error)) mutationprotocol.Result[runtimepipeline.CommittedWorkflowTimerOccurrence] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, write)
	}, s.RunLifecyclePostgresOwner, command)
}

func (s *PipelineSQLiteOwner) CommitWorkflowTimerOccurrence(ctx context.Context, command runtimepipeline.WorkflowTimerOccurrenceCommand) (runtimepipeline.CommittedWorkflowTimerOccurrence, error) {
	return commitWorkflowTimerOccurrence(ctx, s, false, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedWorkflowTimerOccurrence, error)) mutationprotocol.Result[runtimepipeline.CommittedWorkflowTimerOccurrence] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite workflow timer occurrence", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, write)
	}, s.RunLifecycleSQLiteOwner, command)
}

var _ runtimepipeline.WorkflowTimerOccurrenceOwner = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowTimerOccurrenceOwner = (*PipelineSQLiteOwner)(nil)
