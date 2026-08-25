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
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

func commitWorkflowTimerOccurrence(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	reserve func(context.Context) (*runLifecycleCandidateHandoffReservation, error),
	prepare func(*runLifecycleCandidateHandoffReservation, runtimerunlifecycle.CandidateRequestResult) error,
	requestCandidate func(context.Context, *sql.Tx, string) (runtimerunlifecycle.CandidateRequestResult, error),
	command runtimepipeline.WorkflowTimerOccurrenceCommand,
) (runtimepipeline.CommittedWorkflowTimerOccurrence, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, fmt.Errorf("workflow timer publication has unexpected type %T", command.Publication)
	}
	handoff, err := reserve(ctx)
	if err != nil {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, err
	}
	defer handoff.Rollback()

	result := runtimepipeline.CommittedWorkflowTimerOccurrence{}
	err = run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
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

		committed, err := store.commitPublicationTx(txctx, tx, story, plan.PublicationCommand(), handoff)
		if err != nil {
			return fmt.Errorf("commit workflow timer publication: %w", err)
		}
		if committed.AppendOutcome == runtimebus.EventAppendExactDuplicate {
			return fmt.Errorf("active workflow timer occurrence already has a persisted event")
		}
		if committed.AppendOutcome != runtimebus.EventAppendInserted {
			return fmt.Errorf("workflow timer publication returned invalid append outcome")
		}

		next, err := advanceWorkflowEngineTimerOccurrence(txctx, tx, postgres, activation, command.FiredAt)
		if err != nil {
			return err
		}
		if !activation.Recurring {
			candidate, err := requestCandidate(txctx, tx, activation.RunID)
			if err != nil {
				return err
			}
			if err := prepare(handoff, candidate); err != nil {
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
	if err != nil {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, err
	}
	if err := handoff.Commit(); err != nil {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, err
	}
	return result, nil
}

func advanceWorkflowEngineTimerOccurrence(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
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
	if postgres {
		query = `
			UPDATE timers SET status = $1, fired_at = $2, fire_at = $3
			WHERE timer_id = $4::uuid AND task_type = 'workflow_timer' AND status = 'active' AND fire_at = $5
		`
	}
	updated, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, fmt.Errorf("advance workflow timer occurrence: %w", err)
	}
	rows, err := updated.RowsAffected()
	if err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, err
	}
	if rows != 1 {
		return runtimepipeline.WorkflowTimerActivation{}, fmt.Errorf("workflow timer occurrence advanced %d rows", rows)
	}
	next.Status = nextStatus
	next = next.Canonical()
	if err := next.Validate(); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, err
	}
	return next, nil
}

func (s *PipelinePostgresOwner) CommitWorkflowTimerOccurrence(ctx context.Context, command runtimepipeline.WorkflowTimerOccurrenceCommand) (runtimepipeline.CommittedWorkflowTimerOccurrence, error) {
	effects := newRevisionEffects()
	if err := addTimerRevisionEffects(effects, command.Activation.RunID); err != nil {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, err
	}
	if err := addPublicationRevisionEffects(effects, command.Publication); err != nil {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, err
	}
	return commitWorkflowTimerOccurrence(
		ctx,
		s,
		true,
		func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, effects, fn)
		},
		reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		},
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestPostgresCompletionCandidateTx(ctx, tx, runID, nil, false)
		},
		command,
	)
}

func (s *PipelineSQLiteOwner) CommitWorkflowTimerOccurrence(ctx context.Context, command runtimepipeline.WorkflowTimerOccurrenceCommand) (runtimepipeline.CommittedWorkflowTimerOccurrence, error) {
	effects := newRevisionEffects()
	if err := addTimerRevisionEffects(effects, command.Activation.RunID); err != nil {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, err
	}
	if err := addPublicationRevisionEffects(effects, command.Publication); err != nil {
		return runtimepipeline.CommittedWorkflowTimerOccurrence{}, err
	}
	return commitWorkflowTimerOccurrence(
		ctx,
		s,
		false,
		func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, "sqlite workflow timer occurrence", effects, fn)
		},
		reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		},
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestSQLiteCompletionCandidateTx(ctx, tx, runID, nil, s.now(), false)
		},
		command,
	)
}

var _ runtimepipeline.WorkflowTimerOccurrenceOwner = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowTimerOccurrenceOwner = (*PipelineSQLiteOwner)(nil)
