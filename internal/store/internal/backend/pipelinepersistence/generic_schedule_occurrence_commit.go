package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privategenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
)

func commitGenericScheduleOccurrence(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	effects *revisionEffects,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	reserve func(context.Context) (*runLifecycleCandidateHandoffReservation, error),
	prepare func(*runLifecycleCandidateHandoffReservation, runtimerunlifecycle.CandidateRequestResult) error,
	requestCandidate func(context.Context, *sql.Tx, string) (runtimerunlifecycle.CandidateRequestResult, error),
	stampAcceptedAt func(context.Context, *sql.Tx) (time.Time, error),
	command runtimegenericschedule.CommitCommand,
) (runtimegenericschedule.CommitResult, error) {
	if err := command.ValidatePrepared(); err != nil {
		return runtimegenericschedule.CommitResult{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimegenericschedule.CommitResult{}, fmt.Errorf("generic schedule publication has unexpected type %T", command.Publication)
	}
	handoff, err := reserve(ctx)
	if err != nil {
		return runtimegenericschedule.CommitResult{}, err
	}
	defer handoff.Rollback()

	result := runtimegenericschedule.CommitResult{}
	err = run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		acceptedAt, err := stampAcceptedAt(txctx, tx)
		if err != nil {
			return fmt.Errorf("stamp generic schedule occurrence acceptance: %w", err)
		}
		command.AcceptedAt = acceptedAt
		if err := command.Validate(); err != nil {
			return err
		}
		persisted, found, err := privategenericschedule.LoadOccurrenceCommitStateTx(txctx, tx, postgres, command.Activation.ID)
		if err != nil {
			return err
		}
		if !found {
			result.Outcome = runtimegenericschedule.CommitTerminal
			return nil
		}
		if persisted.Status == runtimegenericschedule.StatusCancelled {
			result = runtimegenericschedule.CommitResult{Outcome: runtimegenericschedule.CommitStaleCancelled, Next: persisted}
			return nil
		}
		if persisted.ImmutableHash != command.Activation.ImmutableHash {
			result = runtimegenericschedule.CommitResult{Outcome: runtimegenericschedule.CommitTerminal, Next: persisted}
			return nil
		}

		currentOccurrence := persisted.Status == runtimegenericschedule.StatusActive &&
			persisted.CurrentDueAt.Equal(command.Occurrence.DueAt) &&
			persisted.CurrentEventID == command.Occurrence.EventID &&
			persisted.CurrentEventAdmittedAt.Equal(command.Occurrence.AdmittedAt)
		committedReplay := persisted.Status == runtimegenericschedule.StatusFired ||
			(persisted.Command.Due.Recurring() && persisted.CurrentDueAt.After(command.Occurrence.DueAt))
		if !currentOccurrence && !committedReplay {
			result = runtimegenericschedule.CommitResult{Outcome: runtimegenericschedule.CommitTerminal, Next: persisted}
			return nil
		}

		committed, err := store.commitPublicationTx(txctx, tx, story, effects, plan.PublicationCommand(), handoff)
		if err != nil {
			return fmt.Errorf("commit generic schedule publication: %w", err)
		}
		evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
		if err != nil {
			return err
		}
		if committedReplay {
			if committed.AppendOutcome != runtimebus.EventAppendExactDuplicate {
				return fmt.Errorf("generic schedule committed replay unexpectedly inserted event")
			}
			result = runtimegenericschedule.CommitResult{
				Outcome: runtimegenericschedule.CommitCommitted, Next: persisted, Publication: evidence,
				PublicationAlreadyCommitted: true,
			}
			return nil
		}
		if committed.AppendOutcome == runtimebus.EventAppendExactDuplicate {
			failed, failErr := privategenericschedule.FailActivationTx(
				txctx, tx, postgres, persisted, "event_identity_conflict",
				"active generic schedule occurrence already has an accepted event", command.AcceptedAt,
			)
			if failErr != nil {
				return failErr
			}
			if err := addTimerRevisionEffects(effects, persisted.Command.RunID); err != nil {
				return err
			}
			result = runtimegenericschedule.CommitResult{Outcome: runtimegenericschedule.CommitTerminal, Next: failed}
			return nil
		}
		if committed.AppendOutcome != runtimebus.EventAppendInserted {
			return fmt.Errorf("generic schedule publication returned invalid append outcome")
		}
		next, outcome, err := privategenericschedule.AdvanceOccurrenceTx(txctx, tx, postgres, command)
		if err != nil {
			return err
		}
		if outcome != runtimegenericschedule.CommitCommitted {
			return fmt.Errorf("generic schedule changed state after event admission")
		}
		if err := addTimerRevisionEffects(effects, next.Command.RunID); err != nil {
			return err
		}
		if next.Status == runtimegenericschedule.StatusFired && next.Command.RunID != "" {
			candidate, err := requestCandidate(txctx, tx, next.Command.RunID)
			if err != nil {
				return err
			}
			if err := prepare(handoff, candidate); err != nil {
				return err
			}
		}
		result = runtimegenericschedule.CommitResult{Outcome: outcome, Next: next, Publication: evidence}
		return nil
	})
	if err != nil {
		return runtimegenericschedule.CommitResult{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimegenericschedule.CommitResult{}, err
	}
	if err := handoff.Commit(); err != nil {
		return runtimegenericschedule.CommitResult{}, err
	}
	return result, nil
}

func (s *PipelinePostgresOwner) CommitGenericScheduleOccurrence(ctx context.Context, command runtimegenericschedule.CommitCommand) (runtimegenericschedule.CommitResult, error) {
	effects := newRevisionEffects()
	return commitGenericScheduleOccurrence(
		ctx, s, true, effects,
		func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, effects, fn)
		},
		reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		},
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestPostgresCompletionCandidateTx(ctx, tx, runID, nil, false)
		}, func(ctx context.Context, tx *sql.Tx) (time.Time, error) {
			return privategenericschedule.SelectedStoreTimeTx(ctx, tx, true, nil)
		}, command,
	)
}

func (s *PipelineSQLiteOwner) CommitGenericScheduleOccurrence(ctx context.Context, command runtimegenericschedule.CommitCommand) (runtimegenericschedule.CommitResult, error) {
	effects := newRevisionEffects()
	return commitGenericScheduleOccurrence(
		ctx, s, false, effects,
		func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, "sqlite generic schedule occurrence", effects, fn)
		},
		reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		},
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestSQLiteCompletionCandidateTx(ctx, tx, runID, nil, s.now(), false)
		}, func(ctx context.Context, tx *sql.Tx) (time.Time, error) {
			return privategenericschedule.SelectedStoreTimeTx(ctx, tx, false, s.now)
		}, command,
	)
}
