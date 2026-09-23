package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	privategenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func commitGenericScheduleOccurrence(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (runtimegenericschedule.CommitResult, error)) mutationprotocol.Result[runtimegenericschedule.CommitResult],
	candidateWriter mutationprotocol.CandidateWriter,
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
	outcome := run(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.CommitResult, error) {
		result := runtimegenericschedule.CommitResult{}
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			acceptedAt, err := stampAcceptedAt(txctx, tx)
			if err != nil {
				return fmt.Errorf("stamp generic schedule occurrence acceptance: %w", err)
			}
			command.AcceptedAt = acceptedAt
			if err := command.Validate(); err != nil {
				return err
			}
			persisted, found, err := privategenericschedule.LoadOccurrenceCommitStateTx(txctx, attempt, postgres, command.Activation.ID)
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

			committed, err := store.commitPublicationTx(txctx, attempt, plan.PublicationCommand())
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
					txctx, attempt, postgres, persisted, "event_identity_conflict",
					"active generic schedule occurrence already has an accepted event", command.AcceptedAt,
				)
				if failErr != nil {
					return failErr
				}
				if err := attempt.AddFact(persisted.Command.RunID, privaterunforkrevision.FamilyTimers, persisted.ID); err != nil {
					return err
				}
				result = runtimegenericschedule.CommitResult{Outcome: runtimegenericschedule.CommitTerminal, Next: failed}
				return nil
			}
			if committed.AppendOutcome != runtimebus.EventAppendInserted {
				return fmt.Errorf("generic schedule publication returned invalid append outcome")
			}
			next, outcome, err := privategenericschedule.AdvanceOccurrenceTx(txctx, attempt, postgres, command)
			if err != nil {
				return err
			}
			if outcome != runtimegenericschedule.CommitCommitted {
				return fmt.Errorf("generic schedule changed state after event admission")
			}
			if err := attempt.AddFact(next.Command.RunID, privaterunforkrevision.FamilyTimers, next.ID); err != nil {
				return err
			}
			if next.Status == runtimegenericschedule.StatusFired && next.Command.RunID != "" {
				if _, err := attempt.RequestCompletion(txctx, candidateWriter, next.Command.RunID, nil); err != nil {
					return err
				}
			}
			result = runtimegenericschedule.CommitResult{Outcome: outcome, Next: next, Publication: evidence}
			return nil
		})
		return result, err
	})
	result, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimegenericschedule.CommitResult{}, outcome.Err()
	}
	return result, errors.Join(outcome.Err(), result.Validate())
}

func (s *PipelinePostgresOwner) CommitGenericScheduleOccurrence(ctx context.Context, command runtimegenericschedule.CommitCommand) (runtimegenericschedule.CommitResult, error) {
	return commitGenericScheduleOccurrence(
		ctx, s, true,
		func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimegenericschedule.CommitResult, error)) mutationprotocol.Result[runtimegenericschedule.CommitResult] {
			return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, write)
		},
		s.RunLifecyclePostgresOwner, func(ctx context.Context, tx *sql.Tx) (time.Time, error) {
			return privategenericschedule.SelectedStoreTimeTx(ctx, tx, true, nil)
		}, command,
	)
}

func (s *PipelineSQLiteOwner) CommitGenericScheduleOccurrence(ctx context.Context, command runtimegenericschedule.CommitCommand) (runtimegenericschedule.CommitResult, error) {
	return commitGenericScheduleOccurrence(
		ctx, s, false,
		func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimegenericschedule.CommitResult, error)) mutationprotocol.Result[runtimegenericschedule.CommitResult] {
			return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite generic schedule occurrence", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, write)
		},
		s.RunLifecycleSQLiteOwner, func(ctx context.Context, tx *sql.Tx) (time.Time, error) {
			return privategenericschedule.SelectedStoreTimeTx(ctx, tx, false, s.now)
		}, command,
	)
}
