package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/authoractivity"
)

func commitProposedEffectRoute(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	reserve func(context.Context) (*runLifecycleCandidateHandoffReservation, error),
	prepare func(*runLifecycleCandidateHandoffReservation, runtimerunlifecycle.CandidateRequestResult) error,
	requestCandidate func(context.Context, *sql.Tx, string) (runtimerunlifecycle.CandidateRequestResult, error),
	command runtimepipeline.ProposedEffectRouteCommand,
) (runtimepipeline.CommittedProposedEffectRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedProposedEffectRoute{}, fmt.Errorf("proposed-effect route publication has unexpected type %T", command.Publication)
	}
	handoff, err := reserve(ctx)
	if err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	defer handoff.rollback()

	var result runtimepipeline.CommittedProposedEffectRoute
	err = run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		committed, err := commitPublicationTx(txctx, tx, story, store, postgres, plan.PublicationCommand(), handoff)
		if err != nil {
			return fmt.Errorf("commit proposed-effect route publication: %w", err)
		}
		continuation, changed, err := completeProposedEffectRoute(txctx, tx, command.CardID, command.RouteEventID, command.OccurredAt, postgres)
		if err != nil {
			return err
		}
		if changed {
			candidate, err := requestCandidate(txctx, tx, continuation.RunID)
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
		result.Publication = evidence
		return nil
	})
	if err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	if err := handoff.commit(); err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	return result, nil
}

func commitHumanTaskRoute(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	plan runtimebus.EnginePublicationPlan,
	cardID string,
	routeEventID string,
	occurredAt time.Time,
	completeOutcome bool,
) (runtimepipeline.CommittedHumanTaskRoute, error) {
	var result runtimepipeline.CommittedHumanTaskRoute
	err := run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		committed, err := commitPublicationTx(txctx, tx, story, store, postgres, plan.PublicationCommand(), nil)
		if err != nil {
			return fmt.Errorf("commit human-task route publication: %w", err)
		}
		if completeOutcome {
			if _, _, err := completeHumanTaskOutcome(txctx, tx, cardID, routeEventID, occurredAt, postgres); err != nil {
				return fmt.Errorf("complete human-task route: %w", err)
			}
		}
		evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
		if err != nil {
			return err
		}
		result.Publication = evidence
		return nil
	})
	if err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	return result, nil
}

func (s *PostgresStore) CommitHumanTaskDeferredRoute(ctx context.Context, command runtimepipeline.HumanTaskDeferredRouteCommand) (runtimepipeline.CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("human-task deferred route publication has unexpected type %T", command.Publication)
	}
	return commitHumanTaskRoute(ctx, s, true, s.runPrivateAuthorActivityMutation, plan, command.CardID, command.RouteEventID, command.OccurredAt, false)
}

func (s *SQLiteRuntimeStore) CommitHumanTaskDeferredRoute(ctx context.Context, command runtimepipeline.HumanTaskDeferredRouteCommand) (runtimepipeline.CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("human-task deferred route publication has unexpected type %T", command.Publication)
	}
	return commitHumanTaskRoute(ctx, s, false, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite human-task deferred route", fn)
	}, plan, command.CardID, command.RouteEventID, command.OccurredAt, false)
}

func (s *PostgresStore) CommitHumanTaskOutcomeRoute(ctx context.Context, command runtimepipeline.HumanTaskOutcomeRouteCommand) (runtimepipeline.CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("human-task outcome route publication has unexpected type %T", command.Publication)
	}
	return commitHumanTaskRoute(ctx, s, true, s.runPrivateAuthorActivityMutation, plan, command.CardID, command.RouteEventID, command.OccurredAt, true)
}

func (s *SQLiteRuntimeStore) CommitHumanTaskOutcomeRoute(ctx context.Context, command runtimepipeline.HumanTaskOutcomeRouteCommand) (runtimepipeline.CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("human-task outcome route publication has unexpected type %T", command.Publication)
	}
	return commitHumanTaskRoute(ctx, s, false, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite human-task outcome route", fn)
	}, plan, command.CardID, command.RouteEventID, command.OccurredAt, true)
}

func (s *PostgresStore) CommitProposedEffectRoute(ctx context.Context, command runtimepipeline.ProposedEffectRouteCommand) (runtimepipeline.CommittedProposedEffectRoute, error) {
	return commitProposedEffectRoute(ctx, s, true, s.runPrivateAuthorActivityMutation, reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.prepare(&s.runLifecycleSinks, result)
		}, func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestPostgresCompletionCandidateTx(ctx, tx, runID, nil, false)
		}, command)
}

func (s *SQLiteRuntimeStore) CommitProposedEffectRoute(ctx context.Context, command runtimepipeline.ProposedEffectRouteCommand) (runtimepipeline.CommittedProposedEffectRoute, error) {
	return commitProposedEffectRoute(ctx, s, false,
		func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, "sqlite proposed-effect route", fn)
		}, reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.prepare(&s.runLifecycleSinks, result)
		}, func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestSQLiteCompletionCandidateTx(ctx, tx, runID, nil, s.now(), false)
		}, command)
}

var _ runtimepipeline.WorkflowDecisionRouteOwner = (*PostgresStore)(nil)
var _ runtimepipeline.WorkflowDecisionRouteOwner = (*SQLiteRuntimeStore)(nil)
