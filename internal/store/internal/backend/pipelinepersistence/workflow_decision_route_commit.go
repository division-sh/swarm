package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

type workflowDecisionRouteTxOwner interface {
	CompleteProposedEffectRouteTx(context.Context, *sql.Tx, string, string, time.Time) (decisioncard.ProposedEffectContinuation, bool, error)
	CompleteHumanTaskOutcomeTx(context.Context, *sql.Tx, string, string, time.Time) (decisioncard.HumanTaskContinuation, bool, error)
}

func commitProposedEffectRoute(
	ctx context.Context,
	store eventCommitTxStore,
	decisions workflowDecisionRouteTxOwner,
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
	defer handoff.Rollback()

	var result runtimepipeline.CommittedProposedEffectRoute
	err = run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		committed, err := store.commitPublicationTx(txctx, tx, story, plan.PublicationCommand(), handoff)
		if err != nil {
			return fmt.Errorf("commit proposed-effect route publication: %w", err)
		}
		continuation, changed, err := decisions.CompleteProposedEffectRouteTx(txctx, tx, command.CardID, command.RouteEventID, command.OccurredAt)
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
	if err := handoff.Commit(); err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	return result, nil
}

func commitHumanTaskRoute(
	ctx context.Context,
	store eventCommitTxStore,
	decisions workflowDecisionRouteTxOwner,
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
		committed, err := store.commitPublicationTx(txctx, tx, story, plan.PublicationCommand(), nil)
		if err != nil {
			return fmt.Errorf("commit human-task route publication: %w", err)
		}
		if completeOutcome {
			if _, _, err := decisions.CompleteHumanTaskOutcomeTx(txctx, tx, cardID, routeEventID, occurredAt); err != nil {
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

func (s *PipelinePostgresOwner) CommitHumanTaskDeferredRoute(ctx context.Context, command runtimepipeline.HumanTaskDeferredRouteCommand) (runtimepipeline.CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("human-task deferred route publication has unexpected type %T", command.Publication)
	}
	effects := newRevisionEffects()
	if err := addPublicationRevisionEffects(effects, command.Publication); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	return commitHumanTaskRoute(ctx, s, s.DecisionPostgresOwner, true, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, effects, fn)
	}, plan, command.CardID, command.RouteEventID, command.OccurredAt, false)
}

func (s *PipelineSQLiteOwner) CommitHumanTaskDeferredRoute(ctx context.Context, command runtimepipeline.HumanTaskDeferredRouteCommand) (runtimepipeline.CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("human-task deferred route publication has unexpected type %T", command.Publication)
	}
	effects := newRevisionEffects()
	if err := addPublicationRevisionEffects(effects, command.Publication); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	return commitHumanTaskRoute(ctx, s, s.DecisionSQLiteOwner, false, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite human-task deferred route", effects, fn)
	}, plan, command.CardID, command.RouteEventID, command.OccurredAt, false)
}

func (s *PipelinePostgresOwner) CommitHumanTaskOutcomeRoute(ctx context.Context, command runtimepipeline.HumanTaskOutcomeRouteCommand) (runtimepipeline.CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("human-task outcome route publication has unexpected type %T", command.Publication)
	}
	effects := newRevisionEffects()
	if err := addPublicationRevisionEffects(effects, command.Publication); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	return commitHumanTaskRoute(ctx, s, s.DecisionPostgresOwner, true, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, effects, fn)
	}, plan, command.CardID, command.RouteEventID, command.OccurredAt, true)
}

func (s *PipelineSQLiteOwner) CommitHumanTaskOutcomeRoute(ctx context.Context, command runtimepipeline.HumanTaskOutcomeRouteCommand) (runtimepipeline.CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("human-task outcome route publication has unexpected type %T", command.Publication)
	}
	effects := newRevisionEffects()
	if err := addPublicationRevisionEffects(effects, command.Publication); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	return commitHumanTaskRoute(ctx, s, s.DecisionSQLiteOwner, false, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite human-task outcome route", effects, fn)
	}, plan, command.CardID, command.RouteEventID, command.OccurredAt, true)
}

func (s *PipelinePostgresOwner) CommitProposedEffectRoute(ctx context.Context, command runtimepipeline.ProposedEffectRouteCommand) (runtimepipeline.CommittedProposedEffectRoute, error) {
	effects := newRevisionEffects()
	if err := addPublicationRevisionEffects(effects, command.Publication); err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	return commitProposedEffectRoute(ctx, s, s.DecisionPostgresOwner, true, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, effects, fn)
	}, reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		}, func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestPostgresCompletionCandidateTx(ctx, tx, runID, nil, false)
		}, command)
}

func (s *PipelineSQLiteOwner) CommitProposedEffectRoute(ctx context.Context, command runtimepipeline.ProposedEffectRouteCommand) (runtimepipeline.CommittedProposedEffectRoute, error) {
	effects := newRevisionEffects()
	if err := addPublicationRevisionEffects(effects, command.Publication); err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	return commitProposedEffectRoute(ctx, s, s.DecisionSQLiteOwner, false,
		func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, "sqlite proposed-effect route", effects, fn)
		}, reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		}, func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestSQLiteCompletionCandidateTx(ctx, tx, runID, nil, s.now(), false)
		}, command)
}

var _ runtimepipeline.WorkflowDecisionRouteOwner = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowDecisionRouteOwner = (*PipelineSQLiteOwner)(nil)
