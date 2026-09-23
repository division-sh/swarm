package pipelinepersistence

import (
	"context"
	"errors"
	"fmt"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type workflowDecisionRouteTxOwner interface {
	CompleteProposedEffectRouteTx(context.Context, *mutationprotocol.Attempt, string, string, time.Time) (decisioncard.ProposedEffectContinuation, bool, error)
	CompleteHumanTaskOutcomeTx(context.Context, *mutationprotocol.Attempt, string, string, time.Time) (decisioncard.HumanTaskContinuation, bool, error)
}

func commitProposedEffectRoute(
	ctx context.Context,
	store eventCommitTxStore,
	decisions workflowDecisionRouteTxOwner,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedProposedEffectRoute, error)) mutationprotocol.Result[runtimepipeline.CommittedProposedEffectRoute],
	candidateWriter mutationprotocol.CandidateWriter,
	command runtimepipeline.ProposedEffectRouteCommand,
) (runtimepipeline.CommittedProposedEffectRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedProposedEffectRoute{}, fmt.Errorf("proposed-effect route publication has unexpected type %T", command.Publication)
	}
	outcome := run(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.CommittedProposedEffectRoute, error) {
		committed, err := store.commitPublicationTx(txctx, attempt, plan.PublicationCommand())
		if err != nil {
			return runtimepipeline.CommittedProposedEffectRoute{}, fmt.Errorf("commit proposed-effect route publication: %w", err)
		}
		continuation, changed, err := decisions.CompleteProposedEffectRouteTx(txctx, attempt, command.CardID, command.RouteEventID, command.OccurredAt)
		if err != nil {
			return runtimepipeline.CommittedProposedEffectRoute{}, err
		}
		if changed {
			if _, err := attempt.RequestCompletion(txctx, candidateWriter, continuation.RunID, nil); err != nil {
				return runtimepipeline.CommittedProposedEffectRoute{}, err
			}
		}
		evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
		if err != nil {
			return runtimepipeline.CommittedProposedEffectRoute{}, err
		}
		return runtimepipeline.CommittedProposedEffectRoute{Publication: evidence}, nil
	})
	result, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimepipeline.CommittedProposedEffectRoute{}, outcome.Err()
	}
	return result, errors.Join(outcome.Err(), result.Validate())
}

func commitHumanTaskRoute(
	ctx context.Context,
	store eventCommitTxStore,
	decisions workflowDecisionRouteTxOwner,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskRoute, error)) mutationprotocol.Result[runtimepipeline.CommittedHumanTaskRoute],
	plan runtimebus.EnginePublicationPlan,
	cardID string,
	routeEventID string,
	occurredAt time.Time,
	completeOutcome bool,
) (runtimepipeline.CommittedHumanTaskRoute, error) {
	outcome := run(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskRoute, error) {
		committed, err := store.commitPublicationTx(txctx, attempt, plan.PublicationCommand())
		if err != nil {
			return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("commit human-task route publication: %w", err)
		}
		if completeOutcome {
			if _, _, err := decisions.CompleteHumanTaskOutcomeTx(txctx, attempt, cardID, routeEventID, occurredAt); err != nil {
				return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("complete human-task route: %w", err)
			}
		}
		evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
		if err != nil {
			return runtimepipeline.CommittedHumanTaskRoute{}, err
		}
		return runtimepipeline.CommittedHumanTaskRoute{Publication: evidence}, nil
	})
	result, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimepipeline.CommittedHumanTaskRoute{}, outcome.Err()
	}
	return result, errors.Join(outcome.Err(), result.Validate())
}

func (s *PipelinePostgresOwner) CommitHumanTaskDeferredRoute(ctx context.Context, command runtimepipeline.HumanTaskDeferredRouteCommand) (runtimepipeline.CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedHumanTaskRoute{}, fmt.Errorf("human-task deferred route publication has unexpected type %T", command.Publication)
	}
	return commitHumanTaskRoute(ctx, s, s.DecisionPostgresOwner, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskRoute, error)) mutationprotocol.Result[runtimepipeline.CommittedHumanTaskRoute] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
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
	return commitHumanTaskRoute(ctx, s, s.DecisionSQLiteOwner, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskRoute, error)) mutationprotocol.Result[runtimepipeline.CommittedHumanTaskRoute] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite human-task deferred route", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
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
	return commitHumanTaskRoute(ctx, s, s.DecisionPostgresOwner, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskRoute, error)) mutationprotocol.Result[runtimepipeline.CommittedHumanTaskRoute] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
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
	return commitHumanTaskRoute(ctx, s, s.DecisionSQLiteOwner, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskRoute, error)) mutationprotocol.Result[runtimepipeline.CommittedHumanTaskRoute] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite human-task outcome route", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	}, plan, command.CardID, command.RouteEventID, command.OccurredAt, true)
}

func (s *PipelinePostgresOwner) CommitProposedEffectRoute(ctx context.Context, command runtimepipeline.ProposedEffectRouteCommand) (runtimepipeline.CommittedProposedEffectRoute, error) {
	return commitProposedEffectRoute(ctx, s, s.DecisionPostgresOwner, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedProposedEffectRoute, error)) mutationprotocol.Result[runtimepipeline.CommittedProposedEffectRoute] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, write)
	}, s.RunLifecyclePostgresOwner, command)
}

func (s *PipelineSQLiteOwner) CommitProposedEffectRoute(ctx context.Context, command runtimepipeline.ProposedEffectRouteCommand) (runtimepipeline.CommittedProposedEffectRoute, error) {
	return commitProposedEffectRoute(ctx, s, s.DecisionSQLiteOwner, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedProposedEffectRoute, error)) mutationprotocol.Result[runtimepipeline.CommittedProposedEffectRoute] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite proposed-effect route", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, write)
	}, s.RunLifecycleSQLiteOwner, command)
}

var _ runtimepipeline.WorkflowDecisionRouteOwner = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowDecisionRouteOwner = (*PipelineSQLiteOwner)(nil)
