package pipelinepersistence

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type humanTaskExpiryTxOwner interface {
	ExpireHumanTasksTx(context.Context, *mutationprotocol.Attempt, time.Time, int) ([]events.Event, error)
}

func commitHumanTaskExpirations(
	ctx context.Context,
	store eventCommitTxStore,
	decisions humanTaskExpiryTxOwner,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskExpiry, error)) mutationprotocol.Result[runtimepipeline.CommittedHumanTaskExpiry],
	command runtimepipeline.HumanTaskExpiryCommand,
) (runtimepipeline.CommittedHumanTaskExpiry, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskExpiry{}, err
	}
	plans := make([]runtimebus.EnginePublicationPlan, len(command.Publications))
	for index, publication := range command.Publications {
		plan, ok := publication.(runtimebus.EnginePublicationPlan)
		if !ok {
			return runtimepipeline.CommittedHumanTaskExpiry{}, fmt.Errorf("human-task expiry publication %d has unexpected type %T", index, publication)
		}
		plans[index] = plan
	}
	outcome := run(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskExpiry, error) {
		result := runtimepipeline.CommittedHumanTaskExpiry{Publications: make([]runtimeengine.CommittedDurablePublication, 0, len(plans))}
		expired, err := decisions.ExpireHumanTasksTx(txctx, attempt, command.ObservedAt, command.Limit)
		if err != nil {
			return runtimepipeline.CommittedHumanTaskExpiry{}, err
		}
		if len(expired) != len(plans) {
			return runtimepipeline.CommittedHumanTaskExpiry{}, fmt.Errorf("human-task expiry authority changed before commit: due=%d planned=%d", len(expired), len(plans))
		}
		for index, plan := range plans {
			if strings.TrimSpace(expired[index].ID()) != strings.TrimSpace(plan.DurablePublicationEventID()) {
				return runtimepipeline.CommittedHumanTaskExpiry{}, fmt.Errorf("human-task expiry authority changed before commit at index %d", index)
			}
			committed, err := store.commitPublicationTx(txctx, attempt, plan.PublicationCommand())
			if err != nil {
				return runtimepipeline.CommittedHumanTaskExpiry{}, fmt.Errorf("commit human-task expiry publication %d: %w", index, err)
			}
			evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
			if err != nil {
				return runtimepipeline.CommittedHumanTaskExpiry{}, err
			}
			result.Publications = append(result.Publications, evidence)
		}
		return result, nil
	})
	result, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimepipeline.CommittedHumanTaskExpiry{}, outcome.Err()
	}
	result.Acknowledged = true
	return result, errors.Join(outcome.Err(), result.Validate())
}

func (s *PipelinePostgresOwner) CommitHumanTaskExpirations(ctx context.Context, command runtimepipeline.HumanTaskExpiryCommand) (runtimepipeline.CommittedHumanTaskExpiry, error) {
	return commitHumanTaskExpirations(ctx, s, s.DecisionPostgresOwner, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskExpiry, error)) mutationprotocol.Result[runtimepipeline.CommittedHumanTaskExpiry] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	}, command)
}

func (s *PipelineSQLiteOwner) CommitHumanTaskExpirations(ctx context.Context, command runtimepipeline.HumanTaskExpiryCommand) (runtimepipeline.CommittedHumanTaskExpiry, error) {
	return commitHumanTaskExpirations(ctx, s, s.DecisionSQLiteOwner, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskExpiry, error)) mutationprotocol.Result[runtimepipeline.CommittedHumanTaskExpiry] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite human-task expiry", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	}, command)
}

var _ runtimepipeline.HumanTaskExpiry = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.HumanTaskExpiry = (*PipelineSQLiteOwner)(nil)
