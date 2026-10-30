package pipelinepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type humanTaskExpiryTxOwner interface {
	ExpireHumanTasksTx(context.Context, *mutationprotocol.Attempt, time.Time, int, []string) ([]events.Event, error)
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
	cardIDs := make([]string, len(command.Publications))
	for index, publication := range command.Publications {
		plan, ok := publication.(runtimebus.EnginePublicationPlan)
		if !ok {
			return runtimepipeline.CommittedHumanTaskExpiry{}, fmt.Errorf("human-task expiry publication %d has unexpected type %T", index, publication)
		}
		plans[index] = plan
		event := plan.PublicationCommand().Commit.Event.Event()
		var payload struct {
			CardID string `json:"card_id"`
		}
		if event.Type() != "mailbox.card_expired" || json.Unmarshal(event.Payload(), &payload) != nil || payload.CardID == "" {
			return runtimepipeline.CommittedHumanTaskExpiry{}, runtimefailures.New(runtimefailures.ClassInternalFailure,
				"human_task_expiry_invalid_publication", "workflow-runtime", "expire_human_task_cards", nil)
		}
		cardIDs[index] = payload.CardID
	}
	outcome := run(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskExpiry, error) {
		result := runtimepipeline.CommittedHumanTaskExpiry{Publications: make([]runtimeengine.CommittedDurablePublication, 0, len(plans))}
		expired, err := decisions.ExpireHumanTasksTx(txctx, attempt, command.ObservedAt, command.Limit, cardIDs)
		if err != nil {
			return runtimepipeline.CommittedHumanTaskExpiry{}, err
		}
		if len(expired) != len(plans) {
			return runtimepipeline.CommittedHumanTaskExpiry{}, runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "human_task_expiry_due_set_changed", "workflow-runtime", "expire_human_task_cards",
				map[string]any{"stage": "commit", "due": len(expired), "planned": len(plans)},
				fmt.Errorf("human-task expiry authority changed before commit: due=%d planned=%d", len(expired), len(plans)))
		}
		for index, plan := range plans {
			if strings.TrimSpace(expired[index].ID()) != strings.TrimSpace(plan.DurablePublicationEventID()) {
				return runtimepipeline.CommittedHumanTaskExpiry{}, runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "human_task_expiry_event_identity_changed", "workflow-runtime", "expire_human_task_cards",
					map[string]any{"stage": "commit", "index": index},
					fmt.Errorf("human-task expiry authority changed before commit at index %d", index))
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
	for index, publication := range result.Publications {
		result.Publications[index] = publication.(runtimebus.CommittedEnginePublication).WithCommitAcknowledgment()
	}
	if len(result.Publications) == 0 {
		return result, errors.Join(outcome.Err(), result.Validate())
	}
	return result, errors.Join(outcome.Err(), result.Validate(), store.publishCardChanges(acknowledged, true))
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
