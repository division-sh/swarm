package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/authoractivity"
)

func commitHumanTaskExpirations(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
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
	result := runtimepipeline.CommittedHumanTaskExpiry{Publications: make([]runtimeengine.CommittedDurablePublication, 0, len(plans))}
	err := run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		expired, err := expireHumanTaskCards(txctx, runtimeAuthorActivityMutation(story), tx, command.ObservedAt, command.Limit, postgres)
		if err != nil {
			return err
		}
		if len(expired) != len(plans) {
			return fmt.Errorf("human-task expiry authority changed before commit: due=%d planned=%d", len(expired), len(plans))
		}
		for index, plan := range plans {
			if strings.TrimSpace(expired[index].ID()) != strings.TrimSpace(plan.DurablePublicationEventID()) {
				return fmt.Errorf("human-task expiry authority changed before commit at index %d", index)
			}
			committed, err := commitPublicationTx(txctx, tx, story, store, postgres, plan.PublicationCommand(), publicationCommitOptions{})
			if err != nil {
				return fmt.Errorf("commit human-task expiry publication %d: %w", index, err)
			}
			evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
			if err != nil {
				return err
			}
			result.Publications = append(result.Publications, evidence)
		}
		return nil
	})
	if err != nil {
		return runtimepipeline.CommittedHumanTaskExpiry{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimepipeline.CommittedHumanTaskExpiry{}, err
	}
	return result, nil
}

func (s *PostgresStore) CommitHumanTaskExpirations(ctx context.Context, command runtimepipeline.HumanTaskExpiryCommand) (runtimepipeline.CommittedHumanTaskExpiry, error) {
	return commitHumanTaskExpirations(ctx, s, true, s.runPrivateAuthorActivityMutation, command)
}

func (s *SQLiteRuntimeStore) CommitHumanTaskExpirations(ctx context.Context, command runtimepipeline.HumanTaskExpiryCommand) (runtimepipeline.CommittedHumanTaskExpiry, error) {
	return commitHumanTaskExpirations(ctx, s, false, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite human-task expiry", fn)
	}, command)
}

var _ runtimepipeline.HumanTaskExpiry = (*PostgresStore)(nil)
var _ runtimepipeline.HumanTaskExpiry = (*SQLiteRuntimeStore)(nil)
