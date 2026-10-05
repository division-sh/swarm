package startupownership

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func rebindFlowReadinessSourceSetTx(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, req manager.FlowReadinessSourceSetRebindRequest, binding manager.ProcessExecutionBinding,
	commit func(context.Context, *mutationprotocol.Attempt, manager.FlowReadinessSourceSetRebindRequest, manager.ProcessExecutionBinding) ([]manager.AgentLifecycleTransitionResult, error),
) (manager.FlowReadinessSourceSetRebindResult, error) {
	var result manager.FlowReadinessSourceSetRebindResult
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result.Attempt, err = pipelinepersistence.RebindFlowActivationAttemptTx(ctx, tx, postgres, req, binding)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return manager.FlowReadinessSourceSetRebindResult{}, err
	}
	result.Transitions, err = commit(ctx, attempt, req, binding)
	if err != nil {
		return manager.FlowReadinessSourceSetRebindResult{}, err
	}
	return result, nil
}

func (s *postgresSession) RebindFlowReadinessSourceSet(ctx context.Context, req manager.FlowReadinessSourceSetRebindRequest, binding manager.ProcessExecutionBinding) (manager.FlowReadinessSourceSetRebindResult, error) {
	commit := mutationprotocol.RunRetainedPostgres(ctx, s.lease.Session(), mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (manager.FlowReadinessSourceSetRebindResult, error) {
		return rebindFlowReadinessSourceSetTx(ctx, attempt, true, req, binding, s.owner.agents.CommitFlowReadinessSourceSetTransitionsTx)
	})
	result, acknowledged := commit.Value()
	if !acknowledged {
		return manager.FlowReadinessSourceSetRebindResult{}, commit.Err()
	}
	return result, commit.Err()
}

func (s *sqliteSession) RebindFlowReadinessSourceSet(ctx context.Context, req manager.FlowReadinessSourceSetRebindRequest, binding manager.ProcessExecutionBinding) (manager.FlowReadinessSourceSetRebindResult, error) {
	commit := mutationprotocol.RunSQLite(ctx, s.owner.backend, "sqlite readiness source-set rebind", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (manager.FlowReadinessSourceSetRebindResult, error) {
		return rebindFlowReadinessSourceSetTx(ctx, attempt, false, req, binding, s.owner.agents.CommitFlowReadinessSourceSetTransitionsTx)
	})
	result, acknowledged := commit.Value()
	if !acknowledged {
		return manager.FlowReadinessSourceSetRebindResult{}, commit.Err()
	}
	return result, commit.Err()
}
