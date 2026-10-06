package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *EffectPostgresOwner) SettleCanceledDirectiveTurn(ctx context.Context, attempt runtimeeffects.Attempt) (agentcontrol.DirectiveOperation, error) {
	if err := s.requireCurrent(); err != nil {
		return agentcontrol.DirectiveOperation{}, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates, func(ctx context.Context, mutation *mutationprotocol.Attempt) (agentcontrol.DirectiveOperation, error) {
		return settleCanceledDirectiveTurn(ctx, mutation, true, s.directives, attempt)
	})
	op, acknowledged := result.Value()
	op.Acknowledged = acknowledged
	return op, result.Err()
}

func (s *EffectSQLiteOwner) SettleCanceledDirectiveTurn(ctx context.Context, attempt runtimeeffects.Attempt) (agentcontrol.DirectiveOperation, error) {
	if err := s.requireCurrent(); err != nil {
		return agentcontrol.DirectiveOperation{}, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite settle canceled directive turn", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates, func(ctx context.Context, mutation *mutationprotocol.Attempt) (agentcontrol.DirectiveOperation, error) {
		return settleCanceledDirectiveTurn(ctx, mutation, false, s.directives, attempt)
	})
	op, acknowledged := result.Value()
	op.Acknowledged = acknowledged
	return op, result.Err()
}

func settleCanceledDirectiveTurn(ctx context.Context, mutation *mutationprotocol.Attempt, postgres bool, directives providerDrainDirectiveOwner, attempt runtimeeffects.Attempt) (agentcontrol.DirectiveOperation, error) {
	if directives == nil || attempt.Authority.Kind != runtimeeffects.AuthorityNormalAgent || attempt.Kind != runtimeeffects.KindProviderTurn || attempt.Origin.Kind != runtimeeffects.CompletionOriginDirective || attempt.Origin.Validate() != nil {
		return agentcontrol.DirectiveOperation{}, fmt.Errorf("canceled directive turn requires its exact admitted provider origin")
	}
	var op agentcontrol.DirectiveOperation
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := directives.ProviderDirectiveOriginPendingTx(ctx, tx, attempt.Origin.Directive, attempt.Authority.Target.RunID, attempt.Authority.Normal.Identity); err != nil {
			return err
		}
		facts, err := prepareCanceledTurnSettlementTx(ctx, tx, postgres, attempt)
		if err != nil {
			return err
		}
		op, err = directives.SettleProviderCanceledDirectiveTx(ctx, mutation, attempt.Origin.Directive, facts.reason, facts.now)
		if err != nil {
			return err
		}
		return completeCanceledTurnTx(ctx, tx, postgres, facts.turnID, op.CompletedAt)
	})
	return op, err
}
