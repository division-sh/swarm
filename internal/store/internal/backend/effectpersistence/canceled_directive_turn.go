package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func settleCanceledDirectiveTurn(ctx context.Context, mutation *mutationprotocol.Attempt, postgres bool, directives providerDrainDirectiveOwner, attempt runtimeeffects.Attempt) (agentcontrol.DirectiveOperation, error) {
	if directives == nil || !attempt.Authority.HasBusinessTurnOrigin() || attempt.Kind != runtimeeffects.KindProviderTurn || attempt.Origin.Kind != runtimeeffects.CompletionOriginDirective || attempt.Origin.Validate() != nil {
		return agentcontrol.DirectiveOperation{}, fmt.Errorf("canceled directive turn requires its exact admitted provider origin")
	}
	var op agentcontrol.DirectiveOperation
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := directives.ProviderDirectiveOriginPendingTx(ctx, tx, attempt.Origin.Directive, attempt.Authority.Target.RunID, attempt.Authority.Target.AgentIdentity); err != nil {
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
