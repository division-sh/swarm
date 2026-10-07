package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func validateProviderWorkOrigin(ctx context.Context, tx *sql.Tx, authority effects.Authority, req effects.AuthorizeRequest, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner) error {
	if !authority.HasBusinessTurnOrigin() || req.Kind != effects.KindProviderTurn {
		return nil
	}
	switch req.Origin.Kind {
	case effects.CompletionOriginDelivery:
		if delivery == nil {
			return fmt.Errorf("provider delivery owner is not bound")
		}
		if authority.Kind == effects.AuthoritySelectedContractFork {
			hash, err := requiredExternalEffectBundleHash(ctx, authority)
			if err != nil {
				return err
			}
			source, err := correlation.DecodeSourceArtifactFact(hash)
			if err != nil {
				return err
			}
			exact, err := deliverylifecycle.NewSelectedExecutionAuthority(source, authority.SelectedFork.ExecutionID, authority.SelectedFork.ForkRunID, authority.SelectedFork.Generation)
			if err != nil {
				return err
			}
			return delivery.ValidateSelectedProviderOriginTx(ctx, tx, req.Origin.Delivery, authority.Target.AgentIdentity, exact)
		}
		return delivery.ValidateProviderOriginTx(ctx, tx, req.Origin.Delivery)
	case effects.CompletionOriginDirective:
		if directives == nil {
			return fmt.Errorf("provider directive owner is not bound")
		}
		return directives.ValidateProviderDirectiveOriginTx(ctx, tx, req.Origin.Directive, authority.Target.RunID, authority.Target.AgentIdentity)
	default:
		return fmt.Errorf("business provider origin kind %q is invalid", req.Origin.Kind)
	}
}
