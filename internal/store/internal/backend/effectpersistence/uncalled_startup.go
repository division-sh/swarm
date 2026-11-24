package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func prepareNormalStartupRecoveryTx(ctx context.Context, tx *sql.Tx, mutation *mutationprotocol.Attempt, postgres bool, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner) (context.Context, []storedelivery.UncalledStartupClaim, error) {
	var claims []storedelivery.UncalledStartupClaim
	if delivery != nil {
		var err error
		claims, err = delivery.UncalledStartupClaims(ctx, mutation, false)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(claims) == 0 {
		ctx, err := prepareProviderRecoveryOwnershipTx(ctx, tx, postgres, directives, "", nil)
		return ctx, nil, err
	}
	ids, err := peekProviderRecoveryIDsTx(ctx, tx, "", nil)
	if err != nil {
		return nil, nil, err
	}
	peeks, actors, err := peekRetainedTurnSetTx(ctx, tx, ids, nil)
	if err != nil {
		return nil, nil, err
	}
	sources := make([]retainedTurnActorSource, len(claims))
	for i, candidate := range claims {
		sources[i] = retainedTurnActorSource{candidate.Actor, candidate.BundleHash}
	}
	ctx, err = bindRetainedTurnOwnershipTx(ctx, tx, postgres, directives, peeks, actors, sources)
	if err != nil {
		return nil, nil, err
	}
	current, err := peekProviderRecoveryIDsTx(ctx, tx, "", nil)
	if err != nil {
		return nil, nil, err
	}
	if !slices.Equal(ids, current) {
		return nil, nil, fmt.Errorf("provider recovery population changed under its ownership prefix")
	}
	locked, err := delivery.UncalledStartupClaims(ctx, mutation, true)
	if err != nil {
		return nil, nil, err
	}
	if !slices.Equal(claims, locked) {
		return nil, nil, fmt.Errorf("uncalled startup claims changed under the turn ownership prefix")
	}
	return ctx, locked, nil
}

func settleUncalledStartupClaims(ctx context.Context, mutation *mutationprotocol.Attempt, delivery providerDrainDeliveryOwner, claims []storedelivery.UncalledStartupClaim, now time.Time) (int, error) {
	if len(claims) == 0 {
		return 0, nil
	}
	const reason = "effect_recovery_prelaunch_abandoned"
	failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict, reason, "external-effects", "startup_reconcile",
		map[string]any{"recovered_at": now.UTC().Format(time.RFC3339Nano)}))
	if !ok {
		return 0, fmt.Errorf("construct uncalled startup recovery failure")
	}
	settlement := deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, Failure: &failure, ReasonCode: reason,
		RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}
	for _, candidate := range claims {
		if err := delivery.SettleProviderOriginRecoveryFailureTx(ctx, mutation, candidate.Claim, settlement, ""); err != nil {
			return 0, err
		}
	}
	return len(claims), nil
}
