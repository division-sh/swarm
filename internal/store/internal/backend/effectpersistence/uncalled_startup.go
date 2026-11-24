package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func prepareNormalStartupRecoveryTx(ctx context.Context, tx *sql.Tx, postgres bool, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner) (context.Context, []storedelivery.UncalledStartupClaim, error) {
	if delivery == nil {
		ctx, err := prepareProviderRecoveryOwnershipTx(ctx, tx, postgres, directives, "", nil)
		return ctx, nil, err
	}
	authority, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil {
		return nil, nil, err
	}
	if authority.HasRuntimeGrant() && authority.GrantEvidence().SelectedFork != nil {
		ctx, err := prepareProviderRecoveryOwnershipTx(ctx, tx, postgres, directives, "", nil)
		return ctx, nil, err
	}
	processID := authority.ProcessAuthority().LeaseStamp().LeaseID
	claims, err := delivery.UncalledStartupClaimsTx(ctx, tx, processID, false)
	if err != nil {
		return nil, nil, err
	}
	sources := make([]retainedTurnActorSource, len(claims))
	for i, candidate := range claims {
		sources[i] = retainedTurnActorSource{candidate.Actor, candidate.BundleHash}
	}
	ctx, err = prepareProviderRecoveryOwnershipTx(ctx, tx, postgres, directives, "", nil, sources...)
	if err != nil || len(claims) == 0 {
		return ctx, nil, err
	}
	locked, err := delivery.UncalledStartupClaimsTx(ctx, tx, processID, true)
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
