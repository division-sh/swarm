package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func InstallActiveForkReceiverHeaderDoneFault(ctx context.Context, selected any, runID, entityID string) error {
	return private.InstallActiveForkReceiverHeaderDoneFaultForTest(ctx, selected, runID, entityID)
}

func ExpireExactDeliveryClaimFault(ctx context.Context, selected any, claim deliverylifecycle.Claim) error {
	return private.ExpireExactDeliveryClaimFaultForTest(ctx, selected, claim)
}
