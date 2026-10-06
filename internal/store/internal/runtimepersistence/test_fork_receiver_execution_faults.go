package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func InstallActiveForkReceiverHeaderDoneFaultForTest(ctx context.Context, selected any, runID, entityID string) error {
	for _, id := range []string{runID, entityID} {
		if err := validateSelectedForkStorageIdentity(id); err != nil {
			return err
		}
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.InstallActiveForkReceiverHeaderDoneFaultTx(ctx, tx, runID, entityID)
	})
}

func ExpireExactDeliveryClaimFaultForTest(ctx context.Context, selected any, claim deliverylifecycle.Claim) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return delivery.ExpireExactDeliveryClaimFaultTx(ctx, tx, claim)
	})
}
