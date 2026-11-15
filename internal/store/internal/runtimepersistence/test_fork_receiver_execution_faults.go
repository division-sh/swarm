package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
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
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.InstallActiveForkReceiverHeaderDoneFaultForTest(ctx, runID, entityID)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.InstallActiveForkReceiverHeaderDoneFaultForTest(ctx, runID, entityID)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func ExpireExactDeliveryClaimFaultForTest(ctx context.Context, selected any, claim deliverylifecycle.Claim) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.deliveryPostgresOwner.ExpireExactDeliveryClaimFaultForTest(ctx, claim)
	case *SQLiteRuntimeStore:
		return owner.deliverySQLiteOwner.ExpireExactDeliveryClaimFaultForTest(ctx, claim)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func TieExactPendingPublicationCreatedAtFaultForTest(ctx context.Context, selected any, eventID, firstID, secondID string) error {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.deliveryPostgresOwner.TieExactPendingPublicationCreatedAtFaultForTest(ctx, eventID, firstID, secondID)
	case *SQLiteRuntimeStore:
		return owner.deliverySQLiteOwner.TieExactPendingPublicationCreatedAtFaultForTest(ctx, eventID, firstID, secondID)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
