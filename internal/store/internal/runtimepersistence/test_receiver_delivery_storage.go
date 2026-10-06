package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type ReceiverDeliveryStorageEvidence = delivery.ReceiverDeliveryStorageRow

// B633f25f19 /6013471853: retain the original selected read transaction while
// delegating the fixed delivery query to its existing closed SQL owner.
func ReadReceiverDeliveryStorageForTest(ctx context.Context, selected any, runID string) ([]ReceiverDeliveryStorageEvidence, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	var out []ReceiverDeliveryStorageEvidence
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadReceiverDeliveryStorage(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
