package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type ManagedDeliveryFailureStorage = delivery.ManagedDeliveryFailureStorage
type ManagedDeliveryStorage = delivery.ManagedDeliveryStorage

func ReadManagedDeliveryStorageForTest(ctx context.Context, selected any, runID, agentID string) (ManagedDeliveryStorage, error) {
	var out ManagedDeliveryStorage
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return out, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadManagedDeliveryStorage(ctx, tx, runID, agentID)
		return err
	})
	if err != nil {
		return ManagedDeliveryStorage{}, err
	}
	return out, nil
}
