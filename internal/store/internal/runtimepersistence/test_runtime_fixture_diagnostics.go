package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

type FanOutDrainDeliveryStorage = delivery.FanOutDrainDeliveryStorage
type FanOutDrainNullableText = delivery.WorkloadNullableText

func ReadFanOutDrainDeliveryStorageForTest(ctx context.Context, selected any, runID string) ([]FanOutDrainDeliveryStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return nil, err
	}
	var out []FanOutDrainDeliveryStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadFanOutDrainDeliveryStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadLifecycleProjectionLogPayloadsForTest(ctx context.Context, selected any) ([][]byte, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out [][]byte
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadLifecycleProjectionLogPayloadsTx(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
