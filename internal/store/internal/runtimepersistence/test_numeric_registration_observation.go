package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

type NumericRegistrationStorage = eventrecord.NumericRegistrationStorage

func ReadNumericRegistrationStorageForTest(ctx context.Context, selected any, runID string) ([]NumericRegistrationStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return nil, err
	}
	var out []NumericRegistrationStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadNumericRegistrationStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
