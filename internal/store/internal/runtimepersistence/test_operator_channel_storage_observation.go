package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
)

func CountOperatorChannelActionIntentStorageForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = operatorchannel.CountActionIntentStorageTx(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadOperatorChannelTextIntentFactStorageForTest(ctx context.Context, selected any, providerEventID string) ([]byte, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	_, postgres := selected.(*PostgresStore)
	var fact []byte
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		fact, err = operatorchannel.ReadTextIntentFactStorageTx(ctx, tx, providerEventID, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return fact, nil
}
