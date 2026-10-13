package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/apiidempotency"
)

func ReadAPIIdempotencyCardinalityForTest(ctx context.Context, selected any, method, key string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = apiidempotency.ReadMethodKeyCardinalityTx(ctx, tx, method, key)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
