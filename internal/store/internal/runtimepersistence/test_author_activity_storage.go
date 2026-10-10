package runtimepersistence

import (
	"context"
	"database/sql"

	authoractivityadapter "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity/readadapter"
)

func CountRecordedDeadLetterStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		count, err = authoractivityadapter.CountRecordedDeadLetterStorage(ctx, tx, postgres, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
