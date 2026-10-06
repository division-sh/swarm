package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type ReceiverJoinedInventoryStorageRow = pipelinepersistence.ReceiverJoinedInventoryStorageRow

func ReadReceiverJoinedInventoryStorageForTest(ctx context.Context, selected any, runID string) ([]ReceiverJoinedInventoryStorageRow, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	var out []ReceiverJoinedInventoryStorageRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadReceiverJoinedInventoryStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
