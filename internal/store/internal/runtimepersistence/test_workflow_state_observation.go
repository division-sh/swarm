package runtimepersistence

import (
	"context"
	"database/sql"

	entitystorage "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func ReadWorkflowStateEntityStorageForTest(ctx context.Context, selected any, runID, instance, state string) (string, bool, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return "", false, err
	}
	var entityID string
	var found bool
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		entityID, err = pipelinepersistence.ReadWorkflowStateEntityStorageTx(ctx, tx, runID, instance, state)
		if err == sql.ErrNoRows {
			return nil
		}
		found = err == nil
		return err
	})
	if err != nil {
		return "", false, err
	}
	return entityID, found, nil
}

func CountRunStateEntityStorageForTest(ctx context.Context, selected any, runID, state string) (int, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = entitystorage.CountRunStateEntityStorageTx(ctx, tx, runID, state)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
