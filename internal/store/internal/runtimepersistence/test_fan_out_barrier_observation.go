package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func ReadRunFanOutBarrierStatusStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return "", err
	}
	var status string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		status, err = pipelinepersistence.ReadRunFanOutBarrierStatusStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return "", err
	}
	return status, nil
}
