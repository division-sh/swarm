package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type SuspendedForkReceiverConstruction = pipelinepersistence.SuspendedForkReceiverConstruction

func SuspendForkReceiverConstructionForTest(ctx context.Context, selected any, runID, entityID string) (SuspendedForkReceiverConstruction, error) {
	var saved SuspendedForkReceiverConstruction
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return saved, err
	}
	err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		saved, err = pipelinepersistence.SuspendForkReceiverConstructionTxForTest(ctx, tx, runID, entityID)
		return err
	})
	if err != nil {
		return SuspendedForkReceiverConstruction{}, err
	}
	return saved, nil
}

func RestoreForkReceiverConstructionForTest(ctx context.Context, selected any, saved SuspendedForkReceiverConstruction) error {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.RestoreForkReceiverConstructionTxForTest(ctx, tx, saved)
	})
}
