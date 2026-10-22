package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func RemoveForkReceiverFieldCompanionForTest(ctx context.Context, selected any, runID, entityID, instance string) error {
	for _, id := range []string{runID, entityID} {
		if err := validateSelectedForkStorageIdentity(id); err != nil {
			return err
		}
	}
	if instance == "" {
		return fmt.Errorf("receiver field-companion fault requires its exact instance")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.RemoveForkReceiverFieldCompanionTxForTest(ctx, tx, runID, entityID, instance)
	})
}

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
