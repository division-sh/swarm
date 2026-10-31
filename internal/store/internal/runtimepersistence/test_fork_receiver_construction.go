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
