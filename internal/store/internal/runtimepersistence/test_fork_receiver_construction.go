package runtimepersistence

import (
	"context"
	"fmt"
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
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.RemoveForkReceiverFieldCompanionForTest(ctx, runID, entityID, instance)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.RemoveForkReceiverFieldCompanionForTest(ctx, runID, entityID, instance)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
