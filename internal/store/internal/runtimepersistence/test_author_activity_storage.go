package runtimepersistence

import (
	"context"
)

func CountRecordedDeadLetterStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.activityPostgresOwner.CountRecordedDeadLetterStorageForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return owner.activitySQLiteOwner.CountRecordedDeadLetterStorageForTest(ctx, runID)
	}
	return 0, nil
}
