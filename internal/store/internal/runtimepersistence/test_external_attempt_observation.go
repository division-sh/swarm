package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
)

type ExternalAttemptStorage = effectpersistence.ExternalAttemptStorage

func ReadExternalAttemptStorageForTest(ctx context.Context, selected any) ([]ExternalAttemptStorage, error) {
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.effectPostgresOwner == nil {
			return nil, fmt.Errorf("attempt observation requires the original postgres effect owner")
		}
		return owner.effectPostgresOwner.ReadExternalAttemptStorageForTest(ctx)
	case *SQLiteRuntimeStore:
		if owner == nil || owner.effectSQLiteOwner == nil {
			return nil, fmt.Errorf("attempt observation requires the original sqlite effect owner")
		}
		return owner.effectSQLiteOwner.ReadExternalAttemptStorageForTest(ctx)
	default:
		return nil, fmt.Errorf("attempt observation requires the original native selected owner, got %T", selected)
	}
}
