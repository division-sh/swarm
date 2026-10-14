package runtimepersistence

import (
	"context"
	"fmt"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
)

type CompletionAttemptStorage = effectpersistence.CompletionAttemptStorage
type CompletionSpendStorage = effectpersistence.CompletionSpendStorage
type CompletionTargetStorage = effectpersistence.CompletionTargetStorage

func ReadCompletionAttemptStorageForTest(ctx context.Context, selected runtimeeffects.Store, adapter, attemptID string) ([]CompletionAttemptStorage, error) {
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.effectPostgresOwner == nil {
			return nil, fmt.Errorf("completion observation requires the original postgres effect owner")
		}
		return owner.effectPostgresOwner.ReadCompletionAttemptStorageForTest(ctx, adapter, attemptID)
	case *SQLiteRuntimeStore:
		if owner == nil || owner.effectSQLiteOwner == nil {
			return nil, fmt.Errorf("completion observation requires the original sqlite effect owner")
		}
		return owner.effectSQLiteOwner.ReadCompletionAttemptStorageForTest(ctx, adapter, attemptID)
	default:
		return nil, fmt.Errorf("completion observation requires the original selected effect owner, got %T", selected)
	}
}
