package runtimepersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func CorruptRunOriginForTest(ctx context.Context, selected any, runID string, origin runlifecycle.RunOrigin) error {
	switch store := selected.(type) {
	case *PostgresStore:
		if store != nil {
			return store.runLifecyclePostgresOwner.CorruptOriginForTest(ctx, runID, origin)
		}
	case *SQLiteRuntimeStore:
		if store != nil {
			return store.runLifecycleSQLiteOwner.CorruptOriginForTest(ctx, runID, origin)
		}
	}
	return fmt.Errorf("run origin fault requires its original native lifecycle owner")
}

func CorruptSQLiteRunStateForTest(ctx context.Context, selected any, runID, state string, endedAt time.Time) error {
	store, ok := selected.(*SQLiteRuntimeStore)
	if !ok || store == nil {
		return fmt.Errorf("run state fault requires its original native sqlite lifecycle owner")
	}
	return store.runLifecycleSQLiteOwner.CorruptStateForTest(ctx, runID, state, endedAt)
}
