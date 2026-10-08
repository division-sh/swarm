package runtimepersistence

import (
	"context"
	"fmt"
	"testing"

	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

// AttemptCorruptRunSnapshotForTest selects only the exact backend's named
// lifecycle fault. It deliberately does not admit a run or source artifact.
func AttemptCorruptRunSnapshotForTest(ctx context.Context, selected any, snapshot runlifecyclefixture.CorruptSnapshot) error {
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil || !store.backend.Valid() {
			return fmt.Errorf("postgres snapshot fault store is required")
		}
		return storerunlifecycle.AttemptCorruptPostgresSnapshotForTest(ctx, store.backend.ConstructionHandle(), snapshot)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil || !store.backend.Valid() {
			return fmt.Errorf("sqlite snapshot fault store is required")
		}
		return storerunlifecycle.AttemptCorruptSQLiteSnapshotForTest(ctx, store.backend.ConstructionHandle(), snapshot)
	default:
		return fmt.Errorf("unsupported run snapshot fault store %T", selected)
	}
}

func RequireCorruptRunSnapshotForTest(t testing.TB, ctx context.Context, selected any, snapshot runlifecyclefixture.CorruptSnapshot) {
	t.Helper()
	if err := AttemptCorruptRunSnapshotForTest(ctx, selected, snapshot); err != nil {
		t.Fatalf("materialize corrupt run snapshot %s: %v", snapshot.RunID, err)
	}
}
