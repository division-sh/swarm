package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func LoadManagedTurnRootedSourceForTest(ctx context.Context, selected any, runID string) (sourceartifact.RootedSource, error) {
	switch store := selected.(type) {
	case *PostgresStore:
		if store != nil {
			return store.runLifecyclePostgresOwner.LoadRunRootedSource(ctx, runID)
		}
	case *SQLiteRuntimeStore:
		if store != nil {
			return store.runLifecycleSQLiteOwner.LoadRunRootedSource(ctx, runID)
		}
	}
	return sourceartifact.RootedSource{}, fmt.Errorf("managed turn fixture requires its original native source owner")
}
