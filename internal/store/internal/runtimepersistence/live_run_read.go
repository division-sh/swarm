package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *PostgresStore) LoadLiveRun(ctx context.Context, scope runlifecycle.LiveRunScope) (runlifecycle.LiveRun, bool, error) {
	return s.runLifecyclePostgresOwner.LoadLiveRun(ctx, scope)
}

func (s *SQLiteRuntimeStore) LoadLiveRun(ctx context.Context, scope runlifecycle.LiveRunScope) (runlifecycle.LiveRun, bool, error) {
	return s.runLifecycleSQLiteOwner.LoadLiveRun(ctx, scope)
}

func (s *PostgresStore) LoadRunLiveScope(ctx context.Context, runID string) (runlifecycle.LiveRunScope, error) {
	return s.runLifecyclePostgresOwner.LoadRunLiveScope(ctx, runID)
}

func (s *SQLiteRuntimeStore) LoadRunLiveScope(ctx context.Context, runID string) (runlifecycle.LiveRunScope, error) {
	return s.runLifecycleSQLiteOwner.LoadRunLiveScope(ctx, runID)
}
