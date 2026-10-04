package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func (s *PostgresStore) InspectSelectedForkRecovery(ctx context.Context, entry runfork.SelectedForkRecoveryEntry) (runfork.SelectedForkRecoveryInspection, error) {
	return s.runForkPostgresOwner.InspectSelectedForkRecovery(ctx, entry)
}

func (s *SQLiteRuntimeStore) InspectSelectedForkRecovery(ctx context.Context, entry runfork.SelectedForkRecoveryEntry) (runfork.SelectedForkRecoveryInspection, error) {
	return s.runForkSQLiteOwner.InspectSelectedForkRecovery(ctx, entry)
}
