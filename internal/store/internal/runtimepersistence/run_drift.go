package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
)

func (s *PostgresStore) InspectRunMutationDrift(ctx context.Context, runID string) (mutationlog.DriftReport, error) {
	return s.runForkPostgresOwner.InspectRunMutationDrift(ctx, runID)
}

func (s *SQLiteRuntimeStore) InspectRunMutationDrift(ctx context.Context, runID string) (mutationlog.DriftReport, error) {
	return s.runForkSQLiteOwner.InspectRunMutationDrift(ctx, runID)
}
