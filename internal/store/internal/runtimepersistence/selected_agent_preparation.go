package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func (s *PostgresStore) ReadSelectedAgentPreparation(ctx context.Context, expected deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error) {
	return s.runForkPostgresOwner.ReadSelectedAgentPreparation(ctx, expected)
}

func (s *SQLiteRuntimeStore) ReadSelectedAgentPreparation(ctx context.Context, expected deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error) {
	return s.runForkSQLiteOwner.ReadSelectedAgentPreparation(ctx, expected)
}
