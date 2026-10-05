package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/bus"
)

func (s *PostgresStore) CommitDeploymentRunCreation(ctx context.Context, command bus.DeploymentRunCreationCommand) (bus.CommittedDeploymentRunCreation, error) {
	return s.eventPostgresOwner.CommitDeploymentRunCreation(ctx, command)
}

func (s *SQLiteRuntimeStore) CommitDeploymentRunCreation(ctx context.Context, command bus.DeploymentRunCreationCommand) (bus.CommittedDeploymentRunCreation, error) {
	return s.eventSQLiteOwner.CommitDeploymentRunCreation(ctx, command)
}
