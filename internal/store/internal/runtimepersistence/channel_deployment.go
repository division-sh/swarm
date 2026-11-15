package runtimepersistence

import (
	"context"
	"github.com/division-sh/swarm/internal/channelonboarding"
)

func (s *PostgresStore) DeploymentChannelOperations() channelonboarding.OperationStore {
	return s.channelOnboardingPostgresOwner.DeploymentOperations()
}

func (s *SQLiteRuntimeStore) DeploymentChannelOperations() channelonboarding.OperationStore {
	return s.channelOnboardingSQLiteOwner.DeploymentOperations()
}

func (s *PostgresStore) ListDeploymentChannelOperations(ctx context.Context) ([]channelonboarding.Operation, error) {
	return s.channelOnboardingPostgresOwner.DeploymentOperations().ListChannelOnboardingOperations(ctx)
}

func (s *SQLiteRuntimeStore) ListDeploymentChannelOperations(ctx context.Context) ([]channelonboarding.Operation, error) {
	return s.channelOnboardingSQLiteOwner.DeploymentOperations().ListChannelOnboardingOperations(ctx)
}
