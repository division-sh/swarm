package runtimepersistence

import "github.com/division-sh/swarm/internal/channelonboarding"

func (s *PostgresStore) DeploymentChannelOperations() channelonboarding.OperationStore {
	return s.channelOnboardingPostgresOwner.DeploymentOperations()
}

func (s *SQLiteRuntimeStore) DeploymentChannelOperations() channelonboarding.OperationStore {
	return s.channelOnboardingSQLiteOwner.DeploymentOperations()
}
