package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
)

func (s *PostgresStore) CommitDeploymentRunCreation(ctx context.Context, command durabledata.RunCreationCommand, request apiidempotency.Request) (durabledata.RunCreationOperationRecord, error) {
	return s.eventPostgresOwner.CommitDeploymentRunCreation(ctx, command, request)
}

func (s *SQLiteRuntimeStore) CommitDeploymentRunCreation(ctx context.Context, command durabledata.RunCreationCommand, request apiidempotency.Request) (durabledata.RunCreationOperationRecord, error) {
	return s.eventSQLiteOwner.CommitDeploymentRunCreation(ctx, command, request)
}
