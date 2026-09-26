package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func (s *PostgresStore) LoadForkOperation(ctx context.Context, actor, key, transportHash string) (runfork.ForkOperationRecord, bool, error) {
	return s.runForkPostgresOwner.LoadForkOperation(ctx, actor, key, transportHash)
}

func (s *SQLiteRuntimeStore) LoadForkOperation(ctx context.Context, actor, key, transportHash string) (runfork.ForkOperationRecord, bool, error) {
	return s.runForkSQLiteOwner.LoadForkOperation(ctx, actor, key, transportHash)
}

func (s *PostgresStore) LoadForkOperationByID(ctx context.Context, operationID string) (runfork.ForkOperationRecord, bool, error) {
	return s.runForkPostgresOwner.LoadForkOperationByID(ctx, operationID)
}

func (s *SQLiteRuntimeStore) LoadForkOperationByID(ctx context.Context, operationID string) (runfork.ForkOperationRecord, bool, error) {
	return s.runForkSQLiteOwner.LoadForkOperationByID(ctx, operationID)
}

func (s *PostgresStore) FailMaterializedSelectedContractExecutionFork(ctx context.Context, forkRunID string, failure runfork.ForkOperationFailure) error {
	return s.runForkPostgresOwner.FailMaterializedSelectedContractExecutionFork(ctx, forkRunID, failure)
}

func (s *SQLiteRuntimeStore) FailMaterializedSelectedContractExecutionFork(ctx context.Context, forkRunID string, failure runfork.ForkOperationFailure) error {
	return s.runForkSQLiteOwner.FailMaterializedSelectedContractExecutionFork(ctx, forkRunID, failure)
}
