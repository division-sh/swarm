package runtimepersistence

import (
	"context"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func (s *PostgresStore) ReconcileSelectedSuccessorDeliveryAuthority(ctx context.Context, predecessorID string, successor runtimedelivery.ExecutionAuthority) error {
	return s.runForkPostgresOwner.ReconcileSelectedSuccessorDeliveryAuthority(ctx, predecessorID, successor)
}

func (s *SQLiteRuntimeStore) ReconcileSelectedSuccessorDeliveryAuthority(ctx context.Context, predecessorID string, successor runtimedelivery.ExecutionAuthority) error {
	return s.runForkSQLiteOwner.ReconcileSelectedSuccessorDeliveryAuthority(ctx, predecessorID, successor)
}
