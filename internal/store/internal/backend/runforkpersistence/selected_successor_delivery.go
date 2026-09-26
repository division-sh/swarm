package runforkpersistence

import (
	"context"
	"fmt"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *RunForkPostgresOwner) ReconcileSelectedSuccessorDeliveryAuthority(ctx context.Context, predecessorID string, successor runtimedelivery.ExecutionAuthority) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("selected successor delivery requires postgres store")
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil,
		func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, postgresDeliveryAdapter.TransferSelectedSuccessorAuthority(ctx, attempt, predecessorID, successor)
		})
	return result.Err()
}

func (s *RunForkSQLiteOwner) ReconcileSelectedSuccessorDeliveryAuthority(ctx context.Context, predecessorID string, successor runtimedelivery.ExecutionAuthority) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("selected successor delivery requires sqlite store")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "reconcile selected successor delivery", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil,
		func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, sqliteDeliveryAdapter.TransferSelectedSuccessorAuthority(ctx, attempt, predecessorID, successor)
		})
	return result.Err()
}
