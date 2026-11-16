package runtimepersistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type PostgresRunTableReadBarrier = runlifecycle.PostgresRunTableReadBarrier

func HoldPostgresRunTableReadBarrierForTest(ctx context.Context, selected any) (*PostgresRunTableReadBarrier, error) {
	if ctx == nil {
		return nil, errors.New("run read barrier requires a context")
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	owner, ok := selected.(*PostgresStore)
	if !ok {
		return nil, fmt.Errorf("run read lock barrier is PostgreSQL-scoped")
	}
	return owner.runLifecyclePostgresOwner.HoldPostgresRunTableReadBarrierForTest(ctx)
}

func ReadPostgresRunOriginLockCountForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	owner, ok := selected.(*PostgresStore)
	if !ok {
		return 0, fmt.Errorf("origin lock observation is PostgreSQL-scoped")
	}
	return owner.runLifecyclePostgresOwner.ReadPostgresRunOriginLockCountForTest(ctx)
}

func ReadPostgresDatabaseLockCountForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	owner, ok := selected.(*PostgresStore)
	if !ok {
		return 0, fmt.Errorf("database lock observation is PostgreSQL-scoped")
	}
	return owner.runLifecyclePostgresOwner.ReadPostgresDatabaseLockCountForTest(ctx)
}
