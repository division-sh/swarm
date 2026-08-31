package adminpersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
)

const (
	destructiveResetLockKey = "swarm:destructive-reset"
)

func (s *DestructiveResetPostgresOwner) AcquireDestructiveReset(ctx context.Context) (destructivereset.LockLease, bool, error) {
	if s == nil || s.backend == nil {
		return nil, false, fmt.Errorf("postgres store is required")
	}
	return acquireAdministrativeLease(ctx, s.backend, destructiveResetLockKey)
}

func acquireAdministrativeLease(ctx context.Context, backend *postgresbackend.Backend, lockKey string) (destructivereset.LockLease, bool, error) {
	releaseCapacity := backend.RetainConnectionCapacity()
	lease, acquired, err := postgresbackend.AcquireAdvisoryLockLease(ctx, backend, lockKey)
	if lease == nil {
		releaseCapacity()
		return nil, acquired, err
	}
	if !lease.InstallTerminalOwner(releaseCapacity, nil, nil) {
		releaseCapacity()
		return nil, false, fmt.Errorf("install administrative lease capacity owner")
	}
	return terminalAdvisoryLockLease{lease: lease}, acquired, err
}

type terminalAdvisoryLockLease struct {
	lease *postgresbackend.AdvisoryLockLease
}

func (l terminalAdvisoryLockLease) Release(ctx context.Context) error {
	if l.lease == nil {
		return nil
	}
	return l.lease.Release(ctx)
}
