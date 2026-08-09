package adminpersistence

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
)

func (s *AdminPostgresOwner) TryAcquire(ctx context.Context, lockKey string) (destructivereset.LockLease, bool, error) {
	if s == nil || s.backend == nil {
		return nil, false, fmt.Errorf("postgres store is required")
	}
	lockKey = strings.TrimSpace(lockKey)
	if lockKey == "" {
		return nil, false, fmt.Errorf("destructive reset lock key is required")
	}
	lease, acquired, err := postgresbackend.AcquireAdvisoryLockLease(ctx, s.backend, lockKey)
	if lease == nil {
		return nil, acquired, err
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
