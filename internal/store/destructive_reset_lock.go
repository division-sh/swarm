package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
)

func (s *PostgresStore) TryAcquire(ctx context.Context, lockKey string) (destructivereset.LockLease, bool, error) {
	if s == nil || s.DB == nil {
		return nil, false, fmt.Errorf("postgres store is required")
	}
	lockKey = strings.TrimSpace(lockKey)
	if lockKey == "" {
		return nil, false, fmt.Errorf("destructive reset lock key is required")
	}
	lease, acquired, err := acquireAdvisoryLockLease(ctx, s.DB, lockKey)
	if lease == nil {
		return nil, acquired, err
	}
	return lease, acquired, err
}
