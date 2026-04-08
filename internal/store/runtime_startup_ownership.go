package store

import (
	"context"
	"fmt"
	"strings"

	runtimestartupownership "swarm/internal/runtime/startupownership"
)

const runtimeSharedStoreOwnershipLock = "swarm:runtime:shared-store-owner"

func (s *PostgresStore) AcquireRuntimeStartupOwnership(ctx context.Context, ownerID string) (runtimestartupownership.Lease, error) {
	if s == nil || s.DB == nil {
		return nil, nil
	}
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		return nil, fmt.Errorf("runtime owner id is required")
	}
	lease, acquired, err := acquireAdvisoryLockLease(ctx, s.DB, runtimeSharedStoreOwnershipLock)
	if err != nil {
		return nil, fmt.Errorf("acquire shared runtime store ownership for %s: %w", ownerID, err)
	}
	if !acquired {
		return nil, fmt.Errorf("shared runtime store already owned by another runtime instance")
	}
	return lease, nil
}
