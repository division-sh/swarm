package startupownership

import (
	"context"
	"errors"
	"fmt"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
)

// RetainConstructionPossession precedes schema bootstrap. The capability adopts
// this exact session rather than releasing and reacquiring the process lease.
func (s *StartupPostgresOwner) RetainConstructionPossession(ctx context.Context) error {
	if s == nil || ctx == nil {
		return errors.New("PostgreSQL process construction requires an owner and context")
	}
	s.constructionMu.Lock()
	defer s.constructionMu.Unlock()
	if s.constructionLease != nil {
		return errors.New("PostgreSQL construction possession already retained")
	}
	lease, capacity, err := s.acquireConstructionPossession(ctx)
	if err != nil {
		return err
	}
	s.constructionLease, s.constructionCapacity = lease, capacity
	return nil
}

func (s *StartupPostgresOwner) acquireConstructionPossession(ctx context.Context) (*postgresbackend.AdvisoryLockLease, func(), error) {
	releaseCapacity := s.backend.RetainConnectionCapacity()
	lease, err := postgresbackend.AcquireBlockingAdvisoryLockLease(ctx, s.backend, runtimeSharedStoreOwnershipLock)
	if err != nil {
		releaseCapacity()
		return nil, nil, fmt.Errorf("wait for retained runtime store session: %w", err)
	}
	return lease, releaseCapacity, nil
}

func (s *StartupPostgresOwner) takeConstructionPossession(ctx context.Context) (*postgresbackend.AdvisoryLockLease, func(), error) {
	s.constructionMu.Lock()
	defer s.constructionMu.Unlock()
	if s.constructionLease == nil {
		return s.acquireConstructionPossession(ctx)
	}
	lease, capacity := s.constructionLease, s.constructionCapacity
	s.constructionLease, s.constructionCapacity = nil, nil
	return lease, capacity, nil
}

func (s *StartupPostgresOwner) ReleaseConstructionPossession() error {
	if s == nil {
		return nil
	}
	s.constructionMu.Lock()
	defer s.constructionMu.Unlock()
	if s.constructionLease == nil {
		return nil
	}
	err := s.constructionLease.Release(context.Background())
	s.constructionCapacity()
	s.constructionLease, s.constructionCapacity = nil, nil
	return err
}
