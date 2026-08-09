package runtimepersistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
)

func (s *PostgresStore) ActiveRunBundleAvailabilities(ctx context.Context) ([]runbundle.Availability, error) {
	if s == nil || s.runBundles == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.runBundles.ListActive(ctx)
}

func (s *PostgresStore) ActiveRunBundleAvailabilityConflicts(ctx context.Context) ([]runbundle.Availability, error) {
	availabilities, err := s.ActiveRunBundleAvailabilities(ctx)
	if err != nil {
		return nil, err
	}
	conflicts := make([]runbundle.Availability, 0, len(availabilities))
	for _, availability := range availabilities {
		if !availability.Available() {
			conflicts = append(conflicts, availability)
		}
	}
	return conflicts, nil
}

func (s *PostgresStore) LoadRunBundleAvailability(ctx context.Context, runID string) (runbundle.Availability, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runbundle.Availability{}, err
	}
	availability, err := s.runBundles.Load(ctx, runID)
	if errors.Is(err, runbundle.ErrRunNotFound) {
		return runbundle.Availability{}, runbundle.ErrRunNotFound
	}
	return availability, err
}
