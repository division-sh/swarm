package store

import (
	"context"
	"errors"
	"fmt"

	"swarm/internal/store/runbundle"
)

type ActiveRunBundleAvailabilityConflict = runbundle.Availability

func (s *PostgresStore) ActiveRunBundleAvailabilities(ctx context.Context) ([]runbundle.Availability, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if !caps.Events.HasRuns || !caps.Events.RunBundleHash || !caps.Events.RunBundleSource {
		return nil, fmt.Errorf("active run bundle availability requires runs.bundle_hash and runs.bundle_source")
	}
	return runbundle.ListActiveAvailabilities(ctx, s.DB)
}

func (s *PostgresStore) ActiveRunBundleAvailabilityConflicts(ctx context.Context) ([]ActiveRunBundleAvailabilityConflict, error) {
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
	if s == nil || s.DB == nil {
		return runbundle.Availability{}, fmt.Errorf("postgres store is required")
	}
	availability, err := runbundle.LoadAvailability(ctx, s.DB, runID)
	if errors.Is(err, runbundle.ErrRunNotFound) {
		return runbundle.Availability{}, ErrRunNotFound
	}
	return availability, err
}
