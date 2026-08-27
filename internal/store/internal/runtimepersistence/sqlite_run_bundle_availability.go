package runtimepersistence

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
)

func (s *SQLiteRuntimeStore) LoadRunBundleAvailability(ctx context.Context, runID string) (runbundle.Availability, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runbundle.Availability{}, err
	}
	availability, err := s.runBundles.Load(ctx, runID)
	if errors.Is(err, runbundle.ErrRunNotFound) {
		return runbundle.Availability{}, runbundle.ErrRunNotFound
	}
	return availability, err
}

func (s *SQLiteRuntimeStore) ActiveNonStandingRunBundleAvailabilities(ctx context.Context) ([]runbundle.Availability, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.runBundles.ListActiveNonStanding(ctx)
}

func (s *SQLiteRuntimeStore) ActiveNonStandingRunBundleAvailabilityConflicts(ctx context.Context) ([]runbundle.Availability, error) {
	availabilities, err := s.ActiveNonStandingRunBundleAvailabilities(ctx)
	if err != nil {
		return nil, err
	}
	var conflicts []runbundle.Availability
	for _, availability := range availabilities {
		if !availability.Available() {
			conflicts = append(conflicts, availability)
		}
	}
	return conflicts, nil
}
