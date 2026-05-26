package store

import (
	"context"
	"fmt"

	"swarm/internal/store/runbundle"
)

type ActiveRunBundleAvailabilityConflict = runbundle.Availability

func (s *PostgresStore) ActiveRunBundleAvailabilityConflicts(ctx context.Context) ([]ActiveRunBundleAvailabilityConflict, error) {
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
	return runbundle.ListActiveConflicts(ctx, s.DB)
}
