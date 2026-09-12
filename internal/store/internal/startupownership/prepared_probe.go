package startupownership

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

// PreparedProbeProcessCurrent consumes the canonical process head, not a live
// generation grant. Mutation callers lock that head in their owning transaction.
func PreparedProbeProcessCurrent(ctx context.Context, q authorityQueryer, preparation managedcapabilities.PreparedSelectedForkProbeAuthority, generation uint64, sqlite, lock bool) (bool, error) {
	if err := preparation.Validate(); err != nil {
		return false, err
	}
	return PreparedProcessCurrent(ctx, q, preparation.SelectedForkPreparationCoordinates, generation, sqlite, lock)
}

// PreparedProcessCurrent also admits an explicitly empty prospective census.
func PreparedProcessCurrent(ctx context.Context, q authorityQueryer, preparation managedcapabilities.SelectedForkPreparationCoordinates, generation uint64, sqlite, lock bool) (bool, error) {
	if err := preparation.Validate(); err != nil {
		return false, err
	}
	if generation == 0 {
		return false, fmt.Errorf("prepared probe requires process generation")
	}
	record, found, err := loadAuthorityHeadRecord(ctx, q, sqlite, lock)
	if err != nil || !found {
		return false, err
	}
	backend := "postgres_retained_session"
	if sqlite {
		backend = "sqlite_retained_owner"
	}
	authority, err := validateAuthorityLineage(ctx, q, record, backend, sqlite, make(map[string]struct{}))
	if err != nil {
		return false, err
	}
	return authority.State == runtimeownership.StateActive &&
		authority.AuthorityID == preparation.ProcessAuthorityID &&
		authority.OwnerID == preparation.ProcessOwnerID &&
		authority.BootID == preparation.ProcessBootID &&
		authority.AuthorityGeneration == generation, nil
}

// PreparedProcessPredecessor proves recorded ancestry, rather than inferring
// abandonment from a different UUID or an elapsed lease.
func PreparedProcessPredecessor(ctx context.Context, q authorityQueryer, preparation managedcapabilities.SelectedForkPreparationCoordinates, generation uint64, current runtimeownership.Authority, sqlite bool) (bool, error) {
	if err := preparation.Validate(); err != nil {
		return false, err
	}
	backend := "postgres_retained_session"
	if sqlite {
		backend = "sqlite_retained_owner"
	}
	for id := current.PredecessorAuthorityID; id != ""; {
		record, found, err := loadAuthorityRecord(ctx, q, id, nil, sqlite)
		if err != nil {
			return false, err
		}
		if !found {
			return false, fmt.Errorf("selected recovery predecessor is missing")
		}
		authority, err := validateAuthorityLineage(ctx, q, record, backend, sqlite, make(map[string]struct{}))
		if err != nil {
			return false, err
		}
		if authority.AuthorityID == preparation.ProcessAuthorityID {
			return authority.AuthorityGeneration == generation && authority.OwnerID == preparation.ProcessOwnerID && authority.BootID == preparation.ProcessBootID, nil
		}
		id = authority.PredecessorAuthorityID
	}
	return false, nil
}
