package startupownership

import (
	"context"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

// ProcessAuthorityCurrent proves a process-owned control against the same
// retained-possession lineage used by startup and prepared execution.
func ProcessAuthorityCurrent(ctx context.Context, q authorityQueryer, expected runtimeownership.Authority, sqlite, lock bool) (bool, error) {
	if err := expected.Validate(); err != nil {
		return false, err
	}
	record, found, err := loadAuthorityHeadRecord(ctx, q, sqlite, lock)
	if err != nil || !found {
		return false, err
	}
	backend := "postgres_retained_session"
	if sqlite {
		backend = "sqlite_retained_owner"
	}
	current, err := validateAuthorityLineage(ctx, q, record, backend, sqlite, make(map[string]struct{}))
	if err != nil {
		return false, err
	}
	return current.State == runtimeownership.StateActive && expected.State == runtimeownership.StateActive &&
		current.AuthorityID == expected.AuthorityID && current.AuthorityGeneration == expected.AuthorityGeneration &&
		current.StateVersion == expected.StateVersion && current.OwnerID == expected.OwnerID && current.BootID == expected.BootID &&
		current.RuntimeInstanceID == expected.RuntimeInstanceID && current.Backend == expected.Backend, nil
}
