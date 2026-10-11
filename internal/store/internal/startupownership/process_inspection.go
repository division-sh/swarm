package startupownership

import (
	"context"
	"database/sql"
	"errors"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
)

type authorityQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *StartupPostgresOwner) InspectAuthority(ctx context.Context) (runtimeownership.AuthorityInspection, error) {
	if s == nil || s.catalogEmpty == nil || s.backend == nil {
		return runtimeownership.AuthorityInspection{}, errors.New("PostgreSQL generation inspection requires the selected reader")
	}
	empty, err := s.catalogEmpty(ctx)
	if err != nil {
		return runtimeownership.AuthorityInspection{}, err
	}
	return inspectProcessGeneration(ctx, s.backend, "postgres_retained_session", empty, false)
}

func (s *StartupSQLiteOwner) InspectAuthority(ctx context.Context) (runtimeownership.AuthorityInspection, error) {
	if s == nil || s.catalogEmpty == nil || s.backend == nil {
		return runtimeownership.AuthorityInspection{}, errors.New("SQLite generation inspection requires the selected reader")
	}
	empty, err := s.catalogEmpty(ctx)
	if err != nil {
		return runtimeownership.AuthorityInspection{}, err
	}
	return inspectProcessGeneration(ctx, s.backend, "sqlite_retained_owner", empty, true)
}

func inspectProcessGeneration(ctx context.Context, q authorityQueryer, backend string, empty, sqlite bool) (runtimeownership.AuthorityInspection, error) {
	result := runtimeownership.AuthorityInspection{Status: runtimeownership.AuthorityInspectionEmpty, Backend: backend}
	if !empty {
		stamp, err := generationauthority.ReadProcessGeneration(ctx, q, sqlite)
		if err != nil {
			return runtimeownership.AuthorityInspection{}, err
		}
		if stamp.Generation != 0 {
			authority := runtimeownership.AuthorityFromLease(stamp)
			result.Authority, result.Status = &authority, runtimeownership.AuthorityInspectionValid
		}
	}
	return result, result.Validate()
}
