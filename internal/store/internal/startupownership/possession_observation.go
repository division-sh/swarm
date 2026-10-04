package startupownership

import (
	"context"
	"errors"
	"time"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

func (s *StartupPostgresOwner) ProbePossession(ctx context.Context) (runtimestartupownership.PossessionObservation, error) {
	result := runtimestartupownership.PossessionObservation{Backend: "postgres_retained_session", StartedAt: time.Now().UTC()}
	if s == nil || s.backend == nil {
		return result, errors.New("PostgreSQL possession observation requires the selected backend")
	}
	available, err := s.backend.ObserveAdvisoryLock(ctx, runtimeSharedStoreOwnershipLock)
	result.Available, result.FinishedAt = available, time.Now().UTC()
	return result, errors.Join(err, result.Validate(), ctx.Err())
}

func (s *StartupSQLiteOwner) ProbePossession(ctx context.Context) (runtimestartupownership.PossessionObservation, error) {
	result := runtimestartupownership.PossessionObservation{Backend: "sqlite_retained_owner", StartedAt: time.Now().UTC()}
	if s == nil || s.path == "" {
		return result, errors.New("SQLite possession observation requires an existing selected path")
	}
	available, err := probeSQLitePossession(ctx, s.path, s.backendIdentity)
	result.Available, result.FinishedAt = available, time.Now().UTC()
	return result, errors.Join(err, result.Validate(), ctx.Err())
}
