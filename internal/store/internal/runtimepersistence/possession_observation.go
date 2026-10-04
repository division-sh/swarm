package runtimepersistence

import (
	"context"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func (s *PostgresStore) ProbePossession(ctx context.Context) (startupownership.PossessionObservation, error) {
	return s.startupPostgresOwner.ProbePossession(ctx)
}

func (s *SQLiteRuntimeStore) ProbePossession(ctx context.Context) (startupownership.PossessionObservation, error) {
	return s.startupSQLiteOwner.ProbePossession(ctx)
}
