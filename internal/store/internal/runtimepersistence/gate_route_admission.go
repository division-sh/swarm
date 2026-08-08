package runtimepersistence

import (
	"context"
	"fmt"

	gaterouteadapter "github.com/division-sh/swarm/internal/store/internal/gateroute"
)

func (s *PostgresStore) RequireGateRouteAdmitted(ctx context.Context, runID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return gaterouteadapter.RequirePostgres(ctx, s.backend, runID)
}

func (s *SQLiteRuntimeStore) RequireGateRouteAdmitted(ctx context.Context, runID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return gaterouteadapter.RequireSQLite(ctx, s.backend, runID)
}
