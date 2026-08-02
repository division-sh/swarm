package store

import (
	"context"
	"fmt"

	runtimeworkflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
	workflowrouteadapter "github.com/division-sh/swarm/internal/store/internal/workflowroute"
)

func (s *PostgresStore) LoadActiveWorkflowRoute(ctx context.Context, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	if s == nil || s.backend == nil || s.backend.db == nil {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return workflowrouteadapter.LoadActivePostgres(ctx, s.backend.db, instancePath)
}

func (s *SQLiteRuntimeStore) LoadActiveWorkflowRoute(ctx context.Context, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	if s == nil || s.backend == nil || s.backend.db == nil {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return workflowrouteadapter.LoadActiveSQLite(ctx, s.backend.db, instancePath)
}
