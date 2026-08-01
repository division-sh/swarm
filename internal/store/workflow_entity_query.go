package store

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/store/internal/workflowentityquery"
)

func (s *PostgresStore) CountWorkflowEntities(ctx context.Context, request entityquery.Request) (int, error) {
	if s == nil || s.backend == nil || s.backend.db == nil {
		return 0, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return 0, err
	}
	return workflowentityquery.CountPostgres(ctx, s.backend.db, request)
}

func (s *SQLiteRuntimeStore) CountWorkflowEntities(ctx context.Context, request entityquery.Request) (int, error) {
	if s == nil || s.SQLiteSchemaStore == nil || s.SQLiteSchemaStore.backend == nil || s.SQLiteSchemaStore.backend.db == nil {
		return 0, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return 0, err
	}
	return workflowentityquery.CountSQLite(ctx, s.SQLiteSchemaStore.backend.db, request)
}
