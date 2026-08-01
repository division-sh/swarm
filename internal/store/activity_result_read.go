package store

import (
	"context"
	"fmt"

	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
	activityresultadapter "github.com/division-sh/swarm/internal/store/internal/activityresult"
)

func (s *PostgresStore) LoadRecordedActivityResult(ctx context.Context, request runtimeactivityresult.Query) (runtimeactivityresult.Record, bool, error) {
	if s == nil || s.backend == nil || s.backend.db == nil {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeactivityresult.Record{}, false, err
	}
	return activityresultadapter.LoadPostgres(ctx, s.backend.db, request)
}

func (s *SQLiteRuntimeStore) LoadRecordedActivityResult(ctx context.Context, request runtimeactivityresult.Query) (runtimeactivityresult.Record, bool, error) {
	if s == nil || s.SQLiteSchemaStore == nil || s.SQLiteSchemaStore.backend == nil || s.SQLiteSchemaStore.backend.db == nil {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeactivityresult.Record{}, false, err
	}
	return activityresultadapter.LoadSQLite(ctx, s.SQLiteSchemaStore.backend.db, request)
}
