package runtimepersistence

import (
	"context"
	"fmt"

	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
	activityresultadapter "github.com/division-sh/swarm/internal/store/internal/activityresult"
)

func (s *PostgresStore) LoadRecordedActivityResult(ctx context.Context, request runtimeactivityresult.Query) (runtimeactivityresult.Record, bool, error) {
	if s == nil || s.backend == nil {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeactivityresult.Record{}, false, err
	}
	return activityresultadapter.LoadPostgres(ctx, s.backend, request)
}

func (s *SQLiteRuntimeStore) LoadRecordedActivityResult(ctx context.Context, request runtimeactivityresult.Query) (runtimeactivityresult.Record, bool, error) {
	if s == nil || s.backend == nil {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeactivityresult.Record{}, false, err
	}
	return activityresultadapter.LoadSQLite(ctx, s.backend, request)
}
