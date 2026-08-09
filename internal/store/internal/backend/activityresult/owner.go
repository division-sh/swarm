package activityresult

import (
	"context"
	"fmt"

	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type ActivityResultPostgresOwner struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
}

type ActivityResultSQLiteOwner struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*ActivityResultPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("activity result postgres backend is required")
	}
	return &ActivityResultPostgresOwner{backend: backend, schemaGuard: schemaGuard}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error) (*ActivityResultSQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("activity result sqlite backend is required")
	}
	return &ActivityResultSQLiteOwner{backend: backend, schemaGuard: schemaGuard}, nil
}

func (s *ActivityResultPostgresOwner) requireCurrentSchema() error {
	if s == nil || s.schemaGuard == nil {
		return fmt.Errorf("activity result postgres schema guard is required")
	}
	return s.schemaGuard()
}

func (s *ActivityResultSQLiteOwner) requireCurrentSchema() error {
	if s == nil || s.schemaGuard == nil {
		return fmt.Errorf("activity result sqlite schema guard is required")
	}
	return s.schemaGuard()
}

func (s *ActivityResultPostgresOwner) LoadRecordedActivityResult(ctx context.Context, request runtimeactivityresult.Query) (runtimeactivityresult.Record, bool, error) {
	if s == nil || s.backend == nil {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeactivityresult.Record{}, false, err
	}
	return LoadPostgres(ctx, s.backend, request)
}

func (s *ActivityResultSQLiteOwner) LoadRecordedActivityResult(ctx context.Context, request runtimeactivityresult.Query) (runtimeactivityresult.Record, bool, error) {
	if s == nil || s.backend == nil {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeactivityresult.Record{}, false, err
	}
	return LoadSQLite(ctx, s.backend, request)
}
