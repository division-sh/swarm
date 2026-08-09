package entitystore

import (
	"fmt"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type EntityPostgresOwner struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*EntityPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("entity postgres backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("entity postgres schema guard is required")
	}
	return &EntityPostgresOwner{backend: backend, schemaGuard: schemaGuard}, nil
}

type EntitySQLiteOwner struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	nowFn       func() time.Time
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error, nowFn func() time.Time) (*EntitySQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("entity sqlite backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("entity sqlite schema guard is required")
	}
	if nowFn == nil {
		nowFn = time.Now
	}
	return &EntitySQLiteOwner{backend: backend, schemaGuard: schemaGuard, nowFn: nowFn}, nil
}

func (s *EntitySQLiteOwner) now() time.Time {
	if s == nil || s.nowFn == nil {
		return time.Now().UTC()
	}
	return s.nowFn().UTC()
}
