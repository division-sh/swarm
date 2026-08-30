package sourceartifactstore

import (
	"fmt"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type Postgres struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*Postgres, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("source artifact postgres backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("source artifact postgres schema guard is required")
	}
	return &Postgres{backend: backend, schemaGuard: schemaGuard}, nil
}

type SQLite struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	now         func() time.Time
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error, now func() time.Time) (*SQLite, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("source artifact sqlite backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("source artifact sqlite schema guard is required")
	}
	if now == nil {
		now = time.Now
	}
	return &SQLite{backend: backend, schemaGuard: schemaGuard, now: now}, nil
}
