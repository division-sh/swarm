package entitystore

import (
	"fmt"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type EntityPostgresOwner struct {
	backend *postgresbackend.Backend
}

func NewPostgres(backend *postgresbackend.Backend) (*EntityPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("entity postgres backend is required")
	}
	return &EntityPostgresOwner{backend: backend}, nil
}

type EntitySQLiteOwner struct {
	backend *sqlitebackend.Backend
}

func NewSQLite(backend *sqlitebackend.Backend) (*EntitySQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("entity sqlite backend is required")
	}
	return &EntitySQLiteOwner{backend: backend}, nil
}
