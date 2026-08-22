package schemastore

import (
	"context"
	"fmt"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
)

type Postgres struct {
	backend         *postgresbackend.Backend
	schemaAdmission schemaAdmission
}

func (s *Postgres) CatalogEmpty(ctx context.Context) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("postgres schema owner is required")
	}
	tables, err := postgresPublicTables(ctx, s.backend)
	if err != nil {
		return false, err
	}
	return len(tables) == 0, nil
}

func NewPostgres(backend *postgresbackend.Backend) (*Postgres, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("postgres backend is required")
	}
	return &Postgres{backend: backend}, nil
}

func NewMissingPostgresOwnerError() error {
	return fmt.Errorf("postgres schema owner is required")
}

func NewMissingSQLiteOwnerError() error {
	return fmt.Errorf("sqlite schema store is required")
}

func (s *Postgres) AcceptCurrentForTest() {
	if s != nil {
		s.schemaAdmission.markCurrent()
	}
}

func (s *SQLite) AcceptCurrentForTest() {
	if s != nil {
		s.schemaAdmission.markCurrent()
	}
}
