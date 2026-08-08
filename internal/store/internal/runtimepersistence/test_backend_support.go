package runtimepersistence

import (
	"database/sql"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

// NewPostgresStoreForTest binds an externally managed test database to the
// selected-store facade. Production construction must use NewPostgresStore.
func NewPostgresStoreForTest(db *sql.DB) *PostgresStore {
	return &PostgresStore{backend: mustPostgresBackend(db)}
}

func mustPostgresBackend(db *sql.DB) *postgresbackend.Backend {
	backend, err := postgresbackend.New(db)
	if err != nil {
		panic(err)
	}
	return backend
}

// NewSQLiteRuntimeStoreForTest binds an externally managed test database to
// the selected-store facade. Production construction must use
// NewSQLiteRuntimeStore.
func NewSQLiteRuntimeStoreForTest(db *sql.DB) *SQLiteRuntimeStore {
	backend, err := sqlitebackend.New(db)
	if err != nil {
		panic(err)
	}
	return &SQLiteRuntimeStore{
		schema:  &SQLiteSchemaStore{backend: backend},
		backend: backend,
	}
}

// DatabaseForTest exposes a fixture-owned database for exact persistence
// readback without adding a raw capability to either selected-store facade.
func DatabaseForTest(selected any) *sql.DB {
	switch store := selected.(type) {
	case *PostgresStore:
		if store != nil && store.backend != nil {
			return store.backend.ConstructionHandle()
		}
	case *SQLiteRuntimeStore:
		if store != nil && store.backend != nil {
			return store.backend.ConstructionHandle()
		}
	}
	return nil
}
