package store

import "database/sql"

// NewPostgresStoreForTest binds an externally managed test database to the
// selected-store facade. Production construction must use NewPostgresStore.
func NewPostgresStoreForTest(db *sql.DB) *PostgresStore {
	return &PostgresStore{backend: &postgresRuntimeBackend{db: db}}
}

// NewSQLiteRuntimeStoreForTest binds an externally managed test database to
// the selected-store facade. Production construction must use
// NewSQLiteRuntimeStore.
func NewSQLiteRuntimeStoreForTest(db *sql.DB) *SQLiteRuntimeStore {
	backend := &sqliteRuntimeBackend{db: db}
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
			return store.backend.db
		}
	case *SQLiteRuntimeStore:
		if store != nil && store.backend != nil {
			return store.backend.db
		}
	}
	return nil
}
