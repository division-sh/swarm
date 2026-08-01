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
	return &SQLiteRuntimeStore{
		SQLiteSchemaStore: &SQLiteSchemaStore{backend: &sqliteRuntimeBackend{db: db}},
	}
}

// TestDatabase exposes the fixture-owned database for exact persistence
// readback. It is not a runtime capability surface.
func (s *PostgresStore) TestDatabase() *sql.DB {
	if s == nil || s.backend == nil {
		return nil
	}
	return s.backend.db
}

// TestDatabase exposes the fixture-owned database for exact persistence
// readback. It is not a runtime capability surface.
func (s *SQLiteRuntimeStore) TestDatabase() *sql.DB {
	if s == nil || s.backend == nil {
		return nil
	}
	return s.backend.db
}
