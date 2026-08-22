// Package construction owns the raw database handles used while assembling a
// serve process. Runtime consumers receive only the typed store returned with
// each handle.
package construction

import (
	"database/sql"
	"fmt"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	_ "github.com/lib/pq"
)

func OpenPostgres(dsn string) (*private.PostgresStore, *sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	backend, err := postgresbackend.New(db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	store, err := private.ComposePostgresStore(backend)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return store, db, nil
}

func OpenSQLiteRuntime(path string) (*private.SQLiteRuntimeStore, *sql.DB, error) {
	schema, backend, err := storeschema.OpenSQLiteForConstruction(path)
	if err != nil {
		return nil, nil, err
	}
	store, err := private.ComposeSQLiteRuntimeStore(schema, backend)
	if err != nil {
		_ = schema.Close()
		return nil, nil, err
	}
	return store, backend.ConstructionHandle(), nil
}

func OpenSQLiteRuntimeReadOnly(path string) (*private.SQLiteRuntimeStore, error) {
	schema, backend, err := storeschema.OpenSQLiteReadOnlyForInspection(path)
	if err != nil {
		return nil, err
	}
	store, err := private.ComposeSQLiteRuntimeStore(schema, backend)
	if err != nil {
		_ = schema.Close()
		return nil, err
	}
	return store, nil
}

func NewPostgres(dsn string) (*private.PostgresStore, error) {
	store, _, err := OpenPostgres(dsn)
	return store, err
}

func NewSQLiteRuntime(path string) (*private.SQLiteRuntimeStore, error) {
	store, _, err := OpenSQLiteRuntime(path)
	return store, err
}
