// Package construction owns the raw database handles used while assembling a
// serve process. Runtime consumers receive only the typed store returned with
// each handle.
package construction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	storestartupownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
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

// OpenSQLiteRuntimeWithOwnershipBinding is the mutable process/repair
// construction path. It binds the opened pool to the exact database and
// possession-coordinate identities that later retained possession must prove.
func OpenSQLiteRuntimeWithOwnershipBinding(path string) (*private.SQLiteRuntimeStore, *sql.DB, error) {
	constructionGuard, err := storestartupownership.AcquireSQLiteConstructionGuard(path)
	if err != nil {
		return nil, nil, err
	}
	schema, backend, err := storeschema.OpenSQLiteForConstruction(path)
	if err != nil {
		return nil, nil, errors.Join(err, constructionGuard.Release())
	}
	identity, err := constructionGuard.BackendIdentity(context.Background())
	if err != nil {
		return nil, nil, errors.Join(err, schema.Close(), constructionGuard.Release())
	}
	store, err := private.ComposeSQLiteRuntimeStoreWithBackendIdentity(schema, backend, identity)
	if err != nil {
		return nil, nil, errors.Join(err, identity.ReleaseConstructionPossession(), schema.Close(), constructionGuard.Release())
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
