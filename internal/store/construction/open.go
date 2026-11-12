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

	"github.com/division-sh/swarm/internal/channelprovider"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	storestartupownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
	"github.com/lib/pq"
)

func OpenPostgres(dsn string, providerFacts channelprovider.FactsLookup) (*private.PostgresStore, *sql.DB, error) {
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open postgres: %w", err)
	}
	connector, err := pq.NewConnectorConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("open postgres: %w", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	backend, err := postgresbackend.NewWithInspectionConfig(db, cfg)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	store, err := private.ComposePostgresStore(backend, providerFacts)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return store, db, nil
}

func OpenPostgresRuntime(ctx context.Context, dsn string, providerFacts channelprovider.FactsLookup) (*private.PostgresStore, *sql.DB, error) {
	selected, handle, err := OpenPostgres(dsn, providerFacts)
	if err != nil {
		return nil, nil, err
	}
	if err := selected.RetainConstructionPossession(ctx); err != nil {
		return nil, nil, errors.Join(err, selected.Close())
	}
	return selected, handle, nil
}

func OpenSQLiteRuntime(path string, providerFacts channelprovider.FactsLookup) (*private.SQLiteRuntimeStore, *sql.DB, error) {
	schema, backend, err := storeschema.OpenSQLiteForConstruction(path)
	if err != nil {
		return nil, nil, err
	}
	store, err := private.ComposeSQLiteRuntimeStore(schema, backend, providerFacts)
	if err != nil {
		_ = schema.Close()
		return nil, nil, err
	}
	return store, backend.ConstructionHandle(), nil
}

func OpenSQLiteRuntimeForServing(ctx context.Context, path string, providerFacts channelprovider.FactsLookup) (*private.SQLiteRuntimeStore, *sql.DB, error) {
	constructionGuard, err := storestartupownership.WaitSQLiteConstructionGuard(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	return openSQLiteUnderPossession(ctx, path, constructionGuard, providerFacts)
}

func openSQLiteUnderPossession(ctx context.Context, path string, constructionGuard *storestartupownership.SQLiteConstructionGuard, providerFacts channelprovider.FactsLookup) (*private.SQLiteRuntimeStore, *sql.DB, error) {
	schema, backend, err := storeschema.OpenSQLiteForConstruction(path)
	if err != nil {
		return nil, nil, errors.Join(err, constructionGuard.Release())
	}
	identity, err := constructionGuard.BackendIdentity(ctx)
	if err != nil {
		return nil, nil, errors.Join(err, schema.Close(), constructionGuard.Release())
	}
	store, err := private.ComposeSQLiteRuntimeStoreWithBackendIdentity(schema, backend, identity, providerFacts)
	if err != nil {
		return nil, nil, errors.Join(err, identity.ReleaseConstructionPossession(), schema.Close(), constructionGuard.Release())
	}
	return store, backend.ConstructionHandle(), nil
}

func OpenSQLiteRuntimeReadOnly(path string, providerFacts channelprovider.FactsLookup) (*private.SQLiteRuntimeStore, error) {
	identity, err := storestartupownership.CaptureSQLiteInspectionIdentity(path)
	if err != nil {
		return nil, err
	}
	schema, backend, err := storeschema.OpenSQLiteReadOnlyForInspection(path)
	if err != nil {
		return nil, err
	}
	store, err := private.ComposeSQLiteRuntimeStoreWithBackendIdentity(schema, backend, identity, providerFacts)
	if err != nil {
		_ = schema.Close()
		return nil, err
	}
	return store, nil
}

func ObserveSQLiteInspectionAbsence(ctx context.Context, path string) (bool, error) {
	return storestartupownership.ObserveSQLiteInspectionAbsence(ctx, path)
}

func OpenPostgresReadOnly(dsn string, providerFacts channelprovider.FactsLookup) (*private.PostgresStore, error) {
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL inspection: %w", err)
	}
	backend, err := postgresbackend.OpenForInspection(cfg)
	if err != nil {
		return nil, err
	}
	store, err := private.ComposePostgresStore(backend, providerFacts)
	if err != nil {
		return nil, errors.Join(err, backend.Close())
	}
	return store, nil
}

func NewPostgres(dsn string, providerFacts channelprovider.FactsLookup) (*private.PostgresStore, error) {
	store, _, err := OpenPostgres(dsn, providerFacts)
	return store, err
}

func NewSQLiteRuntime(path string, providerFacts channelprovider.FactsLookup) (*private.SQLiteRuntimeStore, error) {
	store, _, err := OpenSQLiteRuntime(path, providerFacts)
	return store, err
}
