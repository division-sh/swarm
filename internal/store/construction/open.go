// Package construction owns the raw database handles used while assembling a
// serve process. Runtime consumers receive only the typed store returned with
// each handle.
package construction

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	storestartupownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
	"github.com/lib/pq"
)

func OpenPostgres(dsn string) (*private.PostgresStore, *sql.DB, error) {
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open postgres: %w", err)
	}
	return openPostgresConfig(cfg)
}

func openPostgresConfig(cfg pq.Config) (*private.PostgresStore, *sql.DB, error) {
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
	store, err := private.ComposePostgresStore(backend)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return store, db, nil
}

func OpenPostgresRuntime(ctx context.Context, dsn string, checks ...bootverify.ConstructionChecks) (*private.PostgresStore, *sql.DB, error) {
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		return nil, nil, err
	}
	target, err := postgresCheckTarget(cfg)
	if err != nil {
		return nil, nil, err
	}
	selected, handle, err := openPostgresConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	if err := selected.RetainConstructionPossession(ctx); err != nil {
		return nil, nil, errors.Join(err, selected.Close())
	}
	if len(checks) != 0 && checks[0] != nil {
		if _, err := checks[0].InspectConstruction(ctx, target); err != nil {
			return nil, nil, errors.Join(err, selected.Close())
		}
	}
	return selected, handle, nil
}

// PostgresCheckTarget seals parsed inputs, not DSN spelling. Construction uses
// its original frozen config; a later parser cannot retarget retained exclusion.
func PostgresCheckTarget(dsn string) (bootverify.ConstructionTarget, error) {
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		return bootverify.ConstructionTarget{}, err
	}
	return postgresCheckTarget(cfg)
}

func postgresCheckTarget(cfg pq.Config) (bootverify.ConstructionTarget, error) {
	cfg = cfg.Clone()
	hostAddress, _ := netip.ParseAddr(cfg.Host)
	if hostAddress.IsValid() && hostAddress == cfg.Hostaddr {
		cfg.Hostaddr = netip.Addr{}
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return bootverify.ConstructionTarget{}, err
	}
	return bootverify.PostgresConstructionTarget(string(raw)), nil
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

func OpenSQLiteRuntimeForServing(ctx context.Context, path string, input ...*SQLiteInspectionInput) (*private.SQLiteRuntimeStore, *sql.DB, error) {
	return OpenSQLiteRuntimeWithChecks(ctx, path, nil, input...)
}

func OpenSQLiteRuntimeWithChecks(ctx context.Context, path string, checks bootverify.ConstructionChecks, input ...*SQLiteInspectionInput) (*private.SQLiteRuntimeStore, *sql.DB, error) {
	constructionGuard, err := storestartupownership.WaitSQLitePrewriteGuard(ctx, path, input...)
	if err != nil {
		return nil, nil, err
	}
	if checks != nil {
		target, err := bootverify.SQLiteConstructionTarget(path)
		if err == nil {
			_, err = checks.InspectConstruction(ctx, target)
		}
		if err != nil {
			return nil, nil, errors.Join(err, constructionGuard.Release())
		}
	}
	if err := constructionGuard.PrepareDatabase(ctx); err != nil {
		return nil, nil, errors.Join(err, constructionGuard.Release())
	}
	return openSQLiteUnderPossession(ctx, path, constructionGuard)
}

type SQLiteInspectionInput = storestartupownership.SQLiteInspectionInput

func CaptureSQLiteInspectionInput(ctx context.Context, path string) (*SQLiteInspectionInput, error) {
	return storestartupownership.CaptureSQLiteInspectionInput(ctx, path)
}

func openSQLiteUnderPossession(ctx context.Context, path string, constructionGuard *storestartupownership.SQLiteConstructionGuard) (*private.SQLiteRuntimeStore, *sql.DB, error) {
	schema, backend, err := storeschema.OpenSQLiteForConstruction(path)
	if err != nil {
		return nil, nil, errors.Join(err, constructionGuard.Release())
	}
	identity, err := constructionGuard.BackendIdentity(ctx)
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
	identity, err := storestartupownership.CaptureSQLiteInspectionIdentity(path)
	if err != nil {
		return nil, err
	}
	schema, backend, err := storeschema.OpenSQLiteReadOnlyForInspection(path)
	if err != nil {
		return nil, err
	}
	store, err := private.ComposeSQLiteRuntimeStoreWithBackendIdentity(schema, backend, identity)
	if err != nil {
		_ = schema.Close()
		return nil, err
	}
	return store, nil
}

func ObserveSQLiteInspectionAbsence(ctx context.Context, path string) (bool, error) {
	return storestartupownership.ObserveSQLiteInspectionAbsence(ctx, path)
}

func OpenPostgresReadOnly(dsn string, expected ...*bootverify.ConstructionTarget) (*private.PostgresStore, error) {
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL inspection: %w", err)
	}
	if len(expected) != 0 && expected[0] != nil {
		actual, err := postgresCheckTarget(cfg)
		if err != nil || actual != *expected[0] {
			return nil, errors.Join(errors.New("PostgreSQL inspection inputs changed after native acquisition"), err)
		}
	}
	backend, err := postgresbackend.OpenForInspection(cfg)
	if err != nil {
		return nil, err
	}
	store, err := private.ComposePostgresStore(backend)
	if err != nil {
		return nil, errors.Join(err, backend.Close())
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
