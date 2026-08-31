package storetest

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

type DurableDataCatalogStore interface {
	EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, runtimedata.Catalog) (sourceartifact.EnsureResult, error)
}

// RequireDurableDataCatalog registers the exact empty data catalog claimed by
// a test run. Tests with authored declarations must register their full catalog.
func RequireDurableDataCatalog(t testing.TB, ctx context.Context, selected DurableDataCatalogStore, bundleHash string) {
	t.Helper()
	if err := registerDurableDataCatalogForTest(ctx, selected, runtimedata.Catalog{BundleHash: bundleHash}); err != nil {
		t.Fatalf("register durable data catalog %s: %v", bundleHash, err)
	}
}

// RequireBundleDataCatalog registers the loader-owned bundle projection and
// its exact declaration/static-data catalog as one selected-store fact.
func RequireBundleDataCatalog(t testing.TB, ctx context.Context, selected DurableDataCatalogStore, bundle *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	catalog, err := runtimecontracts.BuildDurableDataCatalog(bundle)
	if err != nil {
		t.Fatalf("build exact bundle data catalog: %v", err)
	}
	if _, err := selected.EnsureSourceArtifactWithData(ctx, bundle.SourceArtifact, catalog); err != nil {
		t.Fatalf("register exact source artifact and data catalog %s: %v", catalog.BundleHash, err)
	}
}

func registerDurableDataCatalogForTest(ctx context.Context, selected any, catalog runtimedata.Catalog) error {
	db := Database(selected)
	if db == nil {
		return fmt.Errorf("selected store database is required")
	}
	owner, err := durableDataOwnerForSelected(db, selected)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := storedurabledata.RegisterCatalogTx(owner, ctx, tx, catalog, time.Now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

// MaterializeDataForkPins executes the production fork-pin owner against a
// test-selected store. Run lifecycle setup remains the caller's responsibility.
func MaterializeDataForkPins(
	ctx context.Context,
	selected any,
	sourceRunID string,
	forkRunID string,
	targetBundleHash string,
	overrides []runtimedata.ExplicitPin,
	replay bool,
) ([]runtimedata.Pin, error) {
	db := Database(selected)
	if db == nil {
		return nil, fmt.Errorf("selected store database is required")
	}
	owner, err := durableDataOwnerForSelected(db, selected)
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	pins, err := storedurabledata.MaterializeForkPinsTx(
		owner, ctx, tx, sourceRunID, forkRunID, targetBundleHash, overrides, replay, time.Now().UTC(),
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return pins, nil
}

func durableDataOwnerForSelected(db *sql.DB, selected any) (*storedurabledata.Owner, error) {
	switch selected.(type) {
	case *store.PostgresStore:
		backend, err := postgresbackend.New(db)
		if err != nil {
			return nil, err
		}
		return storedurabledata.NewPostgres(backend, func() error { return nil })
	case *store.SQLiteRuntimeStore:
		backend, err := sqlitebackend.New(db)
		if err != nil {
			return nil, err
		}
		return storedurabledata.NewSQLite(backend, func() error { return nil }, time.Now)
	default:
		return nil, fmt.Errorf("unsupported selected store %T", selected)
	}
}
