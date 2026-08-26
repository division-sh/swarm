package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

type DurableDataCatalogStore interface {
	UpsertBundleCatalogWithData(context.Context, bundlecatalog.Upsert, runtimedata.Catalog) (bundlecatalog.UpsertResult, error)
}

// RequireDurableDataCatalog registers the exact empty data catalog claimed by
// a test run. Tests with authored declarations must register their full catalog.
func RequireDurableDataCatalog(t testing.TB, ctx context.Context, selected DurableDataCatalogStore, bundleHash string) {
	t.Helper()
	if _, err := selected.UpsertBundleCatalogWithData(ctx, bundlecatalog.Upsert{
		BundleHash:  bundleHash,
		ContentYAML: "api_version: swarm.bundle.catalog.test.v1\n",
		ParsedJSON:  map[string]any{"projection_version": "swarm.bundle.catalog.v2", "agents": []any{}},
		Metadata:    map[string]any{"source": "test-fixture"},
	}, runtimedata.Catalog{BundleHash: bundleHash}); err != nil {
		t.Fatalf("register durable data catalog %s: %v", bundleHash, err)
	}
}

// RequireBundleDataCatalog registers the loader-owned bundle projection and
// its exact declaration/static-data catalog as one selected-store fact.
func RequireBundleDataCatalog(t testing.TB, ctx context.Context, selected DurableDataCatalogStore, bundle *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("build exact bundle data catalog: %v", err)
	}
	if _, err := selected.UpsertBundleCatalogWithData(ctx, bundlecatalog.Upsert{
		BundleHash: projection.BundleHash, ContentYAML: projection.ContentYAML, ParsedJSON: projection.ParsedJSON,
		DataBlob: projection.DataBlob, Metadata: projection.Metadata,
	}, projection.DataCatalog); err != nil {
		t.Fatalf("register exact bundle data catalog %s: %v", projection.BundleHash, err)
	}
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
	var owner *storedurabledata.Owner
	switch selected.(type) {
	case *store.PostgresStore:
		backend, err := postgresbackend.New(db)
		if err != nil {
			return nil, err
		}
		owner, err = storedurabledata.NewPostgres(backend, func() error { return nil })
		if err != nil {
			return nil, err
		}
	case *store.SQLiteRuntimeStore:
		backend, err := sqlitebackend.New(db)
		if err != nil {
			return nil, err
		}
		owner, err = storedurabledata.NewSQLite(backend, func() error { return nil }, time.Now)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported selected store %T", selected)
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
