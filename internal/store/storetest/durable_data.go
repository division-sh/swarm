package storetest

import (
	"context"
	"testing"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
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
	return private.RegisterDurableDataCatalogForTest(ctx, selected, catalog)
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
	return private.MaterializeDataForkPinsForTest(ctx, selected, sourceRunID, forkRunID, targetBundleHash, overrides, replay)
}
