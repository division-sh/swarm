package serveapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func prepareServeBundleSource(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle, dev bool) (runtimecorrelation.BundleSourceFact, error) {
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("derive canonical bundle hash: %w", err)
	}
	catalog := stores.facade().bundleSourceCatalogStore()
	if dev || catalog == nil {
		return runtimecorrelation.NewEphemeralBundleSourceFact(bundleHash)
	}
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("project bundle catalog row: %w", err)
	}
	if projection.BundleHash != bundleHash {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("bundle catalog projection hash %q does not match source fact %q", projection.BundleHash, bundleHash)
	}
	if _, err := catalog.UpsertBundleCatalog(ctx, bundlecatalog.Upsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return runtimecorrelation.NewPersistedBundleSourceFact(bundleHash)
}
