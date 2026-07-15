package serveapp

import (
	"context"
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

func prepareServeBundleSource(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle, legacyFingerprint string, dev bool) (runtimecorrelation.BundleSourceFact, error) {
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("derive canonical bundle hash: %w", err)
	}
	source := storerunlifecycle.BundleSourcePersisted
	catalog := stores.facade().bundleSourceCatalogStore()
	if dev || catalog == nil {
		source = storerunlifecycle.BundleSourceEphemeral
	}
	fact := runtimecorrelation.BundleSourceFact{
		BundleHash:        bundleHash,
		BundleSource:      source,
		BundleFingerprint: strings.TrimSpace(legacyFingerprint),
	}.Normalized()
	if source == storerunlifecycle.BundleSourceEphemeral {
		return fact, nil
	}
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("project bundle catalog row: %w", err)
	}
	if projection.BundleHash != bundleHash {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("bundle catalog projection hash %q does not match source fact %q", projection.BundleHash, bundleHash)
	}
	if _, err := catalog.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return fact, nil
}
