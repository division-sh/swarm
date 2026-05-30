package main

import (
	"context"
	"fmt"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/store"
	storerunlifecycle "swarm/internal/store/runlifecycle"
)

func prepareServeBundleSource(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle, legacyFingerprint string, dev bool) (runtimecorrelation.BundleSourceFact, error) {
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("derive canonical bundle hash: %w", err)
	}
	source := storerunlifecycle.BundleSourcePersisted
	if dev {
		source = storerunlifecycle.BundleSourceEphemeral
	}
	fact := runtimecorrelation.BundleSourceFact{
		BundleHash:        bundleHash,
		BundleSource:      source,
		BundleFingerprint: strings.TrimSpace(legacyFingerprint),
	}.Normalized()
	if dev {
		return fact, nil
	}
	if stores.Postgres == nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("postgres bundle catalog store is required for persisted serve bundle source")
	}
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("project bundle catalog row: %w", err)
	}
	if projection.BundleHash != bundleHash {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("bundle catalog projection hash %q does not match source fact %q", projection.BundleHash, bundleHash)
	}
	if _, err := stores.Postgres.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
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
