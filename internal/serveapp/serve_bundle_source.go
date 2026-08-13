package serveapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

type serveBundleSourcePlan struct {
	fact       runtimecorrelation.BundleSourceFact
	projection *runtimecontracts.BundleCatalogProjection
}

func prepareServeBundleSource(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle, dev bool) (runtimecorrelation.BundleSourceFact, error) {
	plan, err := planServeBundleSource(stores, bundle, dev)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return persistServeBundleSourcePlan(ctx, stores, plan)
}

func planServeBundleSource(stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle, dev bool) (serveBundleSourcePlan, error) {
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return serveBundleSourcePlan{}, fmt.Errorf("derive canonical bundle hash: %w", err)
	}
	catalog := stores.facade().bundleSourceCatalogStore()
	if dev || catalog == nil {
		fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(bundleHash)
		return serveBundleSourcePlan{fact: fact}, err
	}
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		return serveBundleSourcePlan{}, fmt.Errorf("project bundle catalog row: %w", err)
	}
	if projection.BundleHash != bundleHash {
		return serveBundleSourcePlan{}, fmt.Errorf("bundle catalog projection hash %q does not match source fact %q", projection.BundleHash, bundleHash)
	}
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact(bundleHash)
	if err != nil {
		return serveBundleSourcePlan{}, err
	}
	return serveBundleSourcePlan{fact: fact, projection: &projection}, nil
}

func persistServeBundleSourcePlan(ctx context.Context, stores storeBundle, plan serveBundleSourcePlan) (runtimecorrelation.BundleSourceFact, error) {
	if err := plan.fact.Validate(); err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("planned bundle source fact: %w", err)
	}
	if plan.fact.IsEphemeral() {
		if plan.projection != nil {
			return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("ephemeral bundle source plan must not carry a catalog projection")
		}
		return plan.fact, nil
	}
	if plan.projection == nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("persisted bundle source plan requires a catalog projection")
	}
	if plan.projection.BundleHash != plan.fact.BundleHash() {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("planned bundle catalog projection hash %q does not match source fact %q", plan.projection.BundleHash, plan.fact.BundleHash())
	}
	catalog := stores.facade().bundleSourceCatalogStore()
	if catalog == nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("persisted bundle source plan requires a bundle catalog store")
	}
	if _, err := catalog.UpsertBundleCatalog(ctx, bundlecatalog.Upsert{
		BundleHash:  plan.projection.BundleHash,
		ContentYAML: plan.projection.ContentYAML,
		ParsedJSON:  plan.projection.ParsedJSON,
		DataBlob:    plan.projection.DataBlob,
		Metadata:    plan.projection.Metadata,
	}); err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return plan.fact, nil
}
