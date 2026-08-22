package serveapp

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func completeServeTestPackContext(t testing.TB, contextDef runtimepkg.BundleContext) runtimepkg.BundleContext {
	t.Helper()
	bundle, ok := semanticview.Bundle(contextDef.Source)
	if !ok || bundle == nil || bundle.PackInventory == nil {
		return contextDef
	}
	catalog, _, err := providertriggers.NewCatalogSnapshotFromInventory(
		bundle.PackInventory,
		strings.TrimSpace(bundle.Platform.Platform.Version),
	)
	if err != nil {
		t.Fatalf("derive test runtime-context provider-trigger catalog: %v", err)
	}
	subjects, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatalf("derive test runtime-context installed provider-trigger subjects: %v", err)
	}
	contextDef.PackInventoryDigest = bundle.PackInventory.Digest()
	contextDef.ProviderTriggerGeneration = catalog.Generation()
	contextDef.InstalledTriggerSubjects = subjects
	return contextDef
}

func serveTestBundlePackCandidate(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle) *bundlePackCandidate {
	t.Helper()
	loaded, err := cliapp.LoadBundlePackRuntime(
		context.Background(),
		cliapp.RuntimeConfigLoadResult{Config: &config.Config{}},
		bundle,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("load test bundle pack runtime: %v", err)
	}
	subjects, err := loaded.ProviderTriggers.Catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatalf("derive test bundle installed provider-trigger subjects: %v", err)
	}
	return &bundlePackCandidate{
		catalog:           loaded.ProviderTriggers.Catalog,
		generation:        loaded.ProviderTriggers.Catalog.Generation(),
		installedSubjects: subjects,
		inventoryDigest:   bundle.PackInventory.Digest(),
		channelPlans:      loaded.Channels.Plans,
		channelBindings:   loaded.Channels.Bindings,
	}
}
