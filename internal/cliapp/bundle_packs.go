package cliapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
)

type BundlePackRuntimeLoad struct {
	ProviderTriggers ProviderTriggerPackLoad
	Connectors       *providerconnectors.PackRegistry
	Channels         ChannelPackLoad
}

func LoadBundlePackRuntime(ctx context.Context, cfgResult RuntimeConfigLoadResult, bundle *runtimecontracts.WorkflowContractBundle, staticCredentials runtimecredentials.Store, managedCredentials runtimemanagedcredentials.Store) (BundlePackRuntimeLoad, error) {
	if bundle == nil || bundle.PackInventory == nil {
		return BundlePackRuntimeLoad{}, fmt.Errorf("workflow bundle effective pack inventory is required")
	}
	if cfgResult.Config == nil {
		return BundlePackRuntimeLoad{}, fmt.Errorf("runtime config is required")
	}
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		return BundlePackRuntimeLoad{}, err
	}
	bindings, err := compileChannelBindings(ctx, cfgResult.Config, projection.ChannelPlans, staticCredentials, managedCredentials)
	if err != nil {
		return BundlePackRuntimeLoad{}, fmt.Errorf("load channel bindings: %w", err)
	}
	triggers := ProviderTriggerPackLoad{
		Catalog: projection.ProviderTriggers, Loaded: projection.LoadedProviderPacks, Inventory: bundle.PackInventory,
	}
	channels := ChannelPackLoad{
		Loaded: projection.LoadedChannelPacks, Plans: projection.ChannelPlans, Bindings: bindings,
	}
	return BundlePackRuntimeLoad{ProviderTriggers: triggers, Connectors: projection.ProviderConnectors, Channels: channels}, nil
}
