package cliapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
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
	bindings, sources, err := compileChannelBindings(ctx, cfgResult.Config, projection.ChannelPlans, staticCredentials, managedCredentials)
	if err != nil {
		return BundlePackRuntimeLoad{}, fmt.Errorf("load channel bindings: %w", err)
	}
	plans, err := capturedChannelProjectionPlans(bundle)
	if err != nil {
		return BundlePackRuntimeLoad{}, err
	}
	triggers := ProviderTriggerPackLoad{
		Catalog: projection.ProviderTriggers, Loaded: projection.LoadedProviderPacks, Inventory: bundle.PackInventory,
	}
	channels := ChannelPackLoad{
		Plans: plans, Bindings: bindings,
		credentialSources: sources,
	}
	return BundlePackRuntimeLoad{ProviderTriggers: triggers, Connectors: projection.ProviderConnectors, Channels: channels}, nil
}

func capturedChannelProjectionPlans(bundle *runtimecontracts.WorkflowContractBundle) ([]packs.SatisfactionPlan, error) {
	providers, err := packadmission.ProviderSourceForBundle(bundle)
	if err != nil {
		return nil, err
	}
	interfaces, err := packs.NewInterfaceRegistry(bundle.Platform)
	if err != nil {
		return nil, err
	}
	var plans []packs.SatisfactionPlan
	for _, flow := range providers.FlowPaths() {
		scope, err := providers.Scope(flow)
		if err != nil {
			return nil, err
		}
		catalog, err := scope.TriggerCatalog()
		if err != nil {
			return nil, err
		}
		var inputs []string
		for _, pin := range bundle.FlowInputEventPins(flow) {
			inputs = append(inputs, pin.EventType())
		}
		for _, definition := range scope.ConsumedTriggerProviders(inputs) {
			if _, present := definition.ChannelPlan(); !present {
				continue
			}
			provider := definition.Provider()
			trigger, hasTrigger := catalog.PackDescriptorForProvider(provider.Key())
			connector, hasConnector := definition.ConnectorDescriptor()
			if !hasTrigger || !hasConnector {
				return nil, fmt.Errorf("channel projection for %s requires its captured trigger and connector", provider.Directory())
			}
			plan, err := packs.CompileProviderChannel(interfaces, provider, trigger, connector)
			if err != nil {
				return nil, err
			}
			plans = append(plans, plan)
		}
	}
	return plans, nil
}
