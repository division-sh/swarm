package cliapp

import (
	"context"
	"fmt"
	"strings"

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
	triggers, err := LoadBundleProviderTriggerPacks(bundle)
	if err != nil {
		return BundlePackRuntimeLoad{}, fmt.Errorf("load provider trigger packs: %w", err)
	}
	connectors, err := providerconnectors.NewPackRegistryFromInventory(bundle.PackInventory, strings.TrimSpace(bundle.Platform.Platform.Version))
	if err != nil {
		return BundlePackRuntimeLoad{}, fmt.Errorf("load provider connector packs: %w", err)
	}
	channels, err := LoadConfiguredChannelPacks(ctx, cfgResult, bundle.Platform, bundle.PackInventory, triggers.Catalog, connectors, staticCredentials, managedCredentials)
	if err != nil {
		return BundlePackRuntimeLoad{}, fmt.Errorf("load channel packs: %w", err)
	}
	return BundlePackRuntimeLoad{ProviderTriggers: triggers, Connectors: connectors, Channels: channels}, nil
}
