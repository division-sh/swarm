package packadmission

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packruntime"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// Projection is the canonical, config-independent admission result for every
// body in one exact effective inventory. Deployment bindings and credentials
// are intentionally downstream concerns.
type Projection struct {
	Inventory           *packartifact.EffectivePackInventory
	ProviderTriggers    *providertriggers.CatalogSnapshot
	LoadedProviderPacks []providertriggers.LoadedPack
	ProviderConnectors  *providerconnectors.PackRegistry
	LoadedChannelPacks  []packs.LoadedChannelPack
	ChannelPlans        []packs.SatisfactionPlan
}

func (p Projection) EffectivePackInventoryDigest() string {
	if p.Inventory == nil {
		return ""
	}
	return p.Inventory.Digest()
}

func Admit(inventory *packartifact.EffectivePackInventory, platform runtimecontracts.PlatformSpecDocument) (Projection, error) {
	if inventory == nil {
		return Projection{}, fmt.Errorf("effective pack inventory is required")
	}
	runningVersion := strings.TrimSpace(platform.Platform.Version)
	if runningVersion == "" {
		return Projection{}, fmt.Errorf("platform version is required for pack admission")
	}
	triggers, loadedTriggers, err := providertriggers.NewCatalogSnapshotFromInventory(inventory, runningVersion)
	if err != nil {
		return Projection{}, fmt.Errorf("admit provider trigger packs: %w", err)
	}
	connectors, err := providerconnectors.NewPackRegistryFromInventory(inventory, runningVersion)
	if err != nil {
		return Projection{}, fmt.Errorf("admit provider connector packs: %w", err)
	}
	channels, err := packruntime.LoadChannelPacks(inventory, runningVersion)
	if err != nil {
		return Projection{}, fmt.Errorf("admit channel packs: %w", err)
	}
	interfaces, err := packs.NewInterfaceRegistry(platform)
	if err != nil {
		return Projection{}, fmt.Errorf("admit channel interfaces: %w", err)
	}
	plans, err := packs.CompileChannelInventory(interfaces, channels, triggers.PackDescriptors(), connectors.PackDescriptors())
	if err != nil {
		return Projection{}, fmt.Errorf("admit channel inventory: %w", err)
	}
	return Projection{
		Inventory: inventory, ProviderTriggers: triggers, LoadedProviderPacks: loadedTriggers,
		ProviderConnectors: connectors, LoadedChannelPacks: channels, ChannelPlans: plans,
	}, nil
}

func AdmitInventory(inventory *packartifact.EffectivePackInventory, platform runtimecontracts.PlatformSpecDocument) (runtimecontracts.PackAdmissionProjection, error) {
	projection, err := Admit(inventory, platform)
	if err != nil {
		return nil, err
	}
	return projection, nil
}

func FromBundle(bundle *runtimecontracts.WorkflowContractBundle) (Projection, error) {
	if bundle == nil || bundle.PackInventory == nil {
		return Projection{}, fmt.Errorf("workflow bundle effective pack inventory is required")
	}
	projection, ok := bundle.PackAdmission.(Projection)
	if !ok || projection.Inventory == nil {
		return Projection{}, fmt.Errorf("workflow bundle admitted pack projection is required")
	}
	if projection.Inventory != bundle.PackInventory || projection.EffectivePackInventoryDigest() != bundle.PackInventory.Digest() {
		return Projection{}, fmt.Errorf("workflow bundle admitted pack projection does not own effective inventory %s", bundle.PackInventory.Digest())
	}
	return projection, nil
}
