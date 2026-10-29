package packadmission

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// Projection is the canonical, config-independent admission result for every
// body in one exact effective inventory. Deployment bindings and credentials
// are intentionally downstream concerns.
type Projection struct {
	Inventory           *packartifact.EffectivePackInventory
	ProviderTriggers    *providertriggers.CatalogSnapshot
	LoadedProviderPacks []providertriggers.LoadedPack
	ProviderConnectors  *providerconnectors.PackRegistry
	ChannelPlans        []packs.SatisfactionPlan
	providerSource      ProviderSource
}

func (p Projection) EffectivePackInventoryDigest() string {
	if p.Inventory == nil {
		return ""
	}
	return p.Inventory.Digest()
}

func (p Projection) PackSourceValues() map[string]yamlsource.Value {
	out := p.ProviderConnectors.SourceValues()
	if p.Inventory != nil {
		for _, entry := range p.Inventory.Entries() {
			if body, present := entry.Provider().Body(packmodel.BodyChannel); present {
				out[entry.ID()+".channel"] = body.SourceValue()
			}
		}
	}
	return out
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
	var plans []packs.SatisfactionPlan
	for _, entry := range inventory.Entries() {
		definition, err := AdmitProviderDirectory(entry.Provider(), platform)
		if err != nil {
			return Projection{}, fmt.Errorf("admit provider directory %q: %w", entry.ID(), err)
		}
		if _, present := definition.ChannelPlan(); present {
			interfaces, err := packs.NewInterfaceRegistry(platform)
			if err != nil {
				return Projection{}, err
			}
			trigger, hasTrigger := triggers.PackDescriptorForProvider(entry.ID())
			connector, hasConnector := definition.ConnectorDescriptor()
			if !hasTrigger || !hasConnector {
				return Projection{}, fmt.Errorf("provider channel %q requires its exact inventory role descriptors", entry.ID())
			}
			plan, err := packs.CompileProviderChannel(interfaces, definition.Provider(), trigger, connector)
			if err != nil {
				return Projection{}, fmt.Errorf("admit inventory provider channel %q: %w", entry.ID(), err)
			}
			plans = append(plans, plan)
		}
	}
	return Projection{
		Inventory: inventory, ProviderTriggers: triggers, LoadedProviderPacks: loadedTriggers,
		ProviderConnectors: connectors, ChannelPlans: plans,
	}, nil
}

func AdmitInventory(rooted sourceartifact.RootedSource, inventory *packartifact.EffectivePackInventory, platform runtimecontracts.PlatformSpecDocument) (runtimecontracts.PackAdmissionProjection, error) {
	projection, err := Admit(inventory, platform)
	if err != nil {
		return nil, err
	}
	projection.providerSource, err = AdmitProviderSource(rooted, platform)
	if err != nil {
		return nil, err
	}
	return projection, nil
}

// ProviderSourceForBundle carries the rooted load's admission, without sharing
// it across loads. Plain bundles without that admission still admit directly.
func ProviderSourceForBundle(bundle *runtimecontracts.WorkflowContractBundle) (ProviderSource, error) {
	if bundle == nil {
		return ProviderSource{}, fmt.Errorf("provider source requires its compiled bundle")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return ProviderSource{}, err
	}
	if projection, ok := bundle.PackAdmission.(Projection); ok && projection.providerSource.rooted.Artifact() != nil {
		providers := projection.providerSource
		if !providers.Valid() || !providers.rooted.Admission().Equal(rooted.Admission()) || providers.rooted.Artifact() != rooted.Artifact() {
			return ProviderSource{}, fmt.Errorf("provider source admission does not own the compiled rooted source")
		}
		return providers, nil
	}
	return AdmitProviderSource(rooted, bundle.Platform)
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
