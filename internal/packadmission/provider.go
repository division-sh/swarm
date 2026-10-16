package packadmission

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// ProviderDefinition freezes every role from one already-selected directory.
// It is declaration evidence, not an enabled ingress or installed transport.
type ProviderDefinition struct {
	provider          packartifact.AdmittedProvider
	triggers          *providertriggers.CatalogSnapshot
	connector         providerconnectors.ConnectorManifest
	connectorIdentity packs.PackIdentity
	channel           packs.SatisfactionPlan
	hasChannel        bool
}

func (p ProviderDefinition) Valid() bool                             { return p.provider.Valid() }
func (p ProviderDefinition) Provider() packartifact.AdmittedProvider { return p.provider }
func (p ProviderDefinition) TriggerCatalog() (*providertriggers.CatalogSnapshot, bool) {
	return p.triggers, p.triggers != nil
}
func (p ProviderDefinition) ConnectorDescriptor() (packs.ConnectorPackDescriptor, bool) {
	if p.connectorIdentity.ID() == "" {
		return packs.ConnectorPackDescriptor{}, false
	}
	tools := make(map[string]runtimecontracts.ToolSchemaEntry, len(p.connector.Tools))
	for name, tool := range p.connector.Tools {
		tools[name] = tool
	}
	return packs.ConnectorPackDescriptor{Identity: p.connectorIdentity, Provider: p.provider.Key(), Tools: tools}, true
}
func (p ProviderDefinition) ChannelPlan() (packs.SatisfactionPlan, bool) {
	return p.channel, p.hasChannel
}

func AdmitProviderDirectory(provider packartifact.AdmittedProvider, platform runtimecontracts.PlatformSpecDocument) (ProviderDefinition, error) {
	if !provider.Valid() {
		return ProviderDefinition{}, fmt.Errorf("provider admission requires a captured provider directory")
	}
	if err := provider.Metadata().ValidatePlatformVersion(platform.Platform.Version); err != nil {
		return ProviderDefinition{}, err
	}
	out := ProviderDefinition{provider: provider}
	if body, present := provider.Body(packmodel.BodyTrigger); present {
		manifest, err := providertriggers.AdmitBody(body)
		if err != nil {
			return ProviderDefinition{}, err
		}
		out.triggers, err = providertriggers.NewCatalogSnapshotFromAdmittedBodies(manifest)
		if err != nil {
			return ProviderDefinition{}, err
		}
	}
	if body, present := provider.Body(packmodel.BodyConnector); present {
		var err error
		out.connector, err = providerconnectors.AdmitBody(body)
		if err != nil {
			return ProviderDefinition{}, err
		}
		out.connectorIdentity, err = packs.NewProviderBodyIdentity(body)
		if err != nil {
			return ProviderDefinition{}, err
		}
	}
	if _, present := provider.Body(packmodel.BodyChannel); present {
		connector, hasConnector := out.ConnectorDescriptor()
		trigger, hasTrigger := out.triggers.PackDescriptorForProvider(provider.Key())
		if !hasTrigger || !hasConnector {
			return ProviderDefinition{}, fmt.Errorf("provider channel requires its own trigger and connector BODY admission")
		}
		interfaces, err := packs.NewInterfaceRegistry(platform)
		if err != nil {
			return ProviderDefinition{}, err
		}
		out.channel, err = packs.CompileProviderChannel(interfaces, provider, trigger, connector)
		if err != nil {
			return ProviderDefinition{}, err
		}
		out.hasChannel = true
	}
	return out, nil
}
