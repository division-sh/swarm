package packs

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
)

const providerChannelInterface = "swarm.hitl-channel/v2"

// CompileProviderChannel consumes one selected directory and its accepted role
// descriptors. Scope selection belongs to sourceartifact, not first-match here.
func CompileProviderChannel(registry *InterfaceRegistry, provider packartifact.AdmittedProvider, trigger TriggerPackDescriptor, connector ConnectorPackDescriptor) (SatisfactionPlan, error) {
	if registry == nil {
		return SatisfactionPlan{}, fmt.Errorf("channel interface registry is required")
	}
	if !provider.Valid() {
		return SatisfactionPlan{}, fmt.Errorf("channel compilation requires an admitted provider directory")
	}
	body, exists := provider.Body(packmodel.BodyChannel)
	if !exists {
		return SatisfactionPlan{}, fmt.Errorf("provider directory %q has no channel.yaml BODY", provider.Directory())
	}
	manifest, err := AdmitChannelBody(body)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	identity, err := NewProviderBodyIdentity(body)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	compilation, err := prepareChannelCompilation(registry, identity, manifest, providerChannelInterface)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	if err := validateProviderRoleIdentity(provider, packmodel.BodyTrigger, trigger.Identity, trigger.Provider); err != nil {
		return SatisfactionPlan{}, err
	}
	if err := validateProviderRoleIdentity(provider, packmodel.BodyConnector, connector.Identity, connector.Provider); err != nil {
		return SatisfactionPlan{}, err
	}
	return compileChannelSatisfaction(compilation, trigger, connector)
}

func validateProviderRoleIdentity(provider packartifact.AdmittedProvider, role packmodel.BodyRole, actual PackIdentity, key string) error {
	body, present := provider.Body(role)
	if !present {
		return fmt.Errorf("provider directory %q has no %s BODY", provider.Directory(), role.FileName())
	}
	wanted, err := NewProviderBodyIdentity(body)
	if err != nil {
		return err
	}
	if key != provider.Key() || actual.ID() != wanted.ID() || actual.Version() != wanted.Version() || actual.ManifestHash() != wanted.ManifestHash() || actual.Type() != wanted.Type() || actual.Source() != wanted.Source() {
		return fmt.Errorf("provider directory %q %s descriptor does not match its captured BODY identity", provider.Directory(), role.FileName())
	}
	return nil
}
