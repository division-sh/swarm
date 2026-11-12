package cliapp

import (
	"github.com/division-sh/swarm/internal/channelprovider"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/tests/support/channelfixture"
)

type channelRegistrationFixture struct{}

func (channelRegistrationFixture) Select(provider string) (ChannelRegistrationBinding, bool) {
	facts, found := channelfixture.Facts(provider)
	return ChannelRegistrationBinding{Facts: facts}, found
}

func (channelRegistrationFixture) Providers() []channelprovider.Binding {
	return []channelprovider.Binding{channelfixture.For("telegram"), channelfixture.For("whatsapp")}
}

func (channelRegistrationFixture) NativeFacts() channelprovider.FactsLookup { return channelfixture.Facts }

func (channelRegistrationFixture) AdmitInventory(rooted sourceartifact.RootedSource, inventory *packartifact.EffectivePackInventory, spec contracts.PlatformSpecDocument) (contracts.PackAdmissionProjection, error) {
	value, err := packadmission.AdmitInventory(rooted, inventory, spec)
	if err != nil {
		return nil, err
	}
	projection := value.(packadmission.Projection)
	for _, facts := range (channelRegistrationFixture{}).Providers() {
		projection = projection.WithProviderFacts(facts)
	}
	return projection, nil
}
