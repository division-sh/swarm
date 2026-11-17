package cliapp

import (
	"context"
	"net/http"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/channelprovider"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// ChannelRegistrationSession is the invocation-owned composition of existing
// session/identity ports. It grants no runtime activation or business execution.
type ChannelRegistrationSession interface {
	channelonboarding.SessionBootstrapOwner
	operatorchannel.CredentialCurrentness
	operatorchannel.SessionAdmissionOwner
	channelonboarding.DeploymentSessionOwner
	channelonboarding.RegisteredAcknowledgement
	PollOperatorClaim(context.Context, channelonboarding.Operation) error
	ConsumeWebhookClaim(context.Context, channelonboarding.AccountReference, func(channelonboarding.CredentialAdmission) bool, func(providertriggers.InboundAdmissionPlan) (providertriggers.Delivery, error), func() bool) (bool, error)
	ReadSessionConnection(context.Context, channelonboarding.Operation) (channelonboarding.SessionConnectionReadback, error)
	Close(context.Context) error
}

type ChannelRegistrationOptions struct {
	OperatorAccount channelonboarding.AccountReference
	OperatorBinding func(context.Context, channelonboarding.Operation) (channelonboarding.Result, error)
	Effects         effects.Store
	Directory       string
	Principal       operatorchannel.Principal
	Process         *worklifetime.Process
	Operations      channelonboarding.OperationStore
	Store           channelonboarding.Store
	Credentials     *credentials.SnapshotOwner
	Identities      operatorchannel.Store
	HTTP            *http.Client
	Plan            packs.SatisfactionPlan
	Trigger         providertriggers.InboundAdmissionPlan
	Webhook         providertriggers.InboundAdmissionPlan
	CurrentWebhook  func(context.Context, string, string) bool
}

type ChannelRegistrationConstructor func(context.Context, ChannelRegistrationOptions) (ChannelRegistrationSession, error)

type ChannelRegistrationBinding struct {
	Facts channelprovider.Binding
	Open  ChannelRegistrationConstructor
}

// The executable selects one compiled constructor before lower validation.
// Validators receive Facts, never this construction view.
type ChannelRegistrationFactory interface {
	Select(string) (ChannelRegistrationBinding, bool)
	Providers() []channelprovider.Binding
	NativeFacts() channelprovider.FactsLookup
	AdmitInventory(sourceartifact.RootedSource, *packartifact.EffectivePackInventory, runtimecontracts.PlatformSpecDocument) (runtimecontracts.PackAdmissionProjection, error)
}

func nativeChannelFacts(factory ChannelRegistrationFactory) channelprovider.FactsLookup {
	if factory == nil {
		return nil
	}
	return factory.NativeFacts()
}

func selectedChannelFacts(factory ChannelRegistrationFactory, provider string) channelprovider.Binding {
	if factory == nil {
		return channelprovider.Binding{}
	}
	selected, found := factory.Select(provider)
	if !found {
		return channelprovider.Binding{}
	}
	return selected.Facts.ForProvider(provider)
}

func bindChannelReadbackFacts(target any, factory ChannelRegistrationFactory, selectedFact channelprovider.Binding) {
	factsFor := func(provider string) channelprovider.Binding {
		if factory == nil {
			return selectedFact.ForProvider(provider)
		}
		return selectedChannelFacts(factory, provider)
	}
	switch result := target.(type) {
	case *channelonboarding.StartSelection:
		result.Candidate = result.Candidate.WithProviderFacts(factsFor(result.Candidate.Provider))
	case *channelonboarding.Result:
		*result = result.WithProviderFacts(factsFor(result.Operation.Provider))
	case *map[string][]channelonboarding.Result:
		for key, rows := range *result {
			for i, row := range rows {
				rows[i] = row.WithProviderFacts(factsFor(row.Operation.Provider))
			}
			(*result)[key] = rows
		}
	case *channelListResult:
		result.facts = make([]channelprovider.Binding, len(result.Channels))
		for i := range result.Channels {
			row := &result.Channels[i]
			var facts channelprovider.Binding
			if row.Operation != nil && row.Operation.Interface.Normalized() == row.Identity.Interface.Normalized() {
				facts = factsFor(row.Operation.Provider)
				op := row.Operation.WithProviderFacts(facts)
				row.Operation = &op
			} else if row.Activation != nil && row.Activation.Interface.Normalized() == row.Identity.Interface.Normalized() {
				facts = factsFor(row.Activation.Provider)
			}
			result.facts[i] = facts
		}
	}
}
