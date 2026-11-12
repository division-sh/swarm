package serveapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelprovider"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type installedChannelBinding struct {
	facts        channelprovider.Binding
	registration cliapp.ChannelRegistrationConstructor
	deployment   func(context.Context, sessionprovider.DeploymentConnectionOptions) (serveSessionConnection, error)
	runtime      func(context.Context, sessionprovider.RuntimeConnectionOptions, bool) (serveSessionConnection, error)
	qualify      func(packs.SatisfactionPlan) error
}

var installedChannelProviders = [...]installedChannelBinding{
	{
		facts: channelprovider.Define(channelprovider.Definition{
			ID: "telegram", DisplayName: "Telegram", Webhook: true, PollClaims: true, Credential: true, SigningCredential: true,
			CanonicalAccount: channelprovider.TelegramAccount, ClaimInstructions: channelprovider.TelegramClaimInstructions,
			RegistrationTeaching:         "Telegram ready. Run `swarm serve` to answer customers. Decisions: answer them in the terminal with `swarm mailbox decide`.",
			OmitBoundRegistrationReceipt: true,
			LanguageHelp: func(operation string) string {
				return fmt.Sprintf("Declare the Telegram language used on this device: swarm channel resume %s --client-language <language>.\n", operation)
			},
		}),
		registration: openTelegramChannelRegistration,
	},
	{
		facts: channelprovider.Define(channelprovider.Definition{
			ID: "whatsapp", DisplayName: "WhatsApp", Session: true,
			CanonicalAccount: channelprovider.WhatsAppAccount, ClaimInstructions: channelprovider.WhatsAppClaimInstructions,
			NativeRead: "whatsapp.read_account", NativeWrite: "whatsapp.send_text", CaptureKey: "whatsapp_capture",
			ReadyTeaching: "WhatsApp ready. Run `swarm serve` to answer customers. Decisions: answer them in the terminal with `swarm mailbox decide`.",
			NotLinked:     "Not linked: run swarm channel connect whatsapp",
			PairingPrompt: "Open WhatsApp > Linked Devices > Link a device, then scan (expires %s):\n%s\n",
		}),
		registration: openWhatsAppChannelRegistration,
		deployment:   openWhatsAppDeploymentConnection,
		runtime:      openWhatsAppRuntimeConnection,
		qualify:      qualifyWhatsAppPlan,
	},
}

func installedChannelProvider(provider string) (installedChannelBinding, bool) {
	for _, binding := range installedChannelProviders {
		if binding.facts.MatchesProvider(provider) {
			return binding, true
		}
	}
	return installedChannelBinding{}, false
}

func installedChannelFactsFor(provider string) (channelprovider.Binding, bool) {
	binding, found := installedChannelProvider(provider)
	return binding.facts, found
}

func installedChannelFacts() []channelprovider.Binding {
	facts := make([]channelprovider.Binding, 0, len(installedChannelProviders))
	for _, binding := range installedChannelProviders {
		facts = append(facts, binding.facts)
	}
	return facts
}

type channelRegistrations struct {
	configure func(cliapp.ChannelRegistrationOptions) cliapp.ChannelRegistrationOptions
	opened    func(cliapp.ChannelRegistrationSession)
}

func ChannelRegistrations() cliapp.ChannelRegistrationFactory { return channelRegistrations{} }

func (r channelRegistrations) Providers() []channelprovider.Binding { return installedChannelFacts() }

func (r channelRegistrations) NativeFacts() channelprovider.FactsLookup {
	return installedChannelFactsFor
}

func (r channelRegistrations) AdmitInventory(rooted sourceartifact.RootedSource, inventory *packartifact.EffectivePackInventory, spec runtimecontracts.PlatformSpecDocument) (runtimecontracts.PackAdmissionProjection, error) {
	value, err := packadmission.AdmitInventory(rooted, inventory, spec)
	if err != nil {
		return nil, err
	}
	projection := value.(packadmission.Projection)
	for _, binding := range installedChannelProviders {
		projection = projection.WithProviderFacts(binding.facts)
	}
	return projection, nil
}

func isInstalledSession(provider string) bool {
	facts, found := installedChannelFactsFor(provider)
	return found && facts.Session()
}

func (r channelRegistrations) Select(provider string) (cliapp.ChannelRegistrationBinding, bool) {
	binding, found := installedChannelProvider(provider)
	if !found || binding.registration == nil {
		return cliapp.ChannelRegistrationBinding{}, false
	}
	open := func(ctx context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
		if r.configure != nil {
			opts = r.configure(opts)
		}
		value, err := openChannelRegistration(ctx, opts, binding)
		if err == nil && r.opened != nil {
			r.opened(value)
		}
		return value, err
	}
	return cliapp.ChannelRegistrationBinding{Facts: binding.facts, Open: open}, true
}
