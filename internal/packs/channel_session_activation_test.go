package packs_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/channelactivation"
)

func sessionCompiledActivationFixture(t *testing.T) (channelonboarding.CompiledActivation, channelonboarding.Candidate) {
	t.Helper()
	plan, trigger := discordPaperPort(t)
	binding, err := packs.NewOutboundBindingPlanWithRegistration("native_selected", plan, "123456789012345678", nil,
		map[string]string{"discord_bot_token": "discord.provider"}, "ingress:.:discord")
	if err != nil {
		t.Fatal(err)
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	activation := channelonboarding.CompiledActivation{
		Source: channelonboarding.ActivationSourceLearned, OnboardingOperationID: "native-parent", OnboardingRevision: 1, ActivationRevision: 1,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), BundleIdentity: "native@1.0.0#bundle",
			PackInventoryGeneration: "sha256:inventory", RuntimeInstanceID: "11111111-1111-4111-8111-111111111111",
			ContextPublicationGeneration: 1, PlanGeneration: generation, TargetGeneration: 1},
		Plan: binding, CredentialAdmissions: []channelonboarding.CredentialAdmission{
			{Role: "discord_bot_token", StoreKey: "discord.provider", Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: packTestCredentialSeal("a")},
		},
		SessionAccount: operatorchannel.SessionAccountAdmission{Provider: "discord", ConnectionID: "22222222-2222-4222-8222-222222222222",
			AccountRef: "123456789012345678", AdmissionID: "33333333-3333-4333-8333-333333333333", Revision: 1},
	}
	profile, found := plan.OnboardingProfile()
	identity, err := plan.InterfaceIdentity()
	if !found || err != nil {
		t.Fatal("session plan has no exact onboarding/identity owner", err)
	}
	candidate := channelonboarding.Candidate{Provider: "discord", Interface: identity, Coordinate: activation.Coordinate, Plan: plan,
		Posture: channelonboarding.ActivationSessionConnection, Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge,
		ProviderCredentialRole: profile.ProviderCredential(), ConfirmationOperation: profile.ConfirmationOperation(), ConnectionHealth: profile.ConnectionHealth(),
		Target: channelonboarding.CandidateTarget{Selector: "ingress:.:discord", ServiceID: "fixture-standing-service", FlowPath: ".", Alias: "discord", Provider: "discord",
			Generation: 1, PublicationSequence: 1, AdmissionGeneration: trigger.Generation}}
	if err := candidate.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := activation.Validate(); err != nil {
		t.Fatal(err)
	}
	return activation, candidate
}

func TestSessionActivationRequiresDeploymentAdmission(t *testing.T) {
	base, _ := sessionCompiledActivationFixture(t)
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{base})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := channelactivation.NewOwner(publication); err == nil {
		t.Fatal("bare constructor installed an unqualified native provider")
	}
	empty, err := channelonboarding.NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := channelactivation.NewOwner(empty)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Replace(publication); err == nil {
		t.Fatal("bare replacement installed an unqualified native provider")
	}
	refusal := errors.New("deployment owner refused native authority")
	if err := owner.ReplaceAdmittedContext(context.Background(), publication, func(context.Context, channelonboarding.ChannelActivationPublication) error { return refusal }); !errors.Is(err, refusal) {
		t.Fatal("replacement ignored its deployment refusal", err)
	}
	current, ok := owner.AcquirePresentation()
	if !ok {
		t.Fatal("refused replacement lost the previous publication")
	}
	defer current.Release()
	if !current.Generation().Equal(empty.Generation()) {
		t.Fatal("refused replacement published native tools")
	}
}

func TestSessionActivationRequiresSelectedAccountEvidence(t *testing.T) {
	activation, _ := sessionCompiledActivationFixture(t)
	activation.SessionAccount = operatorchannel.SessionAccountAdmission{}
	if err := activation.Validate(); err == nil {
		t.Fatal("native executable projection without selected account provenance was accepted")
	}
}

func TestSessionActivationProjectionPreservesAndPinsExactAccount(t *testing.T) {
	base, candidate := sessionCompiledActivationFixture(t)
	selected := channelonboarding.ConnectedChannelActivation{
		Status: channelonboarding.ActivationCurrent, SlotKey: "native-slot", OperationID: base.OnboardingOperationID,
		OperationRevision: base.OnboardingRevision, Revision: base.ActivationRevision, Coordinate: base.Coordinate,
		Interface: candidate.Interface, Provider: candidate.Provider, TargetSelector: candidate.Target.Selector,
		Posture: candidate.Posture, BindingRevision: 1, ConversationRef: "123456789012345678",
		CredentialAdmissions: base.CredentialAdmissions, SessionAccount: base.SessionAccount,
	}
	compiled, err := channelonboarding.CompileLearnedActivation(candidate, selected)
	if err != nil || compiled.SessionAccount != base.SessionAccount {
		t.Fatal("compiler lost selected session provenance", compiled.SessionAccount, err)
	}
	responsibility, err := compiled.AdmissionResponsibility()
	if err != nil || responsibility.SessionAccount != selected.SessionAccount || responsibility.TargetSelector != selected.TargetSelector ||
		responsibility.OperationID != selected.OperationID || responsibility.OperationRevision != selected.OperationRevision ||
		responsibility.ActivationRevision != selected.Revision || responsibility.Coordinate != selected.Coordinate {
		t.Fatal("canonical responsibility lost selected scope", responsibility, err)
	}
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{compiled})
	if err != nil || publication.Activations()[0].SessionAccount != selected.SessionAccount {
		t.Fatal("publication lost selected session provenance", err)
	}
	for name, mutate := range map[string]func(*operatorchannel.SessionAccountAdmission){
		"connection": func(a *operatorchannel.SessionAccountAdmission) {
			a.ConnectionID = "44444444-4444-4444-8444-444444444444"
		},
		"account": func(a *operatorchannel.SessionAccountAdmission) { a.AccountRef = "123456789012345679" },
		"admission": func(a *operatorchannel.SessionAccountAdmission) {
			a.AdmissionID = "55555555-5555-4555-8555-555555555555"
		},
		"revision": func(a *operatorchannel.SessionAccountAdmission) { a.Revision++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := compiled
			mutate(&changed.SessionAccount)
			other, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{changed})
			if err != nil || other.Generation().Equal(publication.Generation()) {
				t.Fatal("native publication identity ignored an account-provenance change", err)
			}
		})
	}
}

func TestSessionActivationRejectsContradictoryAccountAndSource(t *testing.T) {
	base, _ := sessionCompiledActivationFixture(t)
	for name, mutate := range map[string]func(*channelonboarding.CompiledActivation){
		"missing": func(a *channelonboarding.CompiledActivation) {
			a.SessionAccount = operatorchannel.SessionAccountAdmission{}
		},
		"provider":   func(a *channelonboarding.CompiledActivation) { a.SessionAccount.Provider = "whatsapp" },
		"connection": func(a *channelonboarding.CompiledActivation) { a.SessionAccount.ConnectionID = "" },
		"account":    func(a *channelonboarding.CompiledActivation) { a.SessionAccount.AccountRef = "" },
		"admission":  func(a *channelonboarding.CompiledActivation) { a.SessionAccount.AdmissionID = "" },
		"revision":   func(a *channelonboarding.CompiledActivation) { a.SessionAccount.Revision = 0 },
		"declared": func(a *channelonboarding.CompiledActivation) {
			a.Source = channelonboarding.ActivationSourceDeclared
			a.OnboardingOperationID = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if _, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{changed}); err == nil {
				t.Fatal("native publication admitted contradictory selected provenance")
			}
			if _, err := changed.AdmissionResponsibility(); err == nil {
				t.Fatal("native responsibility admitted contradictory selected provenance")
			}
		})
	}
}
