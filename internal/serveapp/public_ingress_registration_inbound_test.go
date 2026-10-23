package serveapp

import (
	"context"

	"errors"
	"fmt"

	"path/filepath"
	"strings"

	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"

	"github.com/division-sh/swarm/internal/runtime/executionposture"

	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"

	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

func TestResolveServeRegistrationPairsRejectsUnsignedIngressTarget(t *testing.T) {
	sourceRoot := filepath.Join(writeStandingTelegramServeFixture(t, "http://127.0.0.1:1"), "telegram-ingress")
	module, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), sourceRoot)
	if err != nil {
		t.Fatalf("load standing fixture: %v", err)
	}
	catalog := testProviderTriggerCatalog(t)
	source := processIngressTransportSource(t, bundle, catalog)
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{"bot": "telegram-bot-token", "webhook_signing.telegram": "telegram-secret"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatal(err)
		}
	}
	credentialOwner, err := runtimecredentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	botSeal, err := credentialOwner.SealCurrentValue(context.Background(), "bot")
	if err != nil {
		t.Fatal(err)
	}
	admission, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram_raw",
		Declaration: providertriggers.AdmissionDeclaration{
			Kind: "raw", Acknowledge: providertriggers.UnsignedWebhookAcknowledgement,
			Authentication: providertriggers.RawAuthenticationDeclaration{Kind: "none"},
			Event:          "inbound.telegram.raw",
			DeliveryID:     providertriggers.RawDeliveryIDDeclaration{Source: "body_sha256"},
			Payload:        "json",
		},
	})
	if err != nil {
		t.Fatalf("CompileAdmission: %v", err)
	}

	bundleHash := "bundle-v2:sha256:" + strings.Repeat("b", 64)
	workOwner := newSupervisorTestRuntimeOccurrence(t, bundleHash)
	bus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEphemeralEventBus: %v", err)
	}
	t.Cleanup(func() { _ = bus.ResetInMemoryState() })
	target := runtimepkg.StandingTarget{
		BundleHash: bundleHash, ServiceID: runtimeflowidentity.StandingServiceID("."),
		FlowPath: ".", Alias: "chat", Provider: "telegram_raw",
		RunID: "41000000-0000-0000-0000-000000000001",

		Generation: 1, PublicationSequence: 1, AdmissionPlan: admission,
	}
	emptyPublication, err := channelonboarding.NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	activationOwner, err := runtimechannelactivation.NewOwner(emptyPublication)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runtimepkg.NewRuntimeContextManager(nil, completeServeTestPackContext(t, runtimepkg.BundleContext{
		SourceArtifactFact: mustServeTestEphemeralSourceArtifactFact(bundleHash),
		Source:             source,
		Runtime: &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: bus, ChannelActivations: activationOwner,
			Options: runtimepkg.RuntimeOptions{RuntimeInstanceID: uuid.NewString(), WorkflowModule: module,
				ProviderTriggerCatalog: catalog, ProviderCredentials: credentialStore}},
		WorkOwner:                 workOwner,
		StandingTargets:           []runtimepkg.StandingTarget{target},
		ProviderTriggerGeneration: catalog.Generation(),
	}))
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	t.Cleanup(func() { _ = manager.QuiesceAllRuntimeContexts(context.Background()) })

	plan := loadSupportedTelegramChannelPlan(t)
	binding, err := packs.NewOutboundBindingPlanWithRegistration(
		"telegram", plan, "42", nil,
		map[string]string{"telegram_bot_token": "bot"},
		"ingress:.:telegram_raw",
	)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlanWithRegistration: %v", err)
	}
	contextDef := manager.LoadedContexts()[0]
	coordinate, err := declaredActivationCoordinate(contextDef, binding)
	if err != nil {
		t.Fatal(err)
	}
	bundleHash = contextDef.SourceArtifactFact.BundleHash()
	compiled := channelonboarding.CompiledActivation{
		Source:     channelonboarding.ActivationSourceDeclared,
		Coordinate: coordinate,
		Plan:       binding,
		CredentialAdmissions: []channelonboarding.CredentialAdmission{{
			Role: "telegram_bot_token", StoreKey: "bot", Kind: channelonboarding.CredentialAdmissionObserved,
			ValueSeal: botSeal.Seal,
		}},
	}
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{compiled})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ReplaceChannelActivationsContext(context.Background(), bundleHash, contextDef.PublicationGeneration, publication); err != nil {
		t.Fatal(err)
	}
	snapshot := serveChannelActivationSnapshot{}
	selection, err := resolveServeRegistrationPairs(snapshot, manager)
	var pairs []runtimepublicingress.RegistrationPair
	if selection != nil {
		defer selection.Release()
		pairs = selection.Pairs
	}
	if err == nil || !strings.Contains(err.Error(), "requires a signing credential role") || !strings.Contains(err.Error(), "UNAUTHENTICATED") {
		t.Fatalf("resolveServeRegistrationPairs pairs=%#v err=%v, want unsigned signing-role contradiction", pairs, err)
	}
	if selection != nil || len(pairs) != 0 {
		t.Fatalf("unsigned target retained registration authority: selection=%#v pairs=%#v", selection, pairs)
	}
}

func TestDeclaredActivationRejectsUnusableCredentialValues(t *testing.T) {
	plan := loadSupportedTelegramChannelPlan(t)
	binding, err := packs.NewOutboundBindingPlanWithRegistration(
		"telegram", plan, "42", nil,
		map[string]string{"telegram_bot_token": "bot", "webhook_signing_secret": "signing"},
		"ingress:telegram-ingress:telegram",
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"bot", "signing"} {
		for _, value := range []string{"", " \t\n "} {
			t.Run(key+"/"+fmt.Sprintf("%q", value), func(t *testing.T) {
				store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				for credentialKey, credentialValue := range map[string]string{"bot": "provider-token", "signing": "signing-token"} {
					if credentialKey == key {
						credentialValue = value
					}
					if err := store.Set(context.Background(), credentialKey, credentialValue); err != nil {
						t.Fatal(err)
					}
				}
				owner, err := runtimecredentials.NewSnapshotOwner(store)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := declaredActivationCredentialAdmissions(context.Background(), owner, channelonboarding.ChannelRuntimeContextCoordinate{}, binding); !errors.Is(err, runtimecredentials.ErrCredentialValueUnusable) {
					t.Fatalf("declared activation unusable credential error = %v", err)
				}
			})
		}
	}
}

func TestDeclaredRegistrationWithoutEnabledTargetStaysDiscoverable(t *testing.T) {
	ctx := context.Background()
	sourceRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	_, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), sourceRoot)
	if err != nil {
		t.Fatal(err)
	}
	catalog := testProviderTriggerCatalog(t)
	source := processIngressTransportSource(t, bundle, catalog)
	plan := loadSupportedTelegramChannelPlan(t)
	binding, err := packs.NewOutboundBindingPlanWithRegistration("telegram", plan, "42", nil,
		map[string]string{"telegram_bot_token": "bot", "webhook_signing_secret": "webhook_signing.telegram"}, "ingress:telegram-ingress:telegram")
	if err != nil {
		t.Fatal(err)
	}
	declared, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication([]packs.OutboundBindingPlan{binding})
	if err != nil {
		t.Fatal(err)
	}
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	workOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	bus, err := runtimebus.NewEphemeralEventBusWithOptions(&processIngressEventStore{}, runtimebus.EventBusOptions{
		WorkOwner: workOwner, ContractBundle: source, SourceArtifactFact: mustServeTestEphemeralSourceArtifactFact(hash),
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runtimepkg.NewRuntimeContextManager(nil, completeServeTestPackContext(t, runtimepkg.BundleContext{
		SourceArtifactFact: mustServeTestEphemeralSourceArtifactFact(hash), Source: source,
		BundleIdentity: runtimecontracts.BundleIdentity{WorkflowName: "telegram", WorkflowVersion: "1", BundleHash: hash},
		Runtime: &runtimepkg.Runtime{Bus: bus, ExecutionPosture: executionposture.Live, Options: runtimepkg.RuntimeOptions{
			RuntimeInstanceID: uuid.NewString(), DeclaredChannelPublication: declared, ChannelPlans: []packs.SatisfactionPlan{plan},
		}}, WorkOwner: workOwner, ChannelPlans: []packs.SatisfactionPlan{plan},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.QuiesceAllRuntimeContexts(context.Background()) })
	file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := runtimecredentials.NewSnapshotOwner(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, provisioned := range []bool{false, true} {
		if provisioned {
			for key, value := range map[string]string{"bot": "bot-token", "webhook_signing.telegram": "signing-value"} {
				if err := file.Set(ctx, key, value); err != nil {
					t.Fatal(err)
				}
			}
		}
		snapshot, err := compileServeChannelActivationSnapshot(ctx, manager, nil, nil, owner)
		if err != nil || len(snapshot.Activations) != 0 || len(snapshot.Prebinding) != 0 {
			t.Fatalf("disabled declared registration provisioned=%t = %#v, %v", provisioned, snapshot, err)
		}
		candidates, err := serveChannelOnboardingCatalog(manager)
		if err != nil || len(candidates.Candidates()) != 1 {
			t.Fatalf("declaration discovery = %#v, %v", candidates, err)
		}
		candidate := candidates.Candidates()[0]
		if candidate.Target.Generation != 0 || candidate.Target.PublicationSequence != 0 || candidate.Coordinate.TargetGeneration != 0 || candidate.Validate() == nil {
			t.Fatalf("discovery invented executable authority: %#v", candidate)
		}
	}
}

func loadSupportedTelegramChannelPlan(t *testing.T) packs.SatisfactionPlan {
	t.Helper()
	repo := repoRootForTest()
	snapshot, err := yamlsource.LoadFile(filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	spec, err = runtimecontracts.AdmitPlatformSpecValue(snapshot.Document(filepath.Join(repo, "platform-spec.yaml")).Root())
	if err != nil {
		t.Fatalf("decode platform spec: %v", err)
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatalf("NewInterfaceRegistry: %v", err)
	}
	channels := packfixture.ChannelPacks(t)
	triggers := packfixture.TriggerCatalog(t)
	var connector packs.ConnectorPackDescriptor
	for _, candidate := range packfixture.ConnectorRegistry(t).PackDescriptors() {
		if candidate.Identity.ID() == "provider.telegram.connector" {
			connector = candidate
			break
		}
	}
	plan, err := packs.CompileChannel(registry, channels[0], triggers.PackDescriptors(), []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatalf("CompileChannel: %v", err)
	}
	return plan
}
