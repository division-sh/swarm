package serveapp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

type supportedRegistrationEffectStore struct {
	*effecttest.Harness
}

func (s *supportedRegistrationEffectStore) IsExternalEffectAuthorityCurrent(context.Context, runtimeeffects.Authority) (bool, error) {
	return true, nil
}

type supportedTelegramRegistrationTransport struct {
	mu          sync.Mutex
	callbackURL string
	applyCount  int
}

func (transport *supportedTelegramRegistrationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	switch {
	case strings.HasSuffix(request.URL.Path, "/getMe"):
		return supportedRegistrationResponse(http.StatusOK, `{"ok":true,"result":{"id":42}}`), nil
	case strings.HasSuffix(request.URL.Path, "/setWebhook"):
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			return nil, err
		}
		transport.callbackURL = strings.TrimSpace(payload["url"].(string))
		transport.applyCount++
		return supportedRegistrationResponse(http.StatusOK, `{"ok":true,"result":true}`), nil
	case strings.HasSuffix(request.URL.Path, "/getWebhookInfo"):
		body, err := json.Marshal(map[string]any{"ok": true, "result": map[string]any{"url": transport.callbackURL}})
		if err != nil {
			return nil, err
		}
		return supportedRegistrationResponse(http.StatusOK, string(body)), nil
	default:
		return supportedRegistrationResponse(http.StatusNotFound, `{}`), nil
	}
}

func (transport *supportedTelegramRegistrationTransport) applies() int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.applyCount
}

func TestProviderRegistrationSigningRotationTraversesRuntimeInboundVerifier(t *testing.T) {
	contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	_, bundle, err := cliapp.NewSwarmWorkflowModule(cliapp.RepoRoot(), contractsRoot, cliapp.ResolvePath(cliapp.RepoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("load standing fixture: %v", err)
	}
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{}
	for _, flow := range bundle.FlowTree.ByID {
		if flow != nil {
			flow.Agents = map[string]runtimecontracts.AgentRegistryEntry{}
		}
	}
	source := semanticview.Wrap(bundle)
	catalog := testProviderTriggerCatalog(t)
	persistence := &processIngressProofStore{}
	eventsStore := &processIngressEventStore{}
	persistence.store = eventsStore
	bundleHash := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	workOwner := newSupervisorTestRuntimeOccurrence(t, bundleHash)
	bus, err := runtimebus.NewEphemeralEventBusWithOptions(eventsStore, runtimebus.EventBusOptions{
		BundleSourceFact:       mustServeTestEphemeralBundleSourceFact(bundleHash),
		ProviderOutputVerifier: catalog,
		WorkOwner:              workOwner, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = bus.ResetInMemoryState() })
	gateway := runtimepkg.NewInboundGateway(bus, nil, nil, executionposture.Live, persistence)
	emptyActivationPublication, err := channelonboarding.NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	activationOwner, err := runtimechannelactivation.NewOwner(emptyActivationPublication)
	if err != nil {
		t.Fatal(err)
	}
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"bot": "telegram-bot-token", "webhook_signing.telegram": "telegram-secret-v1"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}
	gateway.SetCredentialStore(credentialStore)
	admission, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram", SigningSecret: "webhook_signing.telegram",
	})
	if err != nil {
		t.Fatalf("CompileAdmission: %v", err)
	}
	target := runtimepkg.StandingTarget{
		BundleHash: bundleHash, ServiceID: "43000000-0000-0000-0000-000000000001", PackageKey: "telegram-package", FlowID: "telegram-chat",
		Alias: "chat", Provider: "telegram", RunID: "41000000-0000-0000-0000-000000000001", FlowInstance: "telegram-chat/chat",
		InstanceID: "chat", EntityID: "41000000-0000-0000-0000-000000000002", Generation: 1, PublicationSequence: 1,
		SigningSecret: "webhook_signing.telegram", AdmissionPlan: admission,
	}
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatalf("InstalledCapabilitySubjects: %v", err)
	}
	manager, err := runtimepkg.NewRuntimeContextManager(nil, completeServeTestPackContext(t, runtimepkg.BundleContext{
		BundleSourceFact: mustServeTestEphemeralBundleSourceFact(bundleHash), Source: source,
		Runtime: &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: bus, InboundGateway: gateway, ChannelActivations: activationOwner,
			Options: runtimepkg.RuntimeOptions{RuntimeInstanceID: uuid.NewString()}}, WorkOwner: workOwner,
		StandingTargets: []runtimepkg.StandingTarget{target}, ProviderTriggerGeneration: catalog.Generation(), InstalledTriggerSubjects: installed,
	}))
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	t.Cleanup(func() { _ = manager.QuiesceAllRuntimeContexts(context.Background()) })

	channelPlan := loadSupportedTelegramChannelPlan(t)
	learnedBinding, err := packs.NewOutboundBindingPlanWithRegistration(
		"learned-telegram", channelPlan, "42", nil,
		map[string]string{"telegram_bot_token": "bot", "webhook_signing_secret": "channel.generated.signing"},
		"ingress:telegram-package:telegram-chat:telegram",
	)
	if err != nil {
		t.Fatal(err)
	}
	loadedContext := manager.LoadedContexts()[0]
	activationPlanGeneration, err := learnedBinding.PlanGeneration()
	if err != nil {
		t.Fatal(err)
	}
	bundleHash, bundleSource := loadedContext.BundleSourceFact.StorageValues()
	learnedPublication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{{
		Source: channelonboarding.ActivationSourceLearned, OnboardingOperationID: uuid.NewString(), OnboardingRevision: 1,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
			BundleHash: bundleHash, BundleSource: bundleSource, BundleIdentity: "telegram@1.0.0#activation-readiness",
			PackInventoryGeneration: loadedContext.PackInventoryDigest, RuntimeInstanceID: loadedContext.RuntimeInstanceID,
			ContextPublicationGeneration: loadedContext.PublicationGeneration,
			PlanGeneration:               activationPlanGeneration, TargetGeneration: 1,
		},
		ActivationRevision: 1, Plan: learnedBinding,
		CredentialAdmissions: []channelonboarding.CredentialAdmission{
			{Role: "telegram_bot_token", StoreKey: "bot", Kind: channelonboarding.CredentialAdmissionObserved, Receipt: "bot-observation", Epoch: "bot-epoch"},
			{Role: "webhook_signing_secret", StoreKey: "channel.generated.signing", Kind: channelonboarding.CredentialAdmissionWritten, Receipt: "signing-write", Epoch: "signing-epoch"},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ReplaceChannelActivationsContext(context.Background(), bundleHash, loadedContext.PublicationGeneration, learnedPublication); err != nil {
		t.Fatal(err)
	}

	registration := loadSupportedTelegramRegistration(t)
	credentialSnapshots, err := runtimecredentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		t.Fatalf("NewSnapshotOwner: %v", err)
	}
	assertIngressStatus := func(want packs.SubjectStatus) {
		t.Helper()
		subjects, projectionErr := manager.EvaluatedCapabilitySubjects(context.Background(), credentialSnapshots)
		if projectionErr != nil {
			t.Fatal(projectionErr)
		}
		for _, subject := range subjects {
			if subject.Kind == packs.SubjectProviderTrigger && subject.Applicability == "effective" {
				if subject.Status != want {
					t.Fatalf("activation-backed ingress status = %s, want %s", subject.Status, want)
				}
				return
			}
		}
		t.Fatal("effective ingress subject is missing")
	}
	assertIngressStatus(packs.StatusNotReady)
	if err := credentialStore.Set(context.Background(), "channel.generated.signing", "generated-secret"); err != nil {
		t.Fatal(err)
	}
	assertIngressStatus(packs.StatusReady)
	effectsStore := &supportedRegistrationEffectStore{Harness: effecttest.New()}
	transport := &supportedTelegramRegistrationTransport{}
	readiness := runtimepublicingress.NewReadinessOwner(true)
	startup := runtimestartupownership.GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "serve-owner",
		ProcessBootID: uuid.NewString(), BundleHash: bundleHash, BundleSource: "ephemeral",
		RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1, SourceSetRevision: "inbound-registration-test",
		StateVersion: 3, State: runtimestartupownership.GrantAdmitted,
	}
	if err := startup.Validate(); err != nil {
		t.Fatalf("test startup grant: %v", err)
	}
	controller, err := runtimepublicingress.NewProviderRegistrationController(runtimepublicingress.RegistrationControllerOptions{
		CredentialOwner: credentialSnapshots, EffectsStore: effectsStore,
		HTTP:    runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: transport}},
		Posture: executionposture.Live, RuntimeInstanceID: uuid.NewString(),
		StartupAuthority: func() (runtimestartupownership.GrantEvidence, error) { return startup, nil },
		Readiness:        readiness,
	})
	if err != nil {
		t.Fatalf("NewProviderRegistrationController: %v", err)
	}
	planGeneration, err := plangeneration.FromCanonicalValue(map[string]any{"binding": "telegram"})
	if err != nil {
		t.Fatalf("PlanGeneration: %v", err)
	}
	onboardingID := uuid.NewString()
	coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
		BundleHash: bundleHash, BundleSource: "persisted",
		BundleIdentity: "bundle:test@sha256:inbound-registration", PackInventoryGeneration: "sha256:inbound-registration-inventory",
		RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 1,
		PlanGeneration: planGeneration, TargetGeneration: uint64(target.Generation),
	}
	pair := runtimepublicingress.RegistrationPair{
		BindingID: "telegram", PlanGeneration: planGeneration, OnboardingOperationID: onboardingID, OnboardingRevision: 1,
		OnboardingCoordinate: coordinate, PrebindingOperationID: onboardingID, Registration: registration,
		CredentialKeys: map[string]string{"telegram_bot_token": "bot"},
		Target: runtimepublicingress.RegistrationTarget{
			Selector: "ingress:telegram-package:telegram-chat:telegram", BundleHash: bundleHash, ServiceID: target.ServiceID,
			PackageKey: target.PackageKey, FlowID: target.FlowID, Alias: target.Alias, Provider: target.Provider,
			Generation: target.Generation, PublicationSequence: target.PublicationSequence,
			AdmissionPlanGeneration: target.AdmissionPlan.Generation(), SigningCredentialKey: target.SigningSecret,
		},
	}
	now := time.Now().UTC()
	exposure := runtimepublicingress.Generation{ID: uuid.NewString(), Mode: runtimepublicingress.ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: now}
	readiness.SetRuntimeReady(true)
	readiness.SetExposure(runtimepublicingress.ExposureEvidence{GenerationID: exposure.ID, StartupAuthorityID: startup.GrantID, ObservedAt: now, ExpiresAt: now.Add(runtimepublicingress.EvidenceTTL)})
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil {
		t.Fatalf("initial registration reconcile: %v", err)
	}
	initial := readiness.Snapshot(time.Now().UTC())
	if !initial.PublicIngressReady || len(initial.Registrations) != 1 {
		t.Fatalf("initial registration readiness = %#v", initial)
	}
	handler := controller.Handler(runtimeProcessInboundHandler{contexts: manager})
	initialBody := `{"update_id":99,"message":{"message_id":7,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"initial"}}`
	initialRequest := httptest.NewRequest(http.MethodPost, initial.Registrations[0].CallbackURL, strings.NewReader(initialBody))
	initialRequest.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret-v1")
	initialRecorder := httptest.NewRecorder()
	handler.ServeHTTP(initialRecorder, initialRequest)
	if initialRecorder.Code != http.StatusAccepted || len(eventsStore.events) != 2 {
		t.Fatalf("initial registered verifier status/events=%d/%d", initialRecorder.Code, len(eventsStore.events))
	}

	if err := credentialStore.Set(context.Background(), "webhook_signing.telegram", "telegram-secret-v2"); err != nil {
		t.Fatalf("rotate signing credential: %v", err)
	}
	staleCallback := httptest.NewRecorder()
	handler.ServeHTTP(staleCallback, httptest.NewRequest(http.MethodPost, initial.Registrations[0].CallbackURL, strings.NewReader(initialBody)))
	if staleCallback.Code != http.StatusNotFound {
		t.Fatalf("stale callback after rotation status=%d, want 404", staleCallback.Code)
	}
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil {
		t.Fatalf("rotated registration reconcile: %v", err)
	}
	rotated := readiness.Snapshot(time.Now().UTC())
	if !rotated.PublicIngressReady || rotated.Registrations[0].CallbackURL == initial.Registrations[0].CallbackURL || transport.applies() != 2 {
		t.Fatalf("rotated registration = %#v applies=%d", rotated, transport.applies())
	}
	rotatedBody := `{"update_id":100,"message":{"message_id":8,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"rotated"}}`
	oldSecret := httptest.NewRequest(http.MethodPost, rotated.Registrations[0].CallbackURL, strings.NewReader(rotatedBody))
	oldSecret.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret-v1")
	oldSecretRecorder := httptest.NewRecorder()
	handler.ServeHTTP(oldSecretRecorder, oldSecret)
	if oldSecretRecorder.Code != http.StatusUnauthorized || len(eventsStore.events) != 2 {
		t.Fatalf("old secret through fresh registration status/events=%d/%d, want 401/2", oldSecretRecorder.Code, len(eventsStore.events))
	}
	newSecret := httptest.NewRequest(http.MethodPost, rotated.Registrations[0].CallbackURL, strings.NewReader(rotatedBody))
	newSecret.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret-v2")
	newSecretRecorder := httptest.NewRecorder()
	handler.ServeHTTP(newSecretRecorder, newSecret)
	if newSecretRecorder.Code != http.StatusAccepted || len(eventsStore.events) != 4 {
		t.Fatalf("new secret through fresh registration status/events=%d/%d, want 202/4", newSecretRecorder.Code, len(eventsStore.events))
	}
}

func TestResolveServeRegistrationPairsRejectsUnsignedIngressTarget(t *testing.T) {
	catalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatalf("NewCatalogSnapshot: %v", err)
	}
	admission, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram",
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

	bundleHash := "bundle-v1:sha256:" + strings.Repeat("b", 64)
	workOwner := newSupervisorTestRuntimeOccurrence(t, bundleHash)
	bus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEphemeralEventBus: %v", err)
	}
	t.Cleanup(func() { _ = bus.ResetInMemoryState() })
	target := runtimepkg.StandingTarget{
		BundleHash: bundleHash, ServiceID: "43000000-0000-0000-0000-000000000001",
		PackageKey: "telegram-package", FlowID: "telegram-chat", Alias: "chat", Provider: "telegram",
		RunID: "41000000-0000-0000-0000-000000000001", FlowInstance: "telegram-chat/chat",
		InstanceID: "chat", EntityID: "41000000-0000-0000-0000-000000000002",
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
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleSourceFact: mustServeTestEphemeralBundleSourceFact(bundleHash),
		Source:           semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		Runtime: &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: bus, ChannelActivations: activationOwner,
			Options: runtimepkg.RuntimeOptions{RuntimeInstanceID: uuid.NewString()}},
		WorkOwner:                 workOwner,
		StandingTargets:           []runtimepkg.StandingTarget{target},
		ProviderTriggerGeneration: catalog.Generation(),
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	t.Cleanup(func() { _ = manager.QuiesceAllRuntimeContexts(context.Background()) })

	plan := loadSupportedTelegramChannelPlan(t)
	binding, err := packs.NewOutboundBindingPlanWithRegistration(
		"telegram", plan, "42", nil,
		map[string]string{"telegram_bot_token": "bot"},
		"ingress:telegram-package:telegram-chat:telegram",
	)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlanWithRegistration: %v", err)
	}
	contextDef := manager.LoadedContexts()[0]
	generation, err := binding.PlanGeneration()
	if err != nil {
		t.Fatal(err)
	}
	bundleHash, bundleSource := contextDef.BundleSourceFact.StorageValues()
	compiled := channelonboarding.CompiledActivation{
		Source: channelonboarding.ActivationSourceDeclared,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
			BundleHash: bundleHash, BundleSource: bundleSource, BundleIdentity: "test-bundle",
			PackInventoryGeneration: "sha256:test", RuntimeInstanceID: contextDef.RuntimeInstanceID,
			ContextPublicationGeneration: contextDef.PublicationGeneration,
			PlanGeneration:               generation, TargetGeneration: 1,
		},
		Plan: binding,
		CredentialAdmissions: []channelonboarding.CredentialAdmission{{
			Role: "telegram_bot_token", StoreKey: "bot", Kind: channelonboarding.CredentialAdmissionObserved,
			Receipt: "test-receipt", Epoch: "test-epoch",
		}},
	}
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{compiled})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ReplaceChannelActivationsContext(context.Background(), bundleHash, contextDef.PublicationGeneration, publication); err != nil {
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
}

func loadSupportedTelegramRegistration(t *testing.T) packs.CompiledChannelRegistration {
	t.Helper()
	plan := loadSupportedTelegramChannelPlan(t)
	registration, ok := plan.Registration()
	if !ok {
		t.Fatal("Telegram registration plan is missing")
	}
	return registration
}

func loadSupportedTelegramChannelPlan(t *testing.T) packs.SatisfactionPlan {
	t.Helper()
	repo := cliapp.RepoRoot()
	snapshot, err := yamlsource.LoadFile(filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := snapshot.Decode(&spec); err != nil {
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

func supportedRegistrationResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
