package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
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

func TestProviderRegistrationRotationCannotRefreshRuntimeIngressAdmission(t *testing.T) {
	sourceRoot := filepath.Join(writeStandingTelegramServeFixture(t, "http://127.0.0.1:1"), "telegram-ingress")
	module, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), sourceRoot)
	if err != nil {
		t.Fatalf("load standing fixture: %v", err)
	}
	catalog := testProviderTriggerCatalog(t)
	source := processIngressTransportSource(t, bundle, catalog)
	persistence := &processIngressProofStore{}
	bundleHash := bundle.SourceArtifact.BundleHash()
	const runID = "41000000-0000-0000-0000-000000000001"
	eventsStore := newProcessIngressEventStore(t, source, runID)
	persistence.store = eventsStore
	workOwner := newSupervisorTestRuntimeOccurrence(t, bundleHash)
	bus, err := runtimebus.NewEphemeralEventBusWithOptions(eventsStore, runtimebus.EventBusOptions{
		ContractBundle:         source,
		Durable:                runtimebus.DurableDependencies{Instances: eventsStore, ConstructionPublications: eventsStore},
		SourceArtifactFact:     mustServeTestEphemeralSourceArtifactFact(bundleHash),
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
	declaredPublication, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication(nil)
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
	admission, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram", SigningSecret: "webhook_signing.telegram",
	})
	if err != nil {
		t.Fatalf("CompileAdmission: %v", err)
	}
	target := runtimepkg.StandingTarget{
		BundleHash: bundleHash, ServiceID: runtimeflowidentity.StandingServiceID("."), FlowPath: ".",
		Alias: "chat", Provider: "telegram", RunID: runID,
		Generation: 1, PublicationSequence: 1,
		SigningSecret: "webhook_signing.telegram", AdmissionPlan: admission,
	}
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatalf("InstalledCapabilitySubjects: %v", err)
	}
	rt := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: bus, InboundGateway: gateway, ChannelActivations: activationOwner,
		Options: runtimepkg.RuntimeOptions{RuntimeInstanceID: uuid.NewString(), ProviderCredentials: credentialStore, WorkflowModule: module, ProviderTriggerCatalog: catalog, DeclaredChannelPublication: declaredPublication,
			SourceArtifactFact: mustServeTestEphemeralSourceArtifactFact(bundleHash)}}
	if _, err := rt.PlanStandingTargets(); err != nil {
		t.Fatal(err)
	}
	gateway.SetCredentialAdmission(rt.AdmitInboundCredentials)
	manager, err := runtimepkg.NewRuntimeContextManager(nil, completeServeTestPackContext(t, runtimepkg.BundleContext{
		SourceArtifactFact: mustServeTestEphemeralSourceArtifactFact(bundleHash), Source: source,
		Runtime: rt, WorkOwner: workOwner,
		StandingTargets: []runtimepkg.StandingTarget{target}, ProviderTriggerGeneration: catalog.Generation(), InstalledTriggerSubjects: installed,
	}))
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	t.Cleanup(func() { _ = manager.QuiesceAllRuntimeContexts(context.Background()) })

	loadedContext := manager.LoadedContexts()[0]
	bundleHash = loadedContext.SourceArtifactFact.BundleHash()
	credentialSnapshots, err := runtimecredentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		t.Fatal(err)
	}

	registration := loadSupportedTelegramRegistration(t)
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
	assertIngressStatus(packs.StatusReady)
	effectsStore := &supportedRegistrationEffectStore{Harness: effecttest.New()}
	transport := &supportedTelegramRegistrationTransport{}
	readiness := runtimepublicingress.NewReadinessOwner(true)
	registrationNow := time.Now().UTC()
	startup := runtimestartupownership.GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "serve-owner",
		ProcessBootID: uuid.NewString(), BundleHash: bundleHash,
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
		SelectionCurrent: func(context.Context, runtimepublicingress.RegistrationPair, bool) (bool, error) { return true, nil },
		Readiness:        readiness,
		Now:              func() time.Time { return registrationNow },
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
		BundleHash:     bundleHash,
		BundleIdentity: "bundle:test@sha256:inbound-registration", PackInventoryGeneration: "sha256:inbound-registration-inventory",
		RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 1,
		PlanGeneration: planGeneration, TargetGeneration: uint64(target.Generation),
	}
	pair := runtimepublicingress.RegistrationPair{
		BindingID: "telegram", PlanGeneration: planGeneration, OnboardingOperationID: onboardingID, OnboardingRevision: 1,
		OnboardingCoordinate: coordinate, PrebindingOperationID: onboardingID, Registration: registration,
		CredentialKeys: map[string]string{"telegram_bot_token": "bot"},
		Target: runtimepublicingress.RegistrationTarget{
			Selector: "ingress:.:telegram", BundleHash: bundleHash, ServiceID: target.ServiceID,
			FlowPath: target.FlowPath, Alias: target.Alias, Provider: target.Provider,
			Generation: target.Generation, PublicationSequence: target.PublicationSequence,
			AdmissionPlanGeneration: target.AdmissionPlan.Generation(), SigningCredentialKey: target.SigningSecret,
		},
	}
	pair.CredentialKeys[registration.SigningCredential()] = target.SigningSecret
	for role, key := range pair.CredentialKeys {
		evidence, err := credentialSnapshots.SealCurrentValue(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		pair.CredentialAdmissions = append(pair.CredentialAdmissions, channelonboarding.CredentialAdmission{
			Role: role, StoreKey: key, Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: evidence.Seal,
		})
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
	handler := runtimeProcessInboundHandler{contexts: manager, publicRegistration: controller}
	claimReads := 0
	handler.pendingClaim = func(ctx context.Context, account channelonboarding.AccountReference, credential func(channelonboarding.CredentialAdmission) bool, admit func() (providertriggers.Delivery, error), current func() bool) (bool, error) {
		if account.Provider != "telegram" || account.Account != "42" || !current() {
			t.Fatal("claim bridge lost the admitted receiving account or current credentials")
		}
		providerAdmission := pair.CredentialAdmissions[0]
		for _, admission := range pair.CredentialAdmissions {
			if admission.StoreKey == "bot" {
				providerAdmission = admission
			}
		}
		if !credential(providerAdmission) {
			t.Fatal("receiving provider credential was lost")
		}
		delivery, err := admit()
		if err != nil {
			return false, err
		}
		claimReads++
		for _, event := range delivery.Events {
			if event.Kind == providertriggers.OutputKindNormalized && event.Payload["text"] == "SWARM-123456" {
				return true, nil
			}
		}
		return false, nil
	}
	if !controller.OwnsAccountCallback(t.Context(), initial.Registrations[0].CallbackURL, "42") ||
		controller.OwnsAccountCallback(t.Context(), initial.Registrations[0].CallbackURL, "43") ||
		controller.OwnsAccountCallback(t.Context(), strings.Replace(initial.Registrations[0].CallbackURL, "hooks.example.test", "foreign.example.test", 1), "42") {
		t.Fatal("holder accepted a different receiving account or callback")
	}
	claimBody := `{"update_id":98,"message":{"message_id":6,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"SWARM-123456"}}`
	for _, secret := range []string{"", "wrong-secret", "telegram-secret-v1"} {
		before := claimReads
		request := httptest.NewRequest(http.MethodPost, initial.Registrations[0].CallbackURL, strings.NewReader(claimBody))
		request.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if secret == "telegram-secret-v1" {
			if response.Code != http.StatusOK || claimReads != before+1 || len(eventsStore.events) != 0 {
				t.Fatalf("exact pending claim reached runtime publication: status=%d reads=%d", response.Code, claimReads)
			}
		} else if response.Code != http.StatusUnauthorized || claimReads != before || len(eventsStore.events) != 0 {
			t.Fatalf("callback generation replaced payload authentication: status=%d reads=%d", response.Code, claimReads)
		}
	}
	wrongMethod := httptest.NewRequest(http.MethodGet, initial.Registrations[0].CallbackURL, strings.NewReader(claimBody))
	wrongMethod.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret-v1")
	methodRefused := httptest.NewRecorder()
	handler.ServeHTTP(methodRefused, wrongMethod)
	if methodRefused.Code != http.StatusMethodNotAllowed || claimReads != 1 || len(eventsStore.events) != 0 {
		t.Fatal("pending claim accepted a non-POST callback", methodRefused.Code, claimReads)
	}
	// With no pending deployment claim, the runtime's original credential/target
	// refusal order remains authoritative and the callback body is not read.
	handler.pendingClaim = func(_ context.Context, _ channelonboarding.AccountReference, _ func(channelonboarding.CredentialAdmission) bool, _ func() (providertriggers.Delivery, error), _ func() bool) (bool, error) {
		return false, nil
	}
	use, lookup, err := manager.AcquireIngress(t.Context(), "chat", "telegram")
	if err != nil || use == nil {
		t.Fatalf("registration control did not acquire its context: %+v err=%v", lookup, err)
	}
	_, _, admissionErr := use.AdmitPublicIngressTarget(runtimepkg.InboundTarget{
		BundleHash: lookup.Target.BundleHash, ServiceID: lookup.Target.ServiceID, FlowPath: lookup.Target.FlowPath,
		RunID: lookup.Target.RunID, Generation: lookup.Target.Generation, PublicationSequence: lookup.Target.PublicationSequence,
		Alias: lookup.Target.Alias, Provider: lookup.Target.Provider, SigningSecret: lookup.Target.SigningSecret, AdmissionPlan: lookup.Target.AdmissionPlan,
	})
	if err := use.Done(); err != nil {
		t.Fatal(err)
	}
	if admissionErr != nil {
		t.Fatalf("registration control lacks exact acquired ownership: %v", admissionErr)
	}
	initialBody := `{"update_id":99,"message":{"message_id":7,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"initial"}}`
	for _, callback := range []string{"/webhooks/chat/telegram", "/webhooks/chat/telegram?swarm_callback_generation=foreign", "/webhooks/chat/TELEGRAM?swarm_callback_generation=foreign"} {
		request := httptest.NewRequest(http.MethodPost, callback, strings.NewReader(initialBody))
		request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret-v1")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound || len(eventsStore.events) != 0 {
			t.Fatalf("managed callback %s bypassed token fence: status=%d events=%d", callback, response.Code, len(eventsStore.events))
		}
	}
	initialRequest := httptest.NewRequest(http.MethodPost, initial.Registrations[0].CallbackURL, strings.NewReader(initialBody))
	initialRequest.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret-v1")
	initialRecorder := httptest.NewRecorder()
	handler.ServeHTTP(initialRecorder, initialRequest)
	if initialRecorder.Code != http.StatusAccepted || len(eventsStore.events) != 2 {
		t.Fatalf("initial registered verifier status/events=%d/%d commit_error=%v", initialRecorder.Code, len(eventsStore.events), persistence.lastError)
	}

	if err := credentialStore.Set(context.Background(), "webhook_signing.telegram", "telegram-secret-v2"); err != nil {
		t.Fatalf("rotate signing credential: %v", err)
	}
	assertIngressStatus(packs.StatusNotReady)
	staleCallback := httptest.NewRecorder()
	handler.ServeHTTP(staleCallback, httptest.NewRequest(http.MethodPost, initial.Registrations[0].CallbackURL, strings.NewReader(initialBody)))
	if staleCallback.Code != http.StatusNotFound {
		t.Fatalf("stale callback after rotation status=%d, want 404", staleCallback.Code)
	}
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err == nil {
		t.Fatal("unadmitted signing rotation authorized registration")
	}
	seal, err := credentialSnapshots.SealCurrentValue(context.Background(), pair.Target.SigningCredentialKey)
	if err != nil {
		t.Fatal(err)
	}
	for i := range pair.CredentialAdmissions {
		if pair.CredentialAdmissions[i].StoreKey == pair.Target.SigningCredentialKey {
			pair.CredentialAdmissions[i].ValueSeal = seal.Seal
		}
	}
	pair.OnboardingRevision++
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil {
		t.Fatal(err)
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
	if oldSecretRecorder.Code != http.StatusServiceUnavailable || len(eventsStore.events) != 2 {
		t.Fatalf("old secret with stale runtime admission status/events=%d/%d, want 503/2", oldSecretRecorder.Code, len(eventsStore.events))
	}
	newSecret := httptest.NewRequest(http.MethodPost, rotated.Registrations[0].CallbackURL, strings.NewReader(rotatedBody))
	newSecret.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret-v2")
	newSecretRecorder := httptest.NewRecorder()
	handler.ServeHTTP(newSecretRecorder, newSecret)
	if newSecretRecorder.Code != http.StatusServiceUnavailable || len(eventsStore.events) != 2 {
		t.Fatalf("provider registration alone refreshed runtime admission status/events=%d/%d, want 503/2", newSecretRecorder.Code, len(eventsStore.events))
	}

	// Invariant reach: callback authorization precedes every availability
	// response, including after the executable registration has disappeared.
	assertManagedFence := func(t *testing.T) {
		t.Helper()
		for _, callback := range []string{"/webhooks/chat/telegram", "/webhooks/chat/telegram?swarm_callback_generation=", "/webhooks/chat/telegram?swarm_callback_generation=foreign", initial.Registrations[0].CallbackURL} {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, callback, strings.NewReader(rotatedBody)))
			if response.Code != http.StatusNotFound || len(eventsStore.events) != 2 {
				t.Fatalf("managed availability disclosed before token fence: callback=%s status=%d events=%d", callback, response.Code, len(eventsStore.events))
			}
		}
		valid := httptest.NewRecorder()
		handler.ServeHTTP(valid, httptest.NewRequest(http.MethodPost, rotated.Registrations[0].CallbackURL, strings.NewReader(rotatedBody)))
		if valid.Code != http.StatusServiceUnavailable || len(eventsStore.events) != 2 {
			t.Fatalf("valid callback lost its independent runtime refusal: status=%d events=%d", valid.Code, len(eventsStore.events))
		}
		registrationNow = registrationNow.Add(runtimepublicingress.EvidenceTTL)
		expired := httptest.NewRecorder()
		handler.ServeHTTP(expired, httptest.NewRequest(http.MethodPost, rotated.Registrations[0].CallbackURL, strings.NewReader(rotatedBody)))
		registrationNow = registrationNow.Add(-runtimepublicingress.EvidenceTTL)
		if expired.Code != http.StatusNotFound || len(eventsStore.events) != 2 {
			t.Fatalf("expired callback reached availability: status=%d events=%d", expired.Code, len(eventsStore.events))
		}
	}
	t.Run("loaded_callback_fence", assertManagedFence)
	t.Run("suppressed_callback_fence", func(t *testing.T) {
		transition, err := manager.BeginStandingServiceTransition(t.Context(), target.ServiceID)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := transition.Restore(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		assertManagedFence(t)
	})
	t.Run("acquisition_failed_callback_fence", func(t *testing.T) {
		if err := workOwner.Fence(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := workOwner.WaitForQuiescence(ctx); err != nil {
				t.Error(err)
			}
			if err := workOwner.Reopen(); err != nil {
				t.Error(err)
			}
		}()
		assertManagedFence(t)
	})
	t.Run("unloaded_callback_fence", func(t *testing.T) {
		result := manager.DeactivateBundleHash(bundleHash, "managed callback availability control")
		if !result.Found || result.State != runtimepkg.RuntimeContextStateUnloaded || result.ShutdownErr != nil {
			t.Fatalf("unloaded callback precondition: %+v", result)
		}
		assertManagedFence(t)
	})
	readiness.RevokeExposure("managed callback revocation control")
	revoked := httptest.NewRecorder()
	handler.ServeHTTP(revoked, httptest.NewRequest(http.MethodPost, rotated.Registrations[0].CallbackURL, strings.NewReader(rotatedBody)))
	if revoked.Code != http.StatusNotFound || len(eventsStore.events) != 2 {
		t.Fatalf("revoked callback reached availability: status=%d events=%d", revoked.Code, len(eventsStore.events))
	}
	readiness.SetExposure(runtimepublicingress.ExposureEvidence{GenerationID: exposure.ID, StartupAuthorityID: startup.GrantID, ObservedAt: registrationNow, ExpiresAt: registrationNow.Add(runtimepublicingress.EvidenceTTL)})
	if err := controller.Reconcile(t.Context(), exposure, nil); err != nil {
		t.Fatal(err)
	}
	if controller.OwnsRoute("chat", "telegram") {
		t.Fatal("withdrawal control retained current registration ownership")
	}
	for _, callback := range []string{rotated.Registrations[0].CallbackURL, "/webhooks/chat/telegram?swarm_callback_generation=", "/webhooks/chat/telegram?swarm_callback_generation=%zz"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, callback, strings.NewReader(rotatedBody)))
		if response.Code != http.StatusNotFound || len(eventsStore.events) != 2 {
			t.Fatalf("withdrawn generation fell through to author admission: status=%d events=%d", response.Code, len(eventsStore.events))
		}
	}
}

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

func supportedRegistrationResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
