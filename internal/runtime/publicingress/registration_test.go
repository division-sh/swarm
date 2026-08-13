package publicingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

type registrationEffectStore struct {
	*effecttest.Harness
	mu      sync.Mutex
	current bool
}

func (s *registrationEffectStore) IsExternalEffectAuthorityCurrent(context.Context, runtimeeffects.Authority) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current, nil
}

type telegramRegistrationTransport struct {
	t             *testing.T
	mu            sync.Mutex
	applyCount    int
	identifyCount int
	currentURLs   map[string]string
	resourceIDs   map[string]int64
	readbackURL   string
	loseAck       bool
}

func (transport *telegramRegistrationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.t.Helper()
	transport.mu.Lock()
	defer transport.mu.Unlock()
	token := strings.SplitN(strings.TrimPrefix(request.URL.Path, "/bot"), "/", 2)[0]
	switch {
	case strings.HasSuffix(request.URL.Path, "/getMe"):
		transport.identifyCount++
		resourceID := int64(42)
		if configured := transport.resourceIDs[token]; configured > 0 {
			resourceID = configured
		}
		body, err := json.Marshal(map[string]any{"ok": true, "result": map[string]any{"id": resourceID}})
		if err != nil {
			transport.t.Fatal(err)
		}
		return registrationControllerResponse(http.StatusOK, string(body)), nil
	case strings.HasSuffix(request.URL.Path, "/setWebhook"):
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			transport.t.Fatalf("decode setWebhook: %v", err)
		}
		if transport.currentURLs == nil {
			transport.currentURLs = map[string]string{}
		}
		transport.currentURLs[token] = strings.TrimSpace(payload["url"].(string))
		transport.applyCount++
		if transport.loseAck {
			return nil, errors.New("injected post-launch acknowledgment loss")
		}
		return registrationControllerResponse(http.StatusOK, `{"ok":true,"result":true}`), nil
	case strings.HasSuffix(request.URL.Path, "/getWebhookInfo"):
		callbackURL := transport.currentURLs[token]
		if transport.readbackURL != "" {
			callbackURL = transport.readbackURL
		}
		body, err := json.Marshal(map[string]any{"ok": true, "result": map[string]any{"url": callbackURL}})
		if err != nil {
			transport.t.Fatal(err)
		}
		return registrationControllerResponse(http.StatusOK, string(body)), nil
	default:
		transport.t.Fatalf("unexpected provider request: %s", request.URL)
		return nil, nil
	}
}

func (transport *telegramRegistrationTransport) counts() (int, int) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.identifyCount, transport.applyCount
}

func TestProviderRegistrationReconcilerCollisionConvergenceAndNoResend(t *testing.T) {
	registration := loadTelegramRegistrationPlan(t)
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"bot": "token-v1", "bot-other": "token-other", "signing": "signing-v1"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}
	snapshotOwner, err := runtimecredentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		t.Fatalf("NewSnapshotOwner: %v", err)
	}
	effectsStore := &registrationEffectStore{Harness: effecttest.New(), current: true}
	transport := &telegramRegistrationTransport{t: t}
	readiness := NewReadinessOwner(true)
	startup := testStartupAuthority(t, "runtime-a")
	controller, err := NewProviderRegistrationController(RegistrationControllerOptions{
		CredentialOwner: snapshotOwner, EffectsStore: effectsStore,
		HTTP:    runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: transport}},
		Posture: executionposture.Live, RuntimeInstanceID: uuid.NewString(),
		StartupAuthority: func() (startupownership.Authority, error) { return startup, nil },
		Readiness:        readiness,
	})
	if err != nil {
		t.Fatalf("NewProviderRegistrationController: %v", err)
	}
	exposure := Generation{ID: uuid.NewString(), Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: time.Now().UTC()}
	readiness.SetRuntimeReady(true)
	readiness.SetExposure(ExposureEvidence{
		GenerationID: exposure.ID, StartupAuthorityID: startup.AuthorityID,
		ObservedAt: exposure.CreatedAt, ExpiresAt: exposure.CreatedAt.Add(EvidenceTTL),
	})
	pair := testRegistrationPair(t, registration, "hitl", "ingress:support:telegram:telegram")

	t.Run("duplicate slot rejects whole set before writes", func(t *testing.T) {
		other := testRegistrationPair(t, registration, "alerts", "ingress:alerts:telegram:telegram")
		other.CredentialKeys = map[string]string{"telegram_bot_token": "bot-other"}
		if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair, other}); err == nil || !strings.Contains(err.Error(), "selected by both") {
			t.Fatalf("collision error = %v", err)
		}
		identified, applied := transport.counts()
		if identified != 2 || applied != 0 {
			t.Fatalf("collision counts identify=%d apply=%d, want 2/0", identified, applied)
		}
	})

	t.Run("distinct provider resources reconcile independently", func(t *testing.T) {
		other := testRegistrationPair(t, registration, "alerts", "ingress:alerts:telegram:telegram")
		other.CredentialKeys = map[string]string{"telegram_bot_token": "bot-other"}
		distinctTransport := &telegramRegistrationTransport{t: t, resourceIDs: map[string]int64{"token-v1": 42, "token-other": 84}}
		distinctReadiness := NewReadinessOwner(true)
		distinctController, err := NewProviderRegistrationController(RegistrationControllerOptions{
			CredentialOwner: snapshotOwner, EffectsStore: &registrationEffectStore{Harness: effecttest.New(), current: true},
			HTTP:    runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: distinctTransport}},
			Posture: executionposture.Live, RuntimeInstanceID: uuid.NewString(),
			StartupAuthority: func() (startupownership.Authority, error) { return startup, nil },
			Readiness:        distinctReadiness,
		})
		if err != nil {
			t.Fatalf("NewProviderRegistrationController distinct resources: %v", err)
		}
		if err := distinctController.Reconcile(context.Background(), exposure, []RegistrationPair{pair, other}); err != nil {
			t.Fatalf("reconcile distinct provider resources: %v", err)
		}
		if identified, applied := distinctTransport.counts(); identified != 2 || applied != 2 {
			t.Fatalf("distinct resource counts identify=%d apply=%d, want 2/2", identified, applied)
		}
	})

	transport.loseAck = true
	transport.readbackURL = "https://hooks.example.test/stale"
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err == nil {
		t.Fatal("ack-loss mismatched readback returned nil")
	}
	_, applied := transport.counts()
	if applied != 1 {
		t.Fatalf("initial apply count = %d, want 1", applied)
	}
	transport.readbackURL = ""
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
		t.Fatalf("ack-loss exact readback reconcile: %v", err)
	}
	_, applied = transport.counts()
	if applied != 1 {
		t.Fatalf("ack-loss reconciliation resent provider apply: count=%d", applied)
	}
	first := readiness.Snapshot(time.Now().UTC())
	if len(first.Registrations) != 1 || !first.Registrations[0].Applied || !first.Registrations[0].CallbackMatched {
		t.Fatalf("converged readiness = %#v", first)
	}
	firstCallback := first.Registrations[0].CallbackURL

	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
		t.Fatalf("same-intent readback reconcile: %v", err)
	}
	_, applied = transport.counts()
	if applied != 1 {
		t.Fatalf("same intent resent provider apply: count=%d", applied)
	}

	startup = testStartupAuthority(t, "runtime-b")
	readiness.SetExposure(ExposureEvidence{
		GenerationID: exposure.ID, StartupAuthorityID: startup.AuthorityID,
		ObservedAt: exposure.CreatedAt, ExpiresAt: exposure.CreatedAt.Add(EvidenceTTL),
	})
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
		t.Fatalf("unchanged startup handoff reconcile: %v", err)
	}
	_, applied = transport.counts()
	if applied != 1 {
		t.Fatalf("startup handoff changed semantic request identity: apply count=%d", applied)
	}

	transport.loseAck = false
	if err := credentialStore.Set(context.Background(), "signing", "signing-v2"); err != nil {
		t.Fatalf("rotate signing credential: %v", err)
	}
	if stale := readiness.Snapshot(time.Now().UTC()); stale.Ready || stale.PublicIngressReady || !strings.Contains(stale.Failure, "credential snapshots") {
		t.Fatalf("rotated credential read-time readiness = %#v, want revoked before replacement apply", stale)
	}
	staleCredentialRecorder := httptest.NewRecorder()
	controller.Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("stale credential intent reached public callback handler")
	})).ServeHTTP(staleCredentialRecorder, httptest.NewRequest(http.MethodPost, firstCallback, nil))
	if staleCredentialRecorder.Code != http.StatusNotFound {
		t.Fatalf("stale credential callback status=%d, want 404", staleCredentialRecorder.Code)
	}
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
		t.Fatalf("signing credential rotation reconcile: %v", err)
	}
	_, applied = transport.counts()
	if applied != 2 {
		t.Fatalf("credential rotation apply count=%d, want 2", applied)
	}
	rotated := readiness.Snapshot(time.Now().UTC())
	if rotated.Registrations[0].CallbackURL == firstCallback {
		t.Fatal("credential rotation reused callback token")
	}

	nextCalls := 0
	handler := controller.Handler(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		nextCalls++
		response.WriteHeader(http.StatusNoContent)
	}))
	staleRequest := httptest.NewRequest(http.MethodPost, firstCallback, bytes.NewReader(nil))
	staleRecorder := httptest.NewRecorder()
	handler.ServeHTTP(staleRecorder, staleRequest)
	if staleRecorder.Code != http.StatusNotFound || nextCalls != 0 {
		t.Fatalf("stale callback status=%d calls=%d, want 404/0", staleRecorder.Code, nextCalls)
	}
	unregisteredRecorder := httptest.NewRecorder()
	handler.ServeHTTP(unregisteredRecorder, httptest.NewRequest(http.MethodPost, "https://hooks.example.test/webhooks/unregistered/telegram", nil))
	if unregisteredRecorder.Code != http.StatusNotFound || nextCalls != 0 {
		t.Fatalf("unregistered public callback status=%d calls=%d, want 404/0", unregisteredRecorder.Code, nextCalls)
	}
	currentRequest := httptest.NewRequest(http.MethodPost, rotated.Registrations[0].CallbackURL, bytes.NewReader(nil))
	currentRecorder := httptest.NewRecorder()
	handler.ServeHTTP(currentRecorder, currentRequest)
	if currentRecorder.Code != http.StatusNoContent || nextCalls != 1 {
		t.Fatalf("current callback status=%d calls=%d, want 204/1", currentRecorder.Code, nextCalls)
	}

	if err := credentialStore.Set(context.Background(), "bot", "token-v2"); err != nil {
		t.Fatalf("rotate provider credential: %v", err)
	}
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
		t.Fatalf("provider credential rotation reconcile: %v", err)
	}
	_, applied = transport.counts()
	if applied != 3 {
		t.Fatalf("provider credential rotation apply count=%d, want 3", applied)
	}

	routeChanged := pair
	routeChanged.Target.Alias = "renamed-support"
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{routeChanged}); err != nil {
		t.Fatalf("callback route change reconcile: %v", err)
	}
	_, applied = transport.counts()
	if applied != 4 {
		t.Fatalf("callback route change apply count=%d, want 4", applied)
	}
	routed := readiness.Snapshot(time.Now().UTC())
	if len(routed.Registrations) != 1 || !strings.Contains(routed.Registrations[0].CallbackURL, "/webhooks/renamed-support/telegram?") {
		t.Fatalf("callback route change readiness = %#v", routed)
	}
	readiness.SetExposure(ExposureEvidence{
		GenerationID: exposure.ID, StartupAuthorityID: startup.AuthorityID,
		ObservedAt: routed.Registrations[0].ObservedAt, ExpiresAt: routed.Registrations[0].ExpiresAt.Add(time.Minute),
	})
	if expired := readiness.Snapshot(routed.Registrations[0].ExpiresAt); expired.Ready || expired.PublicIngressReady {
		t.Fatalf("expired Telegram registration remained ready: %#v", expired)
	}

	restartedReadiness := NewReadinessOwner(true)
	restartedController, err := NewProviderRegistrationController(RegistrationControllerOptions{
		CredentialOwner: snapshotOwner, EffectsStore: effectsStore,
		HTTP:    runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: transport}},
		Posture: executionposture.Live, RuntimeInstanceID: uuid.NewString(),
		StartupAuthority: func() (startupownership.Authority, error) { return startup, nil },
		Readiness:        restartedReadiness,
	})
	if err != nil {
		t.Fatalf("NewProviderRegistrationController restart: %v", err)
	}
	restartedExposure := exposure
	restartedExposure.ID = uuid.NewString()
	restartedExposure.PublicOrigin = "https://replacement.example.test"
	if err := restartedController.Reconcile(context.Background(), restartedExposure, []RegistrationPair{routeChanged}); err != nil {
		t.Fatalf("process restart registration reconcile: %v", err)
	}
	_, applied = transport.counts()
	if applied != 5 {
		t.Fatalf("process restart apply count=%d, want 5", applied)
	}
	restarted := restartedReadiness.Snapshot(time.Now().UTC())
	if len(restarted.Registrations) != 1 || !strings.HasPrefix(restarted.Registrations[0].CallbackURL, restartedExposure.PublicOrigin) || restarted.Registrations[0].CallbackURL == routed.Registrations[0].CallbackURL {
		t.Fatalf("process restart registration = %#v", restarted.Registrations)
	}
}

func loadTelegramRegistrationPlan(t *testing.T) packs.CompiledChannelRegistration {
	t.Helper()
	repo := filepath.Clean(filepath.Join("..", "..", ".."))
	version, err := platform.PlatformVersion()
	if err != nil {
		t.Fatalf("PlatformVersion: %v", err)
	}
	snapshot, err := yamlsource.LoadFile(filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	var spec contracts.PlatformSpecDocument
	if err := snapshot.Decode(&spec); err != nil {
		t.Fatalf("decode platform spec: %v", err)
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatalf("NewInterfaceRegistry: %v", err)
	}
	channels, err := packs.LoadChannelPackDirs(version, "platform", filepath.Join(repo, "packs", "channels", "telegram"))
	if err != nil || len(channels) != 1 {
		t.Fatalf("load Telegram channel: count=%d err=%v", len(channels), err)
	}
	triggers, _, err := providertriggers.NewCatalogSnapshotFromPackDirs(version, []string{filepath.Join(repo, "packs", "provider-triggers", "telegram")}, nil)
	if err != nil {
		t.Fatalf("load Telegram trigger: %v", err)
	}
	var connector packs.ConnectorPackDescriptor
	for _, candidate := range providerconnectors.DefaultPackRegistry().PackDescriptors() {
		if candidate.Identity.ID() == "provider.telegram.connector" {
			connector = candidate
			break
		}
	}
	plan, err := packs.CompileChannel(registry, channels[0], triggers.PackDescriptors(), []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatalf("CompileChannel: %v", err)
	}
	registration, ok := plan.Registration()
	if !ok {
		t.Fatal("Telegram registration plan is missing")
	}
	return registration
}

func testRegistrationPair(t *testing.T, registration packs.CompiledChannelRegistration, bindingID, selector string) RegistrationPair {
	t.Helper()
	parsed, err := ParseTargetSelector(selector)
	if err != nil {
		t.Fatal(err)
	}
	planGeneration, err := plangeneration.FromCanonicalValue(map[string]any{"binding": bindingID})
	if err != nil {
		t.Fatal(err)
	}
	return RegistrationPair{
		BindingID: bindingID, PlanGeneration: planGeneration, Registration: registration,
		CredentialKeys: map[string]string{"telegram_bot_token": "bot"},
		Target: RegistrationTarget{
			Selector: selector, BundleHash: "bundle-v1:sha256:" + strings.Repeat("b", 64), ServiceID: "service-" + bindingID,
			PackageKey: parsed.PackageKey, FlowID: parsed.FlowID, Alias: parsed.PackageKey, Provider: parsed.Provider,
			Generation: 1, PublicationSequence: 1, AdmissionPlanGeneration: triggergeneration.FromCanonicalBytes([]byte("admission-" + bindingID)), SigningCredentialKey: "signing",
		},
	}
}

func testStartupAuthority(t *testing.T, owner string) startupownership.Authority {
	t.Helper()
	authority, err := startupownership.NewColdAuthority(startupownership.AcquireRequest{
		OwnerID: owner, BootID: uuid.NewString(), BundleHash: "bundle-v1:sha256:" + strings.Repeat("d", 64),
	}, "test")
	if err != nil {
		t.Fatalf("NewColdAuthority: %v", err)
	}
	return authority
}

func registrationControllerResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
