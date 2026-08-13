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

type launchMarkerFaultMode string

const (
	launchMarkerRollback launchMarkerFaultMode = "rollback"
	launchMarkerAckLoss  launchMarkerFaultMode = "commit_then_error"
)

type retryingRegistrationEffectStore struct {
	mu             sync.Mutex
	mode           launchMarkerFaultMode
	faultPending   bool
	current        bool
	byOperation    map[string]runtimeeffects.Attempt
	states         map[string]runtimeeffects.State
	operationOrder []string
}

func newRetryingRegistrationEffectStore(mode launchMarkerFaultMode) *retryingRegistrationEffectStore {
	return &retryingRegistrationEffectStore{
		mode: mode, faultPending: true, current: true,
		byOperation: map[string]runtimeeffects.Attempt{}, states: map[string]runtimeeffects.State{},
	}
}

func (s *retryingRegistrationEffectStore) IsExternalEffectAuthorityCurrent(context.Context, runtimeeffects.Authority) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current, nil
}

func (s *retryingRegistrationEffectStore) AuthorizeExternalAttempt(_ context.Context, authority runtimeeffects.Authority, request runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, exists := s.byOperation[request.OperationID]; exists {
		state := s.states[prior.AttemptID]
		if state == runtimeeffects.StateAuthorized {
			return prior, nil
		}
		if state != runtimeeffects.StateTerminalFailure {
			return runtimeeffects.Attempt{}, errors.New("provider registration replay refused")
		}
		attemptID, err := runtimeeffects.AttemptID(request.OperationID, prior.Ordinal+1)
		if err != nil {
			return runtimeeffects.Attempt{}, err
		}
		attempt := runtimeeffects.Attempt{
			OperationID: request.OperationID, AttemptID: attemptID, Authority: authority,
			Kind: request.Kind, Class: request.Class, Adapter: request.Adapter, Transport: request.Transport,
			Ordinal: prior.Ordinal + 1, AuthorizedAt: request.Now,
		}
		s.byOperation[request.OperationID] = attempt
		s.states[attemptID] = runtimeeffects.StateAuthorized
		return attempt, nil
	}
	attempt := runtimeeffects.Attempt{
		OperationID: request.OperationID, AttemptID: request.AttemptID, Authority: authority,
		Kind: request.Kind, Class: request.Class, Adapter: request.Adapter, Transport: request.Transport,
		Ordinal: 1, AuthorizedAt: request.Now,
	}
	s.byOperation[request.OperationID] = attempt
	s.states[attempt.AttemptID] = runtimeeffects.StateAuthorized
	s.operationOrder = append(s.operationOrder, request.OperationID)
	return attempt, nil
}

func (s *retryingRegistrationEffectStore) MarkExternalAttemptLaunched(_ context.Context, attempt runtimeeffects.Attempt, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.faultPending {
		s.faultPending = false
		if s.mode == launchMarkerAckLoss {
			s.states[attempt.AttemptID] = runtimeeffects.StateLaunched
		}
		return errors.New("injected launch marker failure")
	}
	s.states[attempt.AttemptID] = runtimeeffects.StateLaunched
	return nil
}

func (s *retryingRegistrationEffectStore) MarkExternalAttemptResponseObserved(_ context.Context, attempt runtimeeffects.Attempt, _ map[string]any, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[attempt.AttemptID] = runtimeeffects.StateResponseObserved
	return nil
}

func (s *retryingRegistrationEffectStore) SettleExternalAttempt(_ context.Context, settlement runtimeeffects.Settlement) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[settlement.AttemptID] = settlement.State
	return nil
}

func (s *retryingRegistrationEffectStore) attempts() (string, int, runtimeeffects.State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.operationOrder) != 1 {
		return "", 0, ""
	}
	operationID := s.operationOrder[0]
	attempt := s.byOperation[operationID]
	return operationID, attempt.Ordinal, s.states[attempt.AttemptID]
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
	identifyErr   error
}

func (transport *telegramRegistrationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.t.Helper()
	transport.mu.Lock()
	defer transport.mu.Unlock()
	token := strings.SplitN(strings.TrimPrefix(request.URL.Path, "/bot"), "/", 2)[0]
	switch {
	case strings.HasSuffix(request.URL.Path, "/getMe"):
		transport.identifyCount++
		if transport.identifyErr != nil {
			return nil, transport.identifyErr
		}
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

	transport.mu.Lock()
	transport.identifyErr = errors.New("transient identify unavailable")
	transport.mu.Unlock()
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err == nil {
		t.Fatal("transient identify failure returned nil")
	}
	failedObservation := readiness.Snapshot(time.Now().UTC())
	if failedObservation.PublicIngressReady || len(failedObservation.Registrations) != 1 || failedObservation.Registrations[0].IntentID == "" {
		t.Fatalf("transient identify failure destroyed verified lifecycle state: %#v", failedObservation)
	}
	failedCallback := httptest.NewRecorder()
	controller.Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("observation-failed registration reached callback handler")
	})).ServeHTTP(failedCallback, httptest.NewRequest(http.MethodPost, firstCallback, nil))
	if failedCallback.Code != http.StatusNotFound {
		t.Fatalf("observation-failed callback status=%d, want 404", failedCallback.Code)
	}
	transport.mu.Lock()
	transport.identifyErr = nil
	transport.mu.Unlock()
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
		t.Fatalf("recover transient identify failure: %v", err)
	}
	_, applied = transport.counts()
	if applied != 1 {
		t.Fatalf("transient identify recovery resent provider apply: count=%d", applied)
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

func TestProviderRegistrationRetainsSettlementIdentityAndSharesCallbackCurrentness(t *testing.T) {
	registration := loadTelegramRegistrationPlan(t)
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"bot": "token-v1", "signing": "signing-v1"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}
	snapshotOwner, err := runtimecredentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		t.Fatalf("NewSnapshotOwner: %v", err)
	}
	effectsStore := &registrationEffectStore{Harness: effecttest.New(), current: true}
	effectsStore.SettleErr = errors.New("injected settlement acknowledgment loss")
	transport := &telegramRegistrationTransport{t: t}
	readiness := NewReadinessOwner(true)
	startup := testStartupAuthority(t, "runtime-a")
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	controller, err := NewProviderRegistrationController(RegistrationControllerOptions{
		CredentialOwner: snapshotOwner, EffectsStore: effectsStore,
		HTTP:    runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: transport}},
		Posture: executionposture.Live, RuntimeInstanceID: uuid.NewString(),
		StartupAuthority: func() (startupownership.Authority, error) { return startup, nil },
		Readiness:        readiness, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewProviderRegistrationController: %v", err)
	}
	exposure := Generation{ID: uuid.NewString(), Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: now}
	readiness.SetRuntimeReady(true)
	readiness.SetExposure(ExposureEvidence{
		GenerationID: exposure.ID, StartupAuthorityID: startup.AuthorityID,
		ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
	})
	pair := testRegistrationPair(t, registration, "hitl", "ingress:support:telegram:telegram")
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err == nil {
		t.Fatal("settlement acknowledgment loss returned nil")
	}
	pending := readiness.Snapshot(now)
	if pending.PublicIngressReady || len(pending.Registrations) != 1 || pending.Registrations[0].Phase != string(registrationPhasePendingSettlement) {
		t.Fatalf("pending settlement readiness = %#v", pending)
	}
	_, applied := transport.counts()
	if applied != 1 {
		t.Fatalf("pending settlement apply count=%d, want 1", applied)
	}
	effectsStore.SettleErr = nil
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
		t.Fatalf("settle original attempt by exact readback: %v", err)
	}
	settled := readiness.Snapshot(now)
	if !settled.PublicIngressReady || settled.Registrations[0].Phase != string(registrationPhaseVerified) {
		t.Fatalf("settled registration readiness = %#v", settled)
	}
	_, applied = transport.counts()
	if applied != 1 {
		t.Fatalf("settlement recovery resent provider apply: count=%d", applied)
	}

	handlerCalls := 0
	handler := controller.Handler(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		response.WriteHeader(http.StatusNoContent)
	}))
	callbackURL := settled.Registrations[0].CallbackURL
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, callbackURL, nil))
	if recorder.Code != http.StatusNoContent || handlerCalls != 1 {
		t.Fatalf("current callback status/calls=%d/%d, want 204/1", recorder.Code, handlerCalls)
	}
	now = now.Add(EvidenceTTL)
	expired := httptest.NewRecorder()
	handler.ServeHTTP(expired, httptest.NewRequest(http.MethodPost, callbackURL, nil))
	if expired.Code != http.StatusNotFound || handlerCalls != 1 || readiness.Snapshot(now).PublicIngressReady {
		t.Fatalf("expired callback status/calls=%d/%d snapshot=%#v", expired.Code, handlerCalls, readiness.Snapshot(now))
	}

	readiness.RevokeExposure("managed tunnel stopped")
	revoked := httptest.NewRecorder()
	handler.ServeHTTP(revoked, httptest.NewRequest(http.MethodPost, callbackURL, nil))
	if revoked.Code != http.StatusNotFound || handlerCalls != 1 {
		t.Fatalf("revoked callback status/calls=%d/%d, want 404/1", revoked.Code, handlerCalls)
	}
	if err := controller.Reconcile(context.Background(), exposure, nil); err != nil {
		t.Fatalf("remove selected registrations: %v", err)
	}
	if snapshot := readiness.Snapshot(now); len(snapshot.Registrations) != 0 || controller.snapshot.routeSelected(pair.Target.Alias, pair.Target.Provider) {
		t.Fatalf("removed registration survived atomic selection: %#v", snapshot)
	}
}

func TestProviderRegistrationPrelaunchMarkerFailureRetriesWithoutEarlyDispatch(t *testing.T) {
	registration := loadTelegramRegistrationPlan(t)
	for _, mode := range []launchMarkerFaultMode{launchMarkerRollback, launchMarkerAckLoss} {
		t.Run(string(mode), func(t *testing.T) {
			credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatalf("NewFileStore: %v", err)
			}
			for key, value := range map[string]string{"bot": "token-v1", "signing": "signing-v1"} {
				if err := credentialStore.Set(context.Background(), key, value); err != nil {
					t.Fatalf("Set(%s): %v", key, err)
				}
			}
			credentials, err := runtimecredentials.NewSnapshotOwner(credentialStore)
			if err != nil {
				t.Fatalf("NewSnapshotOwner: %v", err)
			}
			effectsStore := newRetryingRegistrationEffectStore(mode)
			transport := &telegramRegistrationTransport{t: t}
			readiness := NewReadinessOwner(true)
			startup := testStartupAuthority(t, "runtime-a")
			controller, err := NewProviderRegistrationController(RegistrationControllerOptions{
				CredentialOwner: credentials, EffectsStore: effectsStore,
				HTTP:    runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: transport}},
				Posture: executionposture.Live, RuntimeInstanceID: uuid.NewString(),
				StartupAuthority: func() (startupownership.Authority, error) { return startup, nil },
				Readiness:        readiness,
			})
			if err != nil {
				t.Fatalf("NewProviderRegistrationController: %v", err)
			}
			now := time.Now().UTC()
			exposure := Generation{ID: uuid.NewString(), Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: now}
			readiness.SetRuntimeReady(true)
			readiness.SetExposure(ExposureEvidence{GenerationID: exposure.ID, StartupAuthorityID: startup.AuthorityID, ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
			pair := testRegistrationPair(t, registration, "hitl", "ingress:support:telegram:telegram")

			if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err == nil {
				t.Fatal("launch marker failure returned nil")
			}
			if _, applied := transport.counts(); applied != 0 {
				t.Fatalf("provider dispatched before durable launch marker: count=%d", applied)
			}
			firstOperation, ordinal, state := effectsStore.attempts()
			if firstOperation == "" || ordinal != 1 || state != runtimeeffects.StateTerminalFailure {
				t.Fatalf("prelaunch lifecycle operation=%q ordinal=%d state=%q", firstOperation, ordinal, state)
			}
			if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
				t.Fatalf("retry proven prelaunch failure: %v", err)
			}
			operation, ordinal, state := effectsStore.attempts()
			if operation != firstOperation || ordinal != 2 || state != runtimeeffects.StateSettled {
				t.Fatalf("retry lifecycle operation=%q ordinal=%d state=%q, want same/2/settled", operation, ordinal, state)
			}
			if _, applied := transport.counts(); applied != 1 {
				t.Fatalf("provider retry dispatch count=%d, want 1", applied)
			}
		})
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
