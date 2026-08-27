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

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
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
	readbackErr   error
	loseAck       bool
	identifyErr   error
}

type failingSigningSnapshotStore struct {
	runtimecredentials.Store
	err error
}

func (s failingSigningSnapshotStore) Snapshot(ctx context.Context, key string) (runtimecredentials.AtomicSnapshot, error) {
	if key == "signing" {
		return runtimecredentials.AtomicSnapshot{}, s.err
	}
	return s.Store.(runtimecredentials.Snapshotter).Snapshot(ctx, key)
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
		if transport.readbackErr != nil {
			return nil, transport.readbackErr
		}
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

func TestProviderRegistrationRejectsUnusableSigningBeforeAnySideEffect(t *testing.T) {
	registration := loadTelegramRegistrationPlan(t)
	for _, tc := range []struct {
		name       string
		value      string
		set        bool
		unreadable bool
	}{
		{name: "missing"},
		{name: "empty", set: true},
		{name: "whitespace", value: " \t\n", set: true},
		{name: "unreadable", unreadable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Set(ctx, "bot", "provider-token"); err != nil {
				t.Fatal(err)
			}
			if tc.set {
				if err := store.Set(ctx, "signing", tc.value); err != nil {
					t.Fatal(err)
				}
			}
			var credentialStore runtimecredentials.Store = store
			if tc.unreadable {
				credentialStore = failingSigningSnapshotStore{Store: store, err: errors.New("signing store unavailable")}
			}
			owner, err := runtimecredentials.NewSnapshotOwner(credentialStore)
			if err != nil {
				t.Fatal(err)
			}
			effectsStore := &registrationEffectStore{Harness: effecttest.New(), current: true}
			transport := &telegramRegistrationTransport{t: t}
			readiness := NewReadinessOwner(true)
			startup := testStartupAuthority(t, "runtime-negative")
			controller, err := NewProviderRegistrationController(RegistrationControllerOptions{
				CredentialOwner: owner, EffectsStore: effectsStore,
				HTTP:    runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: transport}},
				Posture: executionposture.Live, RuntimeInstanceID: uuid.NewString(),
				StartupAuthority: func() (startupownership.GrantEvidence, error) { return startup, nil }, Readiness: readiness,
			})
			if err != nil {
				t.Fatal(err)
			}
			exposure := Generation{ID: uuid.NewString(), Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: time.Now().UTC()}
			readiness.SetRuntimeReady(true)
			readiness.SetExposure(ExposureEvidence{GenerationID: exposure.ID, StartupAuthorityID: startup.GrantID, ObservedAt: exposure.CreatedAt, ExpiresAt: exposure.CreatedAt.Add(EvidenceTTL)})
			pair := testRegistrationPair(t, registration, "negative", "ingress:support:telegram:telegram")
			if err := controller.Reconcile(ctx, exposure, []RegistrationPair{pair}); err == nil {
				t.Fatal("Reconcile succeeded with unusable target signing credential")
			}
			identified, applied := transport.counts()
			if identified != 0 || applied != 0 {
				t.Fatalf("provider calls identify=%d apply=%d, want 0/0", identified, applied)
			}
			if _, launched := effectsStore.StateForAdapter("provider_registration"); launched {
				t.Fatal("unusable signing credential launched an external effect")
			}
			if snapshot := readiness.Snapshot(time.Now().UTC()); snapshot.PublicIngressReady {
				t.Fatalf("unusable signing credential published readiness: %#v", snapshot)
			}
		})
	}
}

func TestProviderRegistrationRejectedReplacementPreservesVerifiedPredecessor(t *testing.T) {
	ctx := context.Background()
	registration := loadTelegramRegistrationPlan(t)
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{"bot": "predecessor-token", "replacement-bot": "invalid-token", "signing": "signing-secret"} {
		if err := credentialStore.Set(ctx, key, value); err != nil {
			t.Fatal(err)
		}
	}
	owner, err := runtimecredentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	transport := &telegramRegistrationTransport{t: t}
	readiness := NewReadinessOwner(true)
	startup := testStartupAuthority(t, "runtime-replacement")
	controller, err := NewProviderRegistrationController(RegistrationControllerOptions{
		CredentialOwner: owner, EffectsStore: &registrationEffectStore{Harness: effecttest.New(), current: true},
		HTTP: runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: transport}}, Posture: executionposture.Live,
		RuntimeInstanceID: uuid.NewString(), StartupAuthority: func() (startupownership.GrantEvidence, error) { return startup, nil }, Readiness: readiness,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	exposure := Generation{ID: uuid.NewString(), Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: now}
	readiness.SetRuntimeReady(true)
	readiness.SetExposure(ExposureEvidence{GenerationID: exposure.ID, StartupAuthorityID: startup.GrantID, ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
	predecessor := testRegistrationPair(t, registration, "replacement", "ingress:support:telegram:telegram")
	if err := controller.Reconcile(ctx, exposure, []RegistrationPair{predecessor}); err != nil {
		t.Fatalf("reconcile predecessor: %v", err)
	}
	before := readiness.Snapshot(time.Now().UTC())
	if !before.PublicIngressReady || len(before.Registrations) != 1 {
		t.Fatalf("predecessor readiness = %#v", before)
	}
	callbackURL := before.Registrations[0].CallbackURL
	callbackToken := strings.TrimSpace(strings.Split(callbackURL, "swarm_callback_generation=")[1])
	transport.identifyErr = errors.New("telegram rejected replacement token")
	replacement := predecessor
	replacement.PrebindingOperationID = "replacement-operation"
	replacement.OnboardingOperationID = replacement.PrebindingOperationID
	replacement.CredentialKeys = map[string]string{"telegram_bot_token": "replacement-bot"}
	if err := controller.Reconcile(ctx, exposure, []RegistrationPair{replacement}); err == nil || !strings.Contains(err.Error(), "rejected replacement token") {
		t.Fatalf("replacement error = %v", err)
	}
	after := readiness.Snapshot(time.Now().UTC())
	if !after.PublicIngressReady || len(after.Registrations) != 1 || after.Registrations[0].CallbackURL != callbackURL {
		t.Fatalf("rejected replacement changed predecessor readiness: before=%#v after=%#v", before, after)
	}
	if !controller.CallbackCurrent(ctx, predecessor.Target.Alias, predecessor.Target.Provider, callbackToken) {
		t.Fatal("rejected replacement revoked the predecessor callback")
	}
}

func TestProviderRegistrationHandoffKeepsAuthoritiesDistinctUntilPromotion(t *testing.T) {
	registration := loadTelegramRegistrationPlan(t)
	candidate := testRegistrationPair(t, registration, "replacement", "ingress:support:telegram:telegram")
	publication, err := channelonboarding.NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	predecessor := candidate
	predecessor.PrebindingOperationID = ""
	predecessor.ActivationSource = channelonboarding.ActivationSourceLearned
	predecessor.OnboardingOperationID = candidate.PrebindingOperationID
	predecessor.ChannelActivationGeneration = publication.Generation()

	for _, test := range []struct {
		name    string
		pairs   []admittedPair
		want    int
		wantPre bool
		wantErr bool
	}{
		{
			name: "same slot preserves activation authority",
			pairs: []admittedPair{
				{pair: candidate, slotID: "telegram:bot_webhook:42"},
				{pair: predecessor, slotID: "telegram:bot_webhook:42"},
			},
			want: 1,
		},
		{
			name: "same slot ordering is irrelevant",
			pairs: []admittedPair{
				{pair: predecessor, slotID: "telegram:bot_webhook:42"},
				{pair: candidate, slotID: "telegram:bot_webhook:42"},
			},
			want: 1,
		},
		{
			name: "distinct provider slots coexist",
			pairs: []admittedPair{
				{pair: predecessor, slotID: "telegram:bot_webhook:42"},
				{pair: candidate, slotID: "telegram:bot_webhook:84"},
			},
			want: 2, wantPre: true,
		},
		{
			name: "same provider slot with different semantic pair fails closed",
			pairs: []admittedPair{
				{pair: predecessor, slotID: "telegram:bot_webhook:42"},
				{pair: func() RegistrationPair { changed := candidate; changed.BindingID = "other"; return changed }(), slotID: "telegram:bot_webhook:42"},
			},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			selected, err := resolveSlotCollisions(test.pairs)
			if (err != nil) != test.wantErr {
				t.Fatalf("resolve error = %v, want error %v", err, test.wantErr)
			}
			if test.wantErr {
				return
			}
			if len(selected) != test.want {
				t.Fatalf("selected pairs = %d, want %d", len(selected), test.want)
			}
			prebindings := 0
			for _, pair := range selected {
				if pair.pair.PrebindingOperationID != "" {
					prebindings++
				}
			}
			if test.wantPre && prebindings != 1 {
				t.Fatalf("selected prebindings = %d, want 1", prebindings)
			}
			if !test.wantPre && prebindings != 0 {
				t.Fatalf("same-slot candidate replaced predecessor: %#v", selected)
			}
		})
	}
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
		StartupAuthority: func() (startupownership.GrantEvidence, error) { return startup, nil },
		Readiness:        readiness,
	})
	if err != nil {
		t.Fatalf("NewProviderRegistrationController: %v", err)
	}
	exposure := Generation{ID: uuid.NewString(), Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: time.Now().UTC()}
	readiness.SetRuntimeReady(true)
	readiness.SetExposure(ExposureEvidence{
		GenerationID: exposure.ID, StartupAuthorityID: startup.GrantID,
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
			StartupAuthority: func() (startupownership.GrantEvidence, error) { return startup, nil },
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
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
		t.Fatalf("ack-loss exact readback reconcile: %v", err)
	}
	_, applied := transport.counts()
	if applied != 1 {
		t.Fatalf("initial apply count = %d, want 1", applied)
	}
	first := readiness.Snapshot(time.Now().UTC())
	if len(first.Registrations) != 1 || !first.Registrations[0].Applied || !first.Registrations[0].CallbackMatched {
		t.Fatalf("converged readiness = %#v", first)
	}
	firstCallback := first.Registrations[0].CallbackURL
	publication, err := channelonboarding.NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	promoted := pair
	promoted.PrebindingOperationID = ""
	promoted.ActivationSource = channelonboarding.ActivationSourceLearned
	promoted.ChannelActivationGeneration = publication.Generation()
	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{promoted}); err != nil {
		t.Fatalf("promote prebinding registration authority: %v", err)
	}
	if _, applied := transport.counts(); applied != 1 {
		t.Fatalf("same-operation registration promotion resent provider apply: count=%d", applied)
	}
	promotedReadiness := readiness.Snapshot(time.Now().UTC())
	if len(promotedReadiness.Registrations) != 1 || promotedReadiness.Registrations[0].ChannelActivationGeneration != promoted.ChannelActivationGeneration.Diagnostic() {
		t.Fatalf("promoted registration readiness = %#v", promotedReadiness)
	}
	pair = promoted

	if err := controller.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
		t.Fatalf("same-intent readback reconcile: %v", err)
	}
	_, applied = transport.counts()
	if applied != 1 {
		t.Fatalf("same intent resent provider apply: count=%d", applied)
	}

	t.Run("mismatched post-launch readback terminalizes without same-base resend", func(t *testing.T) {
		mismatchTransport := &telegramRegistrationTransport{t: t, loseAck: true, readbackURL: "https://hooks.example.test/stale"}
		mismatchReadiness := NewReadinessOwner(true)
		mismatchReadiness.SetRuntimeReady(true)
		mismatchReadiness.SetExposure(ExposureEvidence{
			GenerationID: exposure.ID, StartupAuthorityID: startup.GrantID,
			ObservedAt: exposure.CreatedAt, ExpiresAt: exposure.CreatedAt.Add(EvidenceTTL),
		})
		mismatchController, err := NewProviderRegistrationController(RegistrationControllerOptions{
			CredentialOwner: snapshotOwner, EffectsStore: &registrationEffectStore{Harness: effecttest.New(), current: true},
			HTTP:    runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: mismatchTransport}},
			Posture: executionposture.Live, RuntimeInstanceID: uuid.NewString(),
			StartupAuthority: func() (startupownership.GrantEvidence, error) { return startup, nil },
			Readiness:        mismatchReadiness,
		})
		if err != nil {
			t.Fatalf("NewProviderRegistrationController mismatch: %v", err)
		}
		if err := mismatchController.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err == nil {
			t.Fatal("mismatched readback returned nil")
		}
		uncertain := mismatchReadiness.Snapshot(time.Now().UTC())
		if uncertain.PublicIngressReady || len(uncertain.Registrations) != 1 || uncertain.Registrations[0].Phase != string(registrationPhaseOutcomeUncertain) {
			t.Fatalf("mismatched readback state = %#v", uncertain)
		}
		mismatchTransport.mu.Lock()
		mismatchTransport.readbackURL = ""
		mismatchTransport.mu.Unlock()
		if err := mismatchController.Reconcile(context.Background(), exposure, []RegistrationPair{pair}); err != nil {
			t.Fatalf("same-base uncertain reconcile: %v", err)
		}
		if _, applied := mismatchTransport.counts(); applied != 1 {
			t.Fatalf("same-base uncertain registration resent apply: count=%d", applied)
		}
	})

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
		GenerationID: exposure.ID, StartupAuthorityID: startup.GrantID,
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
		GenerationID: exposure.ID, StartupAuthorityID: startup.GrantID,
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
		StartupAuthority: func() (startupownership.GrantEvidence, error) { return startup, nil },
		Readiness:        restartedReadiness,
	})
	if err != nil {
		t.Fatalf("NewProviderRegistrationController restart: %v", err)
	}
	restartedExposure := exposure
	restartedExposure.ID = uuid.NewString()
	restartedExposure.PublicOrigin = "https://replacement.example.test"
	restartedReadiness.SetRuntimeReady(true)
	restartedReadiness.SetExposure(ExposureEvidence{
		GenerationID: restartedExposure.ID, StartupAuthorityID: startup.GrantID,
		ObservedAt: restartedExposure.CreatedAt, ExpiresAt: restartedExposure.CreatedAt.Add(EvidenceTTL),
	})
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

	identifiedBeforeDelete, appliedBeforeDelete := transport.counts()
	if err := credentialStore.Delete(context.Background(), "signing"); err != nil {
		t.Fatalf("delete signing credential: %v", err)
	}
	if stale := restartedReadiness.Snapshot(time.Now().UTC()); stale.Ready || stale.PublicIngressReady || !strings.Contains(stale.Failure, "credential snapshots") {
		t.Fatalf("deleted signing credential read-time readiness = %#v, want revoked", stale)
	}
	if err := restartedController.Reconcile(context.Background(), restartedExposure, []RegistrationPair{routeChanged}); err == nil || !strings.Contains(err.Error(), "signing credential") {
		t.Fatalf("deleted signing credential reconcile error = %v", err)
	}
	identifiedAfterDelete, appliedAfterDelete := transport.counts()
	if identifiedAfterDelete != identifiedBeforeDelete || appliedAfterDelete != appliedBeforeDelete {
		t.Fatalf("deleted signing credential reached provider: identify/apply %d/%d -> %d/%d", identifiedBeforeDelete, appliedBeforeDelete, identifiedAfterDelete, appliedAfterDelete)
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
		StartupAuthority: func() (startupownership.GrantEvidence, error) { return startup, nil },
		Readiness:        readiness, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewProviderRegistrationController: %v", err)
	}
	exposure := Generation{ID: uuid.NewString(), Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: now}
	readiness.SetRuntimeReady(true)
	readiness.SetExposure(ExposureEvidence{
		GenerationID: exposure.ID, StartupAuthorityID: startup.GrantID,
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

func TestProviderRegistrationStartupHandoffPhaseBarrier(t *testing.T) {
	registration := loadTelegramRegistrationPlan(t)
	type fixture struct {
		controller *ProviderRegistrationController
		readiness  *ReadinessOwner
		transport  *telegramRegistrationTransport
		pair       RegistrationPair
		exposure   Generation
	}
	newFixture := func(t *testing.T, effects runtimeeffects.Store) fixture {
		t.Helper()
		credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
		if err != nil {
			t.Fatalf("NewFileStore: %v", err)
		}
		for key, value := range map[string]string{"bot": "token-v1", "signing": "signing-v1"} {
			if err := credentialStore.Set(context.Background(), key, value); err != nil {
				t.Fatalf("Set(%s): %v", key, err)
			}
		}
		snapshots, err := runtimecredentials.NewSnapshotOwner(credentialStore)
		if err != nil {
			t.Fatalf("NewSnapshotOwner: %v", err)
		}
		startup := testStartupAuthority(t, "handoff-predecessor")
		now := time.Date(2026, 8, 13, 13, 0, 0, 0, time.UTC)
		readiness := NewReadinessOwner(true)
		transport := &telegramRegistrationTransport{t: t}
		controller, err := NewProviderRegistrationController(RegistrationControllerOptions{
			CredentialOwner: snapshots, EffectsStore: effects,
			HTTP:    runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: transport}},
			Posture: executionposture.Live, RuntimeInstanceID: uuid.NewString(),
			StartupAuthority: func() (startupownership.GrantEvidence, error) { return startup, nil },
			Readiness:        readiness, Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("NewProviderRegistrationController: %v", err)
		}
		exposure := Generation{ID: uuid.NewString(), Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: now}
		readiness.SetRuntimeReady(true)
		readiness.SetExposure(ExposureEvidence{
			GenerationID: exposure.ID, Mode: exposure.Mode, PublicOrigin: exposure.PublicOrigin, ListenAddress: exposure.ListenAddress,
			StartupAuthorityID: startup.GrantID, ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
		})
		return fixture{controller: controller, readiness: readiness, transport: transport, pair: testRegistrationPair(t, registration, "hitl", "ingress:support:telegram:telegram"), exposure: exposure}
	}

	tests := []struct {
		name      string
		prepare   func(*testing.T) fixture
		wantPhase registrationPhase
		wantErr   bool
	}{
		{
			name: "no attempt carries no effect",
			prepare: func(t *testing.T) fixture {
				return newFixture(t, &registrationEffectStore{Harness: effecttest.New(), current: true})
			},
			wantPhase: registrationPhaseNoAttempt,
		},
		{
			name: "verified carries only settled evidence",
			prepare: func(t *testing.T) fixture {
				store := &registrationEffectStore{Harness: effecttest.New(), current: true}
				f := newFixture(t, store)
				if err := f.controller.Reconcile(context.Background(), f.exposure, []RegistrationPair{f.pair}); err != nil {
					t.Fatalf("verified reconcile: %v", err)
				}
				return f
			},
			wantPhase: registrationPhaseVerified,
		},
		{
			name: "pending exact settles before handoff",
			prepare: func(t *testing.T) fixture {
				store := &registrationEffectStore{Harness: effecttest.New(), current: true}
				store.SettleErr = errors.New("injected settlement persistence failure")
				f := newFixture(t, store)
				if err := f.controller.Reconcile(context.Background(), f.exposure, []RegistrationPair{f.pair}); err == nil {
					t.Fatal("initial settlement failure returned nil")
				}
				store.SettleErr = nil
				return f
			},
			wantPhase: registrationPhaseVerified,
		},
		{
			name: "pending mismatch terminalizes before handoff",
			prepare: func(t *testing.T) fixture {
				store := &registrationEffectStore{Harness: effecttest.New(), current: true}
				store.SettleErr = errors.New("injected settlement persistence failure")
				f := newFixture(t, store)
				if err := f.controller.Reconcile(context.Background(), f.exposure, []RegistrationPair{f.pair}); err == nil {
					t.Fatal("initial settlement failure returned nil")
				}
				store.SettleErr = nil
				f.transport.mu.Lock()
				f.transport.readbackURL = "https://hooks.example.test/stale"
				f.transport.mu.Unlock()
				return f
			},
			wantPhase: registrationPhaseOutcomeUncertain,
		},
		{
			name: "pending unavailable terminalizes before handoff",
			prepare: func(t *testing.T) fixture {
				store := &registrationEffectStore{Harness: effecttest.New(), current: true}
				store.SettleErr = errors.New("injected settlement persistence failure")
				f := newFixture(t, store)
				if err := f.controller.Reconcile(context.Background(), f.exposure, []RegistrationPair{f.pair}); err == nil {
					t.Fatal("initial settlement failure returned nil")
				}
				store.SettleErr = nil
				f.transport.mu.Lock()
				f.transport.readbackErr = errors.New("provider readback unavailable")
				f.transport.mu.Unlock()
				return f
			},
			wantPhase: registrationPhaseOutcomeUncertain,
		},
		{
			name: "pending settlement persistence failure aborts handoff",
			prepare: func(t *testing.T) fixture {
				store := &registrationEffectStore{Harness: effecttest.New(), current: true}
				store.SettleErr = errors.New("injected settlement persistence failure")
				f := newFixture(t, store)
				if err := f.controller.Reconcile(context.Background(), f.exposure, []RegistrationPair{f.pair}); err == nil {
					t.Fatal("initial settlement failure returned nil")
				}
				return f
			},
			wantPhase: registrationPhasePendingSettlement,
			wantErr:   true,
		},
		{
			name: "prelaunch failure aborts handoff",
			prepare: func(t *testing.T) fixture {
				store := newRetryingRegistrationEffectStore(launchMarkerRollback)
				f := newFixture(t, store)
				if err := f.controller.Reconcile(context.Background(), f.exposure, []RegistrationPair{f.pair}); err == nil {
					t.Fatal("prelaunch failure returned nil")
				}
				return f
			},
			wantPhase: registrationPhasePrelaunch,
			wantErr:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.prepare(t)
			release, err := f.controller.PrepareStartupHandoff(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("PrepareStartupHandoff error = %v, wantErr=%v", err, tc.wantErr)
			}
			if err == nil {
				if f.controller.reconcileMu.TryLock() {
					f.controller.reconcileMu.Unlock()
					t.Fatal("registration renewal lock was not retained through startup handoff")
				}
				release()
				release()
				if !f.controller.reconcileMu.TryLock() {
					t.Fatal("registration renewal lock was not released after startup handoff")
				}
				f.controller.reconcileMu.Unlock()
			} else if release != nil {
				t.Fatal("failed startup handoff retained the registration renewal lock")
			}
			state, ok := f.controller.snapshot.state(pairKey(f.pair))
			if !ok && tc.wantPhase == registrationPhaseNoAttempt {
				return
			}
			if state.Phase != tc.wantPhase {
				t.Fatalf("phase=%q, want %q (state=%#v)", state.Phase, tc.wantPhase, state)
			}
			_, applied := f.transport.counts()
			if applied > 1 {
				t.Fatalf("handoff barrier resent provider apply: count=%d", applied)
			}
			if tc.wantPhase == registrationPhaseOutcomeUncertain {
				if state.Attempt != nil || state.Terminal == nil || state.Terminal.HasPending || state.Terminal.EffectOperationID == "" || state.Terminal.EffectAttemptID == "" || state.Terminal.EffectAttemptOrdinal != 1 {
					t.Fatalf("terminal outcome evidence = %#v", state)
				}
			}
		})
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
				StartupAuthority: func() (startupownership.GrantEvidence, error) { return startup, nil },
				Readiness:        readiness,
			})
			if err != nil {
				t.Fatalf("NewProviderRegistrationController: %v", err)
			}
			now := time.Now().UTC()
			exposure := Generation{ID: uuid.NewString(), Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: now}
			readiness.SetRuntimeReady(true)
			readiness.SetExposure(ExposureEvidence{GenerationID: exposure.ID, StartupAuthorityID: startup.GrantID, ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
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
		BindingID: bindingID, PlanGeneration: planGeneration, OnboardingOperationID: "test-prebinding-" + bindingID, PrebindingOperationID: "test-prebinding-" + bindingID, Registration: registration,
		CredentialKeys: map[string]string{"telegram_bot_token": "bot"},
		Target: RegistrationTarget{
			Selector: selector, BundleHash: "bundle-v1:sha256:" + strings.Repeat("b", 64), ServiceID: "service-" + bindingID,
			PackageKey: parsed.PackageKey, FlowID: parsed.FlowID, Alias: parsed.PackageKey, Provider: parsed.Provider,
			Generation: 1, PublicationSequence: 1, AdmissionPlanGeneration: triggergeneration.FromCanonicalBytes([]byte("admission-" + bindingID)), SigningCredentialKey: "signing",
		},
	}
}

func testStartupAuthority(t *testing.T, owner string) startupownership.GrantEvidence {
	t.Helper()
	authority := startupownership.GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: owner,
		ProcessBootID: uuid.NewString(), BundleHash: "bundle-v1:sha256:" + strings.Repeat("d", 64), BundleSource: "ephemeral",
		RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1, SourceSetRevision: "test-source-set",
		StateVersion: 3, State: startupownership.GrantAdmitted,
	}
	if err := authority.Validate(); err != nil {
		t.Fatalf("test startup grant: %v", err)
	}
	return authority
}

func registrationControllerResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
