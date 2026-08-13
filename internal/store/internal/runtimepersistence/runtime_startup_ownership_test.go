package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

func testStartupAcquireRequest(ownerID string) runtimestartupownership.AcquireRequest {
	return runtimestartupownership.AcquireRequest{OwnerID: ownerID, BootID: uuid.NewString(), BundleHash: testCanonicalBundleHash}
}

type startupAuthorityParityStore interface {
	runtimestartupownership.Store
	runtimestartupownership.Recorder
	runtimeeffects.Store
	runtimeeffects.RecoveryStore
}

type selectedRegistrationSettlementStore struct {
	startupAuthorityParityStore
	mu       sync.Mutex
	failNext bool
}

func (s *selectedRegistrationSettlementStore) SettleExternalAttempt(ctx context.Context, settlement runtimeeffects.Settlement) error {
	s.mu.Lock()
	if s.failNext {
		s.failNext = false
		s.mu.Unlock()
		return fmt.Errorf("injected provider-registration settlement persistence failure")
	}
	s.mu.Unlock()
	return s.startupAuthorityParityStore.SettleExternalAttempt(ctx, settlement)
}

func (s *selectedRegistrationSettlementStore) failNextSettlement() {
	s.mu.Lock()
	s.failNext = true
	s.mu.Unlock()
}

type selectedTelegramRegistrationTransport struct {
	mu          sync.Mutex
	applyCount  int
	currentURL  string
	readbackURL string
	readbackErr error
}

func (t *selectedTelegramRegistrationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case strings.HasSuffix(request.URL.Path, "/getMe"):
		return selectedRegistrationResponse(http.StatusOK, `{"ok":true,"result":{"id":42}}`), nil
	case strings.HasSuffix(request.URL.Path, "/setWebhook"):
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			return nil, err
		}
		t.currentURL = strings.TrimSpace(fmt.Sprint(body["url"]))
		t.applyCount++
		return selectedRegistrationResponse(http.StatusOK, `{"ok":true,"result":true}`), nil
	case strings.HasSuffix(request.URL.Path, "/getWebhookInfo"):
		if t.readbackErr != nil {
			return nil, t.readbackErr
		}
		callback := t.currentURL
		if t.readbackURL != "" {
			callback = t.readbackURL
		}
		raw, err := json.Marshal(map[string]any{"ok": true, "result": map[string]any{"url": callback}})
		if err != nil {
			return nil, err
		}
		return selectedRegistrationResponse(http.StatusOK, string(raw)), nil
	default:
		return nil, fmt.Errorf("unexpected Telegram registration request %s", request.URL)
	}
}

func (t *selectedTelegramRegistrationTransport) setReadback(url string, err error) {
	t.mu.Lock()
	t.readbackURL = strings.TrimSpace(url)
	t.readbackErr = err
	t.mu.Unlock()
}

func (t *selectedTelegramRegistrationTransport) applies() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.applyCount
}

func TestProviderRegistrationAuthorityAndApplyJournalParity(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (startupAuthorityParityStore, *sql.DB)
	}{
		{
			name: "postgres",
			store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
				_, db, _ := testutil.StartPostgres(t)
				return admitTestPostgresStore(t, db), db
			},
		},
		{
			name: "sqlite",
			store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				return store, store.backend.ConstructionHandle()
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			store, db := tc.store(t)
			lease, err := store.AcquireRuntimeStartupOwnership(ctx, testStartupAcquireRequest("registration-owner-a"))
			if err != nil {
				t.Fatalf("AcquireRuntimeStartupOwnership: %v", err)
			}
			t.Cleanup(func() { _ = lease.Release(testAuthorActivityContext()) })
			startup, err := lease.Authority()
			if err != nil {
				t.Fatalf("Authority: %v", err)
			}
			authority := testServeRegistrationAuthority(startup)
			if current, err := store.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || !current {
				t.Fatalf("initial registration authority current=%v err=%v", current, err)
			}
			registration, ok := runtimeeffects.RegistrationFor("provider_registration")
			if !ok {
				t.Fatal("provider_registration effect is not registered")
			}
			authorize := func(label string) (runtimeeffects.Attempt, error) {
				return store.AuthorizeExternalAttempt(ctx, authority, runtimeeffects.AuthorizeRequest{
					OperationID: uuid.NewString(), AttemptID: uuid.NewString(), Kind: registration.Kind, Class: registration.Class,
					Adapter: registration.Adapter, Transport: registration.Transport,
					RequestFingerprint: runtimeeffects.Fingerprint([]byte("provider-registration-" + label)),
					Lineage:            map[string]string{"binding_id": "hitl", "intent_id": authority.ID}, Now: time.Now().UTC(),
				})
			}

			t.Run("authorize rollback", func(t *testing.T) {
				operationID := uuid.NewString()
				attemptID := uuid.NewString()
				restore := installExternalEffectAttemptFault(t, db, tc.name == "postgres", "INSERT", attemptID, "")
				_, err := store.AuthorizeExternalAttempt(ctx, authority, runtimeeffects.AuthorizeRequest{
					OperationID: operationID, AttemptID: attemptID, Kind: registration.Kind, Class: registration.Class,
					Adapter: registration.Adapter, Transport: registration.Transport,
					RequestFingerprint: runtimeeffects.Fingerprint([]byte("provider-registration-authorize-rollback")),
					Lineage:            map[string]string{"binding_id": "hitl", "intent_id": authority.ID}, Now: time.Now().UTC(),
				})
				if err == nil {
					t.Fatal("authorize succeeded across injected attempt persistence failure")
				}
				restore()
				if got := selectedStoreExternalEffectCount(t, ctx, db, "runtime_external_effect_operations", "operation_id", operationID); got != 0 {
					t.Fatalf("authorize rollback operation rows=%d, want 0", got)
				}
				if got := selectedStoreExternalEffectCount(t, ctx, db, "runtime_external_effect_attempts", "attempt_id", attemptID); got != 0 {
					t.Fatalf("authorize rollback attempt rows=%d, want 0", got)
				}
			})

			t.Run("launch rollback", func(t *testing.T) {
				attempt, err := authorize("launch-rollback")
				if err != nil {
					t.Fatalf("authorize launch rollback: %v", err)
				}
				restore := installExternalEffectAttemptFault(t, db, tc.name == "postgres", "UPDATE", attempt.AttemptID, string(runtimeeffects.StateLaunched))
				if err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err == nil {
					t.Fatal("launch succeeded across injected state persistence failure")
				}
				restore()
				if got := selectedStoreAttemptState(t, ctx, db, attempt.AttemptID); got != string(runtimeeffects.StateAuthorized) {
					t.Fatalf("launch rollback state=%q, want authorized", got)
				}
			})

			for _, marker := range []struct {
				name      string
				committed bool
			}{
				{name: "launch marker rollback", committed: false},
				{name: "launch marker committed acknowledgment loss", committed: true},
			} {
				t.Run("provider registration prelaunch retry after "+marker.name, func(t *testing.T) {
					operationID := uuid.NewString()
					request := runtimeeffects.AuthorizeRequest{
						OperationID: operationID, AttemptID: uuid.NewString(), Kind: registration.Kind, Class: registration.Class,
						Adapter: registration.Adapter, Transport: registration.Transport,
						RequestFingerprint: runtimeeffects.Fingerprint([]byte("provider-registration-prelaunch-" + marker.name)),
						Lineage:            map[string]string{"binding_id": "hitl", "intent_id": authority.ID}, Now: time.Now().UTC(),
					}
					first, err := store.AuthorizeExternalAttempt(ctx, authority, request)
					if err != nil {
						t.Fatalf("authorize first prelaunch attempt: %v", err)
					}
					if marker.committed {
						if err := store.MarkExternalAttemptLaunched(ctx, first, time.Now().UTC()); err != nil {
							t.Fatalf("commit launch marker: %v", err)
						}
					}
					failureErr := runtimefailures.New(
						runtimefailures.ClassDependencyUnavailable,
						"provider_registration_prelaunch_rejected",
						"provider_registration",
						"dispatch",
						map[string]any{"launch_rejected": true},
					)
					failure, ok := runtimefailures.EnvelopeFromError(failureErr)
					if !ok {
						t.Fatal("prelaunch failure envelope is missing")
					}
					if err := store.SettleExternalAttempt(ctx, runtimeeffects.Settlement{
						OperationID: first.OperationID, AttemptID: first.AttemptID, Authority: authority,
						State: runtimeeffects.StateTerminalFailure, Failure: &failure,
						Evidence: map[string]any{"launch_rejected": true}, Now: time.Now().UTC(),
					}); err != nil {
						t.Fatalf("terminalize proven prelaunch attempt: %v", err)
					}
					request.AttemptID = uuid.NewString()
					retry, err := store.AuthorizeExternalAttempt(ctx, authority, request)
					if err != nil {
						t.Fatalf("authorize fresh provider-registration retry: %v", err)
					}
					if retry.OperationID != first.OperationID || retry.AttemptID == first.AttemptID || retry.Ordinal != first.Ordinal+1 {
						t.Fatalf("retry identity = %#v, first = %#v", retry, first)
					}
				})
			}

			t.Run("provider registration retry requires exact launch rejection proof", func(t *testing.T) {
				operationID := uuid.NewString()
				request := runtimeeffects.AuthorizeRequest{
					OperationID: operationID, AttemptID: uuid.NewString(), Kind: registration.Kind, Class: registration.Class,
					Adapter: registration.Adapter, Transport: registration.Transport,
					RequestFingerprint: runtimeeffects.Fingerprint([]byte("provider-registration-unproven-prelaunch")),
					Lineage:            map[string]string{"binding_id": "hitl", "intent_id": authority.ID}, Now: time.Now().UTC(),
				}
				first, err := store.AuthorizeExternalAttempt(ctx, authority, request)
				if err != nil {
					t.Fatalf("authorize unproven prelaunch attempt: %v", err)
				}
				failureErr := runtimefailures.New(
					runtimefailures.ClassDependencyUnavailable,
					"provider_registration_prelaunch_unproven",
					"provider_registration",
					"dispatch",
					nil,
				)
				failure, ok := runtimefailures.EnvelopeFromError(failureErr)
				if !ok || !failure.Retryable {
					t.Fatalf("unproven failure envelope = %#v, want retryable", failure)
				}
				if err := store.SettleExternalAttempt(ctx, runtimeeffects.Settlement{
					OperationID: first.OperationID, AttemptID: first.AttemptID, Authority: authority,
					State: runtimeeffects.StateTerminalFailure, Failure: &failure, Now: time.Now().UTC(),
				}); err != nil {
					t.Fatalf("terminalize unproven prelaunch attempt: %v", err)
				}
				request.AttemptID = uuid.NewString()
				if _, err := store.AuthorizeExternalAttempt(ctx, authority, request); err == nil {
					t.Fatal("provider registration admitted retry without launch_rejected=true")
				}
				if got := selectedStoreExternalEffectCount(t, ctx, db, "runtime_external_effect_attempts", "operation_id", operationID); got != 1 {
					t.Fatalf("unproven prelaunch attempt rows=%d, want 1", got)
				}
			})

			t.Run("provider registration authorization acknowledgment loss resumes same prelaunch attempt", func(t *testing.T) {
				operationID := uuid.NewString()
				request := runtimeeffects.AuthorizeRequest{
					OperationID: operationID, AttemptID: uuid.NewString(), Kind: registration.Kind, Class: registration.Class,
					Adapter: registration.Adapter, Transport: registration.Transport,
					RequestFingerprint: runtimeeffects.Fingerprint([]byte("provider-registration-authorization-ack-loss")),
					Lineage:            map[string]string{"binding_id": "hitl", "intent_id": authority.ID}, Now: time.Now().UTC(),
				}
				first, err := store.AuthorizeExternalAttempt(ctx, authority, request)
				if err != nil {
					t.Fatalf("authorize committed prelaunch attempt: %v", err)
				}
				request.AttemptID = uuid.NewString()
				resumed, err := store.AuthorizeExternalAttempt(ctx, authority, request)
				if err != nil {
					t.Fatalf("resume authorization after acknowledgment loss: %v", err)
				}
				if resumed.OperationID != first.OperationID || resumed.AttemptID != first.AttemptID || resumed.Ordinal != first.Ordinal {
					t.Fatalf("resumed identity = %#v, first = %#v", resumed, first)
				}
			})

			t.Run("settle rollback", func(t *testing.T) {
				attempt, err := authorize("settle-rollback")
				if err != nil {
					t.Fatalf("authorize settle rollback: %v", err)
				}
				if err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err != nil {
					t.Fatalf("launch settle rollback: %v", err)
				}
				restore := installExternalEffectAttemptFault(t, db, tc.name == "postgres", "UPDATE", attempt.AttemptID, string(runtimeeffects.StateSettled))
				err = store.SettleExternalAttempt(ctx, runtimeeffects.Settlement{
					OperationID: attempt.OperationID, AttemptID: attempt.AttemptID, Authority: authority,
					State: runtimeeffects.StateSettled, Evidence: map[string]any{"authority": "provider_readback"}, Now: time.Now().UTC(),
				})
				if err == nil {
					t.Fatal("settlement succeeded across injected state persistence failure")
				}
				restore()
				if got := selectedStoreAttemptState(t, ctx, db, attempt.AttemptID); got != string(runtimeeffects.StateLaunched) {
					t.Fatalf("settle rollback state=%q, want launched", got)
				}
				if err := store.SettleExternalAttempt(ctx, runtimeeffects.Settlement{
					OperationID: attempt.OperationID, AttemptID: attempt.AttemptID, Authority: authority,
					State: runtimeeffects.StateSettled, Evidence: map[string]any{"authority": "provider_readback"}, Now: time.Now().UTC(),
				}); err != nil {
					t.Fatalf("settle original attempt after acknowledgment recovery: %v", err)
				}
				if got := selectedStoreAttemptState(t, ctx, db, attempt.AttemptID); got != string(runtimeeffects.StateSettled) {
					t.Fatalf("settled original attempt state=%q, want settled", got)
				}
			})

			attempt, err := store.AuthorizeExternalAttempt(ctx, authority, runtimeeffects.AuthorizeRequest{
				OperationID: uuid.NewString(), AttemptID: uuid.NewString(), Kind: registration.Kind, Class: registration.Class,
				Adapter: registration.Adapter, Transport: registration.Transport,
				RequestFingerprint: runtimeeffects.Fingerprint([]byte("provider-registration-known")),
				Lineage:            map[string]string{"binding_id": "hitl", "intent_id": authority.ID}, Now: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("AuthorizeExternalAttempt: %v", err)
			}
			if err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err != nil {
				t.Fatalf("MarkExternalAttemptLaunched: %v", err)
			}
			if err := store.MarkExternalAttemptResponseObserved(ctx, attempt, map[string]any{"matched": true}, time.Now().UTC()); err != nil {
				t.Fatalf("MarkExternalAttemptResponseObserved: %v", err)
			}
			if err := store.SettleExternalAttempt(ctx, runtimeeffects.Settlement{
				OperationID: attempt.OperationID, AttemptID: attempt.AttemptID, Authority: authority,
				State: runtimeeffects.StateSettled, Evidence: map[string]any{"authority": "provider_readback"}, Now: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("SettleExternalAttempt: %v", err)
			}
			if got := selectedStoreAttemptState(t, ctx, db, attempt.AttemptID); got != string(runtimeeffects.StateSettled) {
				t.Fatalf("known attempt state=%q, want settled", got)
			}

			successorBundleHash := "bundle-v1:sha256:" + strings.Repeat("e", 64)
			handoff, err := lease.PrepareHandoff(ctx, runtimestartupownership.HandoffRequest{
				CandidateOwnerID: "registration-owner-b", CandidateBootID: uuid.NewString(),
				CandidateBundleHash: successorBundleHash,
			})
			if err != nil {
				t.Fatalf("PrepareHandoff: %v", err)
			}
			if _, err := handoff.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatalf("MarkProbesSettled: %v", err)
			}
			if _, err := handoff.Commit(ctx); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			successor, err := handoff.Finalize(ctx)
			if err != nil {
				t.Fatalf("Finalize: %v", err)
			}
			if current, err := store.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
				t.Fatalf("predecessor registration authority current=%v err=%v, want false", current, err)
			}
			successorCtx := testAuthorActivityContextForBundle(successorBundleHash)
			successorAuthority := testServeRegistrationAuthority(successor)
			uncertain, err := store.AuthorizeExternalAttempt(successorCtx, successorAuthority, runtimeeffects.AuthorizeRequest{
				OperationID: uuid.NewString(), AttemptID: uuid.NewString(), Kind: registration.Kind, Class: registration.Class,
				Adapter: registration.Adapter, Transport: registration.Transport,
				RequestFingerprint: runtimeeffects.Fingerprint([]byte("provider-registration-ack-loss")),
				Lineage:            map[string]string{"binding_id": "hitl", "intent_id": successorAuthority.ID}, Now: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("authorize successor apply: %v", err)
			}
			if err := store.MarkExternalAttemptLaunched(successorCtx, uncertain, time.Now().UTC().Add(-time.Hour)); err != nil {
				t.Fatalf("launch successor apply: %v", err)
			}
			if _, err := store.ReconcileExternalEffectAttempts(successorCtx, runtimeeffects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live)); err != nil {
				t.Fatalf("ReconcileExternalEffectAttempts: %v", err)
			}
			if got := selectedStoreAttemptState(t, ctx, db, uncertain.AttemptID); got != string(runtimeeffects.StateOutcomeUncertain) {
				t.Fatalf("ack-loss attempt state=%q, want outcome_uncertain", got)
			}
		})
	}
}

func TestProviderRegistrationControllerStartupHandoffParity(t *testing.T) {
	registrationPlan := selectedStoreTelegramRegistrationPlan(t)
	tests := []struct {
		name  string
		store func(*testing.T) (startupAuthorityParityStore, *sql.DB)
	}{
		{
			name: "postgres",
			store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
				_, db, _ := testutil.StartPostgres(t)
				return admitTestPostgresStore(t, db), db
			},
		},
		{
			name: "sqlite",
			store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				return store, store.backend.ConstructionHandle()
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected, db := tc.store(t)
			lease, err := selected.AcquireRuntimeStartupOwnership(ctx, testStartupAcquireRequest("registration-controller-predecessor"))
			if err != nil {
				t.Fatalf("AcquireRuntimeStartupOwnership: %v", err)
			}
			t.Cleanup(func() { _ = lease.Release(testAuthorActivityContext()) })
			startup, err := lease.Authority()
			if err != nil {
				t.Fatalf("predecessor authority: %v", err)
			}

			credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatalf("NewFileStore: %v", err)
			}
			for key, value := range map[string]string{"bot": "selected-token-v1", "signing": "selected-signing-v1"} {
				if err := credentialStore.Set(ctx, key, value); err != nil {
					t.Fatalf("set credential %s: %v", key, err)
				}
			}
			credentials, err := runtimecredentials.NewSnapshotOwner(credentialStore)
			if err != nil {
				t.Fatalf("NewSnapshotOwner: %v", err)
			}
			faulting := &selectedRegistrationSettlementStore{startupAuthorityParityStore: selected, failNext: true}
			transport := &selectedTelegramRegistrationTransport{}
			readiness := runtimepublicingress.NewReadinessOwner(true)
			now := time.Date(2026, 8, 13, 14, 0, 0, 0, time.UTC)
			controller, err := runtimepublicingress.NewProviderRegistrationController(runtimepublicingress.RegistrationControllerOptions{
				CredentialOwner: credentials, EffectsStore: faulting,
				HTTP:    runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: transport}},
				Posture: executionposture.Live, RuntimeInstanceID: uuid.NewString(),
				StartupAuthority: func() (runtimestartupownership.Authority, error) { return startup, nil },
				Readiness:        readiness, Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatalf("NewProviderRegistrationController: %v", err)
			}
			exposure := runtimepublicingress.Generation{
				ID: uuid.NewString(), Mode: runtimepublicingress.ModeExternalOrigin,
				PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: now,
			}
			setSelectedStoreExposure(readiness, exposure, startup, now)
			readiness.SetRuntimeReady(true)
			pair := selectedStoreRegistrationPair(t, registrationPlan)

			if err := controller.Reconcile(ctx, exposure, []runtimepublicingress.RegistrationPair{pair}); err == nil {
				t.Fatal("initial settlement persistence failure returned nil")
			}
			operationID, attemptID, ordinal := selectedStoreLatestProviderRegistrationAttempt(t, ctx, db)
			if operationID == "" || attemptID == "" || ordinal != 1 || transport.applies() != 1 {
				t.Fatalf("pending identity/apply = %q/%q/%d/%d", operationID, attemptID, ordinal, transport.applies())
			}
			if got := selectedStoreAttemptState(t, ctx, db, attemptID); got != string(runtimeeffects.StateResponseObserved) {
				t.Fatalf("pending durable state=%q, want response_observed", got)
			}
			pending := readiness.Snapshot(now)
			if pending.PublicIngressReady || len(pending.Registrations) != 1 || pending.Registrations[0].Phase != "pending_settlement" {
				t.Fatalf("pending process state = %#v", pending)
			}

			faulting.failNextSettlement()
			if _, err := controller.PrepareStartupHandoff(ctx); err == nil {
				t.Fatal("handoff barrier accepted an uncommitted settlement")
			}
			predecessorAuthority := testServeRegistrationAuthorityFromIdentity(startup, pending.Registrations[0].IntentID)
			if current, err := selected.IsExternalEffectAuthorityCurrent(ctx, predecessorAuthority); err != nil || !current {
				t.Fatalf("failed barrier predecessor authority current=%v err=%v", current, err)
			}
			if got := selectedStoreAttemptState(t, ctx, db, attemptID); got != string(runtimeeffects.StateResponseObserved) || transport.applies() != 1 {
				t.Fatalf("failed barrier state/apply=%q/%d", got, transport.applies())
			}

			releaseHandoff, err := controller.PrepareStartupHandoff(ctx)
			if err != nil {
				t.Fatalf("PrepareStartupHandoff exact settlement: %v", err)
			}
			defer releaseHandoff()
			if got := selectedStoreAttemptState(t, ctx, db, attemptID); got != string(runtimeeffects.StateSettled) {
				t.Fatalf("pre-handoff durable state=%q, want settled", got)
			}
			if transport.applies() != 1 {
				t.Fatalf("pre-handoff barrier resent apply: %d", transport.applies())
			}

			handoff, err := lease.PrepareHandoff(ctx, runtimestartupownership.HandoffRequest{
				CandidateOwnerID: "registration-controller-successor", CandidateBootID: uuid.NewString(), CandidateBundleHash: testCanonicalBundleHash,
			})
			if err != nil {
				t.Fatalf("PrepareHandoff: %v", err)
			}
			if _, err := handoff.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatalf("MarkProbesSettled: %v", err)
			}
			if _, err := handoff.Commit(ctx); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			startup, err = handoff.Finalize(ctx)
			if err != nil {
				t.Fatalf("Finalize: %v", err)
			}
			if current, err := selected.IsExternalEffectAuthorityCurrent(ctx, testServeRegistrationAuthorityFromIdentity(startup, pending.Registrations[0].IntentID)); err != nil || !current {
				t.Fatalf("successor registration authority current=%v err=%v", current, err)
			}
			setSelectedStoreExposure(readiness, exposure, startup, now)
			releaseHandoff()
			if err := controller.Reconcile(ctx, exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil {
				t.Fatalf("successor verified rebind: %v", err)
			}
			verified := readiness.Snapshot(now)
			if !verified.PublicIngressReady || verified.Registrations[0].Phase != "verified" || transport.applies() != 1 {
				t.Fatalf("successor verified state/apply = %#v/%d", verified, transport.applies())
			}
			if !controller.CallbackCurrent(ctx, pair.Target.Alias, pair.Target.Provider, selectedStoreCallbackToken(verified.Registrations[0].CallbackURL)) {
				t.Fatal("successor callback admission did not consume rebound currentness")
			}

			if err := credentialStore.Set(ctx, "signing", "selected-signing-v2"); err != nil {
				t.Fatalf("rotate signing credential for mismatch: %v", err)
			}
			now = now.Add(time.Second)
			transport.setReadback("https://hooks.example.test/stale", nil)
			if err := controller.Reconcile(ctx, exposure, []runtimepublicingress.RegistrationPair{pair}); err == nil {
				t.Fatal("mismatched readback returned nil")
			}
			_, mismatchAttempt, mismatchOrdinal := selectedStoreLatestProviderRegistrationAttempt(t, ctx, db)
			if mismatchAttempt == attemptID || mismatchOrdinal != 1 || selectedStoreAttemptState(t, ctx, db, mismatchAttempt) != string(runtimeeffects.StateOutcomeUncertain) {
				t.Fatalf("mismatch attempt identity/state = %q/%d/%s", mismatchAttempt, mismatchOrdinal, selectedStoreAttemptState(t, ctx, db, mismatchAttempt))
			}
			mismatch := readiness.Snapshot(now)
			if mismatch.PublicIngressReady || mismatch.Registrations[0].Phase != "outcome_uncertain" || transport.applies() != 2 {
				t.Fatalf("mismatch process state/apply = %#v/%d", mismatch, transport.applies())
			}
			transport.setReadback("", nil)
			if err := controller.Reconcile(ctx, exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil || transport.applies() != 2 {
				t.Fatalf("same-base mismatch reconcile/apply = %v/%d", err, transport.applies())
			}

			if err := credentialStore.Set(ctx, "signing", "selected-signing-v3"); err != nil {
				t.Fatalf("rotate signing credential for unavailable readback: %v", err)
			}
			now = now.Add(time.Second)
			transport.setReadback("", fmt.Errorf("provider readback unavailable"))
			if err := controller.Reconcile(ctx, exposure, []runtimepublicingress.RegistrationPair{pair}); err == nil {
				t.Fatal("unavailable readback returned nil")
			}
			_, unavailableAttempt, unavailableOrdinal := selectedStoreLatestProviderRegistrationAttempt(t, ctx, db)
			if unavailableAttempt == mismatchAttempt || unavailableOrdinal != 1 || selectedStoreAttemptState(t, ctx, db, unavailableAttempt) != string(runtimeeffects.StateOutcomeUncertain) {
				t.Fatalf("unavailable attempt identity/state = %q/%d/%s", unavailableAttempt, unavailableOrdinal, selectedStoreAttemptState(t, ctx, db, unavailableAttempt))
			}
			unavailable := readiness.Snapshot(now)
			if unavailable.PublicIngressReady || unavailable.Registrations[0].Phase != "outcome_uncertain" || transport.applies() != 3 {
				t.Fatalf("unavailable process state/apply = %#v/%d", unavailable, transport.applies())
			}
			if err := controller.Reconcile(ctx, exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil || transport.applies() != 3 {
				t.Fatalf("same-base unavailable reconcile/apply = %v/%d", err, transport.applies())
			}
		})
	}
}

func selectedStoreTelegramRegistrationPlan(t *testing.T) packs.CompiledChannelRegistration {
	t.Helper()
	repo := filepath.Clean(filepath.Join("..", "..", "..", ".."))
	version, err := platform.PlatformVersion()
	if err != nil {
		t.Fatalf("PlatformVersion: %v", err)
	}
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

func selectedStoreRegistrationPair(t *testing.T, registration packs.CompiledChannelRegistration) runtimepublicingress.RegistrationPair {
	t.Helper()
	planGeneration, err := plangeneration.FromCanonicalValue(map[string]any{"binding": "selected-store-telegram"})
	if err != nil {
		t.Fatalf("plan generation: %v", err)
	}
	return runtimepublicingress.RegistrationPair{
		BindingID: "selected-store-telegram", PlanGeneration: planGeneration, Registration: registration,
		CredentialKeys: map[string]string{"telegram_bot_token": "bot"},
		Target: runtimepublicingress.RegistrationTarget{
			Selector: "ingress:support:telegram:telegram", BundleHash: testCanonicalBundleHash,
			ServiceID: "selected-store-service", PackageKey: "support", FlowID: "telegram", Alias: "support", Provider: "telegram",
			Generation: 1, PublicationSequence: 1,
			AdmissionPlanGeneration: triggergeneration.FromCanonicalBytes([]byte("selected-store-registration-admission")),
			SigningCredentialKey:    "signing",
		},
	}
}

func setSelectedStoreExposure(readiness *runtimepublicingress.ReadinessOwner, exposure runtimepublicingress.Generation, startup runtimestartupownership.Authority, now time.Time) {
	readiness.SetExposure(runtimepublicingress.ExposureEvidence{
		GenerationID: exposure.ID, Mode: exposure.Mode, PublicOrigin: exposure.PublicOrigin, ListenAddress: exposure.ListenAddress,
		StartupAuthorityID: startup.AuthorityID, ObservedAt: now, ExpiresAt: now.Add(runtimepublicingress.EvidenceTTL),
	})
}

func selectedStoreLatestProviderRegistrationAttempt(t *testing.T, ctx context.Context, db *sql.DB) (string, string, int) {
	t.Helper()
	var operationID, attemptID string
	var ordinal int
	query := `SELECT operation_id::text, attempt_id::text, attempt_ordinal FROM runtime_external_effect_attempts WHERE adapter='provider_registration' ORDER BY authorized_at DESC, attempt_ordinal DESC LIMIT 1`
	if err := db.QueryRowContext(ctx, query).Scan(&operationID, &attemptID, &ordinal); err != nil {
		query = `SELECT operation_id, attempt_id, attempt_ordinal FROM runtime_external_effect_attempts WHERE adapter='provider_registration' ORDER BY authorized_at DESC, attempt_ordinal DESC LIMIT 1`
		if err := db.QueryRowContext(ctx, query).Scan(&operationID, &attemptID, &ordinal); err != nil {
			t.Fatalf("query latest provider registration attempt: %v", err)
		}
	}
	return operationID, attemptID, ordinal
}

func selectedStoreCallbackToken(callbackURL string) string {
	request, err := http.NewRequest(http.MethodPost, callbackURL, nil)
	if err != nil {
		return ""
	}
	return request.URL.Query().Get("swarm_callback_generation")
}

func selectedRegistrationResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func installExternalEffectAttemptFault(t *testing.T, db *sql.DB, postgres bool, operation, attemptID, state string) func() {
	t.Helper()
	name := "fail_effect_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	when := fmt.Sprintf("NEW.attempt_id = '%s'", attemptID)
	if state != "" {
		when += fmt.Sprintf(" AND NEW.state = '%s'", state)
	}
	if postgres {
		function := name + "_fn"
		if _, err := db.Exec(`CREATE FUNCTION ` + function + `() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'injected external effect persistence failure'; END; $$ LANGUAGE plpgsql`); err != nil {
			t.Fatalf("create postgres external effect fault function: %v", err)
		}
		if _, err := db.Exec(`CREATE TRIGGER ` + name + ` BEFORE ` + operation + ` ON runtime_external_effect_attempts FOR EACH ROW WHEN (` + when + `) EXECUTE FUNCTION ` + function + `()`); err != nil {
			_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + function + `()`)
			t.Fatalf("create postgres external effect fault trigger: %v", err)
		}
		return func() {
			_, _ = db.Exec(`DROP TRIGGER IF EXISTS ` + name + ` ON runtime_external_effect_attempts`)
			_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + function + `()`)
		}
	}
	if _, err := db.Exec(`CREATE TRIGGER ` + name + ` BEFORE ` + operation + ` ON runtime_external_effect_attempts WHEN ` + when + ` BEGIN SELECT RAISE(ABORT, 'injected external effect persistence failure'); END`); err != nil {
		t.Fatalf("create sqlite external effect fault trigger: %v", err)
	}
	return func() { _, _ = db.Exec(`DROP TRIGGER IF EXISTS ` + name) }
}

func selectedStoreExternalEffectCount(t *testing.T, ctx context.Context, db *sql.DB, table, column, identity string) int {
	t.Helper()
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s=$1::uuid", table, column)
	if err := db.QueryRowContext(ctx, query, identity).Scan(&count); err != nil {
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s=?", table, column)
		if err := db.QueryRowContext(ctx, query, identity).Scan(&count); err != nil {
			t.Fatalf("query %s count: %v", table, err)
		}
	}
	return count
}

func testServeRegistrationAuthority(startup runtimestartupownership.Authority) runtimeeffects.Authority {
	return testServeRegistrationAuthorityFromIdentity(startup, uuid.NewString())
}

func testServeRegistrationAuthorityFromIdentity(startup runtimestartupownership.Authority, intentID string) runtimeeffects.Authority {
	return runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityServeRegistration, ID: intentID,
		ExecutionOwner: startup.OwnerID, LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: startup.Generation,
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		ServeRegistration: runtimeeffects.ServeRegistrationAuthority{
			IntentID: intentID, StartupAuthorityID: startup.AuthorityID, StartupStateVersion: startup.StateVersion,
		},
	}
}

func selectedStoreAttemptState(t *testing.T, ctx context.Context, db *sql.DB, attemptID string) string {
	t.Helper()
	var state string
	if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid`, attemptID).Scan(&state); err != nil {
		if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_external_effect_attempts WHERE attempt_id=?`, attemptID).Scan(&state); err != nil {
			t.Fatalf("query external effect attempt state: %v", err)
		}
	}
	return state
}

func TestRuntimeStartupAuthorityTransitionsPersistWithBackendParity(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (startupAuthorityParityStore, *sql.DB)
	}{
		{
			name: "postgres",
			store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
				_, db, _ := testutil.StartPostgres(t)
				return admitTestPostgresStore(t, db), db
			},
		},
		{
			name: "sqlite",
			store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				return store, store.backend.ConstructionHandle()
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, db := tc.store(t)
			lease, err := store.AcquireRuntimeStartupOwnership(ctx, testStartupAcquireRequest("owner-a"))
			if err != nil {
				t.Fatalf("AcquireRuntimeStartupOwnership: %v", err)
			}
			t.Cleanup(func() { _ = lease.Release(context.Background()) })
			active, err := lease.Authority()
			if err != nil {
				t.Fatalf("Authority: %v", err)
			}
			probeAuthority := runtimeeffects.Authority{
				Kind: runtimeeffects.AuthorityStartupProbe, ID: uuid.NewString(), ExecutionOwner: active.OwnerID,
				LeaseExpiresAt: time.Now().UTC().Add(time.Hour), FenceGeneration: active.Generation,
				ExecutionMode: runtimeeffects.ExecutionModeLive,
				StartupProbe: runtimeeffects.StartupProbeAuthority{
					ProbeID: uuid.NewString(), StartupAuthorityID: active.AuthorityID, StartupStateVersion: active.StateVersion,
					ActorID: "agent-a", ExecutionKind: "normal_agent", ExecutionAuthorityID: active.AuthorityID,
				},
			}
			probeAuthority.ID = probeAuthority.StartupProbe.ProbeID
			probeCurrent := func() (bool, error) {
				switch store.(type) {
				case *PostgresStore:
					return externalEffectAuthorityCurrentPostgres(ctx, db, probeAuthority)
				case *SQLiteRuntimeStore:
					return externalEffectAuthorityCurrentSQLite(ctx, db, probeAuthority)
				default:
					return false, nil
				}
			}
			if current, err := probeCurrent(); err != nil || !current {
				t.Fatalf("initial startup probe authority current=%v err=%v, want true", current, err)
			}
			if _, err := lease.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatalf("MarkProbesSettled: %v", err)
			}
			if current, err := probeCurrent(); err != nil || current {
				t.Fatalf("superseded startup probe authority current=%v err=%v, want false", current, err)
			}
			if _, err := lease.AdmitExecution(ctx); err != nil {
				t.Fatalf("AdmitExecution: %v", err)
			}
			first, err := lease.PrepareHandoff(ctx, runtimestartupownership.HandoffRequest{
				CandidateOwnerID: "owner-b", CandidateBootID: uuid.NewString(),
				CandidateBundleHash: "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			})
			if err != nil {
				t.Fatalf("PrepareHandoff first: %v", err)
			}
			if _, err := first.MarkProbesSettled(ctx, []string{uuid.NewString()}); err != nil {
				t.Fatalf("first MarkProbesSettled: %v", err)
			}
			committed, err := first.Commit(ctx)
			if err != nil {
				t.Fatalf("first Commit: %v", err)
			}
			finalized, err := first.Finalize(ctx)
			if err != nil {
				t.Fatalf("first Finalize: %v", err)
			}
			if err := store.RecordRuntimeStartupAuthorityTransition(ctx, &committed, finalized); err == nil || !strings.Contains(err.Error(), "compare-and-set predecessor mismatch") {
				t.Fatalf("stale transition error = %v, want exact predecessor rejection", err)
			}
			second, err := lease.PrepareHandoff(ctx, runtimestartupownership.HandoffRequest{
				CandidateOwnerID: "owner-c", CandidateBootID: uuid.NewString(),
				CandidateBundleHash: "bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			})
			if err != nil {
				t.Fatalf("PrepareHandoff second: %v", err)
			}
			restored, err := second.Rollback(ctx)
			if err != nil {
				t.Fatalf("second Rollback: %v", err)
			}
			var count int
			var ordinal uint64
			var state string
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*),MAX(transition_ordinal) FROM runtime_startup_authority_facts WHERE lease_authority_id=$1`, restored.LeaseAuthorityID).Scan(&count, &ordinal); err != nil {
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*),MAX(transition_ordinal) FROM runtime_startup_authority_facts WHERE lease_authority_id=?`, restored.LeaseAuthorityID).Scan(&count, &ordinal); err != nil {
					t.Fatalf("query transition facts: %v", err)
				}
			}
			if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_startup_authority_facts WHERE lease_authority_id=$1 ORDER BY transition_ordinal DESC LIMIT 1`, restored.LeaseAuthorityID).Scan(&state); err != nil {
				if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_startup_authority_facts WHERE lease_authority_id=? ORDER BY transition_ordinal DESC LIMIT 1`, restored.LeaseAuthorityID).Scan(&state); err != nil {
					t.Fatalf("query transition head: %v", err)
				}
			}
			if count != 10 || ordinal != 10 || state != string(runtimestartupownership.StateActive) {
				t.Fatalf("transition facts count=%d ordinal=%d state=%s, want 10/10/active", count, ordinal, state)
			}
		})
	}
}

func TestPostgresStore_AcquireRuntimeStartupOwnership_DeniesCompetingOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	lease1, err := pg.AcquireRuntimeStartupOwnership(testAuthorActivityContext(), testStartupAcquireRequest("runtime-1"))
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-1): %v", err)
	}
	t.Cleanup(func() { _ = lease1.Release(testAuthorActivityContext()) })

	lease2, err := pg.AcquireRuntimeStartupOwnership(testAuthorActivityContext(), testStartupAcquireRequest("runtime-2"))
	if lease2 != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-2) lease = %#v, want nil", lease2)
	}
	if err == nil || !strings.Contains(err.Error(), "shared runtime store already owned by another runtime instance") {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-2) error = %v, want explicit ownership denial", err)
	}
}

func TestPostgresStore_AcquireRuntimeStartupOwnership_ReleaseAllowsSuccessor(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)

	lease1, err := pg.AcquireRuntimeStartupOwnership(testAuthorActivityContext(), testStartupAcquireRequest("runtime-1"))
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-1): %v", err)
	}
	if err := lease1.Release(testAuthorActivityContext()); err != nil {
		t.Fatalf("Release(runtime-1): %v", err)
	}

	lease2, err := pg.AcquireRuntimeStartupOwnership(testAuthorActivityContext(), testStartupAcquireRequest("runtime-2"))
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-2): %v", err)
	}
	if err := lease2.Release(testAuthorActivityContext()); err != nil {
		t.Fatalf("Release(runtime-2): %v", err)
	}
}
