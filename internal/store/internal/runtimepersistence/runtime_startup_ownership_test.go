package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
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
	return runtimestartupownership.AcquireRequest{
		OwnerID: ownerID, BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	}
}

type startupAuthorityParityStore interface {
	runtimestartupownership.Store
	runtimeeffects.Store
	runtimeeffects.RecoveryStore
}

func admitRegistrationTestGeneration(
	t *testing.T,
	ctx context.Context,
	store startupAuthorityParityStore,
	ownerID string,
) (runtimestartupownership.ProcessCapability, runtimestartupownership.GenerationGrant, runtimestartupownership.GrantEvidence) {
	t.Helper()
	req := testStartupAcquireRequest(ownerID)
	capability, err := store.AcquireProcessCapability(ctx, req)
	if err != nil {
		t.Fatalf("AcquireProcessCapability: %v", err)
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: testCanonicalBundleHash, BundleSource: "ephemeral"}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatalf("build registration source set: %v", err)
	}
	if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatalf("install registration source set: %v", err)
	}
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: coordinate.BundleHash, BundleSource: coordinate.BundleSource,
		RuntimeInstanceID: req.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("issue registration generation grant: %v", err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatalf("settle registration generation probes: %v", err)
	}
	evidence, err := grant.AdmitExecution(ctx)
	if err != nil {
		t.Fatalf("admit registration generation: %v", err)
	}
	return capability, grant, evidence
}

func rotateRegistrationTestGeneration(
	t *testing.T,
	ctx context.Context,
	capability runtimestartupownership.ProcessCapability,
	current runtimestartupownership.GenerationGrant,
	previous runtimestartupownership.GrantEvidence,
) (runtimestartupownership.GenerationGrant, runtimestartupownership.GrantEvidence) {
	t.Helper()
	if err := current.Retire(ctx); err != nil {
		t.Fatalf("retire predecessor registration generation: %v", err)
	}
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: previous.BundleHash, BundleSource: previous.BundleSource,
		RuntimeInstanceID: previous.RuntimeInstanceID, RuntimeGeneration: previous.RuntimeGeneration + 1,
		SourceSetRevision: previous.SourceSetRevision,
	})
	if err != nil {
		t.Fatalf("issue successor registration generation: %v", err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatalf("settle successor registration probes: %v", err)
	}
	evidence, err := grant.AdmitExecution(ctx)
	if err != nil {
		t.Fatalf("admit successor registration generation: %v", err)
	}
	return grant, evidence
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
			capability, grant, startup := admitRegistrationTestGeneration(t, ctx, store, "registration-owner-a")
			t.Cleanup(func() { _ = capability.Release(context.Background()) })
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

			_, successor := rotateRegistrationTestGeneration(t, ctx, capability, grant, startup)
			if current, err := store.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
				t.Fatalf("predecessor registration authority current=%v err=%v, want false", current, err)
			}
			successorCtx := ctx
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
			capability, grant, startup := admitRegistrationTestGeneration(t, ctx, selected, "registration-controller-predecessor")
			t.Cleanup(func() { _ = capability.Release(context.Background()) })

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
				StartupAuthority: func() (runtimestartupownership.GrantEvidence, error) { return startup, nil },
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

			grant, startup = rotateRegistrationTestGeneration(t, ctx, capability, grant, startup)
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

func setSelectedStoreExposure(readiness *runtimepublicingress.ReadinessOwner, exposure runtimepublicingress.Generation, startup runtimestartupownership.GrantEvidence, now time.Time) {
	readiness.SetExposure(runtimepublicingress.ExposureEvidence{
		GenerationID: exposure.ID, Mode: exposure.Mode, PublicOrigin: exposure.PublicOrigin, ListenAddress: exposure.ListenAddress,
		StartupAuthorityID: startup.GrantID, ObservedAt: now, ExpiresAt: now.Add(runtimepublicingress.EvidenceTTL),
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

func testServeRegistrationAuthority(startup runtimestartupownership.GrantEvidence) runtimeeffects.Authority {
	return testServeRegistrationAuthorityFromIdentity(startup, uuid.NewString())
}

func testServeRegistrationAuthorityFromIdentity(startup runtimestartupownership.GrantEvidence, intentID string) runtimeeffects.Authority {
	return runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityServeRegistration, ID: intentID,
		ExecutionOwner: startup.ProcessOwnerID, LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: startup.RuntimeGeneration,
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		ServeRegistration: runtimeeffects.ServeRegistrationAuthority{
			IntentID: intentID, StartupAuthorityID: startup.GrantID, StartupStateVersion: startup.StateVersion,
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

func TestRuntimeProcessCapabilityTransitionsPersistWithBackendParity(t *testing.T) {
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
			selected, db := tc.store(t)
			req := testStartupAcquireRequest("owner-a")
			capability, err := selected.AcquireProcessCapability(ctx, req)
			if err != nil {
				t.Fatalf("AcquireProcessCapability: %v", err)
			}
			t.Cleanup(func() { _ = capability.Release(context.Background()) })
			authority, err := capability.Evidence()
			if err != nil || authority.State != runtimestartupownership.StateActive {
				t.Fatalf("active capability = %#v err=%v", authority, err)
			}
			coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: testCanonicalBundleHash, BundleSource: "ephemeral"}
			plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
				t.Fatalf("InstallCompleteSourceSet: %v", err)
			}
			grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
				BundleHash: coordinate.BundleHash, BundleSource: coordinate.BundleSource,
				RuntimeInstanceID: req.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
			})
			if err != nil {
				t.Fatalf("IssueGenerationGrant: %v", err)
			}
			if _, err := grant.MarkProbesSettled(ctx, []string{uuid.NewString()}); err != nil {
				t.Fatalf("MarkProbesSettled: %v", err)
			}
			if _, err := grant.AdmitExecution(ctx); err != nil {
				t.Fatalf("AdmitExecution: %v", err)
			}
			if err := capability.Release(ctx); err != nil {
				t.Fatalf("Release: %v", err)
			}
			select {
			case <-grant.Done():
			default:
				t.Fatal("process release did not retire generation grant")
			}

			placeholder := "?"
			if tc.name == "postgres" {
				placeholder = "$1::uuid"
			}
			var authorityFacts int
			var authorityState string
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*), MAX(state) FROM runtime_startup_authority_facts WHERE authority_id="+placeholder, authority.AuthorityID).Scan(&authorityFacts, &authorityState); err != nil {
				t.Fatalf("read process authority facts: %v", err)
			}
			if authorityFacts != 2 || authorityState != string(runtimestartupownership.StateReleased) {
				t.Fatalf("process authority facts=%d state=%q, want 2/released", authorityFacts, authorityState)
			}
			grantEvidence, err := grant.Evidence()
			if err == nil {
				t.Fatalf("retired grant still returned evidence: %#v", grantEvidence)
			}
			var grantFacts int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_generation_grants WHERE process_authority_id="+placeholder, authority.AuthorityID).Scan(&grantFacts); err != nil {
				t.Fatalf("read generation grant facts: %v", err)
			}
			if grantFacts != 4 {
				t.Fatalf("generation grant facts=%d, want prepared/probe_settled/admitted/retired", grantFacts)
			}
		})
	}
}

func TestPostgresProcessCapabilityReleaseAllowsCleanSuccessor(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	first, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("runtime-1"))
	if err != nil {
		t.Fatalf("AcquireProcessCapability(runtime-1): %v", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("Release(runtime-1): %v", err)
	}
	second, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("runtime-2"))
	if err != nil {
		t.Fatalf("AcquireProcessCapability(runtime-2): %v", err)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatalf("Release(runtime-2): %v", err)
	}
}

func TestRuntimeProcessCapabilityClosedSourceSetOperationsPersistWithBackendParity(t *testing.T) {
	const secondBundleHash = "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	tests := []struct {
		name  string
		store func(*testing.T) startupAuthorityParityStore
	}{
		{name: "postgres", store: func(t *testing.T) startupAuthorityParityStore {
			_, db, _ := testutil.StartPostgres(t)
			return admitTestPostgresStore(t, db)
		}},
		{name: "sqlite", store: func(t *testing.T) startupAuthorityParityStore {
			return newBootstrappedSQLiteRuntimeStoreForTest(t)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			selected := tc.store(t)
			capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("source-set-operations"))
			if err != nil {
				t.Fatalf("AcquireProcessCapability: %v", err)
			}
			t.Cleanup(func() { _ = capability.Release(context.Background()) })

			firstSource := runtimeagenttopology.SourceCoordinate{BundleHash: testCanonicalBundleHash, BundleSource: "ephemeral"}
			secondSource := runtimeagenttopology.SourceCoordinate{BundleHash: secondBundleHash, BundleSource: "persisted"}
			firstAgent := runtimeagenttopology.DesiredAgent{Identity: testAgentIdentity(t, "source-agent", ""), Source: firstSource, ConfigRevision: "config-v1"}
			secondAgent := runtimeagenttopology.DesiredAgent{Identity: testAgentIdentity(t, "second-agent", ""), Source: secondSource, ConfigRevision: "config-v1"}
			initial, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{firstSource}, []runtimeagenttopology.DesiredAgent{firstAgent})
			if err != nil {
				t.Fatal(err)
			}
			installed, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: initial})
			if err != nil || installed.Operation != runtimeagenttopology.OperationInstallCompleteSourceSet || !hasAgentTopologyChange(installed.Changes, runtimeagenttopology.AgentAdded) {
				t.Fatalf("install result=%#v err=%v", installed, err)
			}

			changedFirst := firstAgent
			changedFirst.ConfigRevision = "config-v2"
			replacement, err := runtimeagenttopology.NewSourceSetPlan(
				[]runtimeagenttopology.SourceCoordinate{firstSource, secondSource},
				[]runtimeagenttopology.DesiredAgent{changedFirst, secondAgent},
			)
			if err != nil {
				t.Fatal(err)
			}
			replaceReq := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: initial.Revision, Plan: replacement}
			replaced, err := capability.ReplaceSourceSet(ctx, replaceReq)
			if err != nil || replaced.Operation != runtimeagenttopology.OperationReplaceSourceSet || !hasAgentTopologyChange(replaced.Changes, runtimeagenttopology.AgentChanged) || !hasAgentTopologyChange(replaced.Changes, runtimeagenttopology.AgentAdded) {
				t.Fatalf("replace result=%#v err=%v", replaced, err)
			}
			replayed, err := capability.ReplaceSourceSet(ctx, replaceReq)
			if err != nil || !replayed.Replayed || replayed.CurrentRevision != replacement.Revision {
				t.Fatalf("replace replay result=%#v err=%v", replayed, err)
			}

			restored, err := capability.RestoreSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
				OperationID: uuid.NewString(), ExpectedRevision: replacement.Revision, Plan: initial,
			})
			if err != nil || restored.Operation != runtimeagenttopology.OperationRestoreSourceSet || !hasAgentTopologyChange(restored.Changes, runtimeagenttopology.AgentChanged) || !hasAgentTopologyChange(restored.Changes, runtimeagenttopology.AgentRemoved) {
				t.Fatalf("restore result=%#v err=%v", restored, err)
			}

			if _, err := capability.ReplaceSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
				OperationID: uuid.NewString(), ExpectedRevision: initial.Revision, Plan: replacement,
			}); err != nil {
				t.Fatalf("prepare remove source: %v", err)
			}
			firstRemoveID := uuid.NewString()
			removed, err := capability.RemoveBundleSource(ctx, runtimeagenttopology.SourceSetCommitRequest{
				OperationID: firstRemoveID, ExpectedRevision: replacement.Revision, Plan: initial, RemovedSource: &secondSource,
			})
			if err != nil || removed.Operation != runtimeagenttopology.OperationRemoveBundleSource || !hasAgentTopologyChange(removed.Changes, runtimeagenttopology.AgentChanged) || !hasAgentTopologyChange(removed.Changes, runtimeagenttopology.AgentRemoved) {
				t.Fatalf("remove result=%#v err=%v", removed, err)
			}
			if _, err := capability.ReplaceSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
				OperationID: uuid.NewString(), ExpectedRevision: initial.Revision, Plan: replacement,
			}); err != nil {
				t.Fatalf("prepare recurring source removal: %v", err)
			}
			secondRemoveID := uuid.NewString()
			if firstRemoveID == secondRemoveID {
				t.Fatal("recurring source removals shared an operation identity")
			}
			removedAgain, err := capability.RemoveBundleSource(ctx, runtimeagenttopology.SourceSetCommitRequest{
				OperationID: secondRemoveID, ExpectedRevision: replacement.Revision, Plan: initial, RemovedSource: &secondSource,
			})
			if err != nil || removedAgain.Replayed || removedAgain.CurrentRevision != initial.Revision || removedAgain.OperationID != secondRemoveID {
				t.Fatalf("recurring remove result=%#v err=%v", removedAgain, err)
			}
			currentAfterSecondRemove, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists || currentAfterSecondRemove.Revision != initial.Revision {
				t.Fatalf("current source set after recurring remove=%#v exists=%v err=%v", currentAfterSecondRemove, exists, err)
			}

			empty, err := runtimeagenttopology.EmptySourceSetPlan()
			if err != nil {
				t.Fatal(err)
			}
			reset, err := capability.ApplyDestructiveResetTopology(ctx, runtimeagenttopology.SourceSetCommitRequest{
				OperationID: uuid.NewString(), ExpectedRevision: initial.Revision, Plan: empty,
			})
			if err != nil || reset.Operation != runtimeagenttopology.OperationApplyDestructiveResetTopology || !hasAgentTopologyChange(reset.Changes, runtimeagenttopology.AgentRemoved) {
				t.Fatalf("reset result=%#v err=%v", reset, err)
			}
			current, exists, err := capability.CurrentSourceSet(ctx)
			if err != nil || !exists || current.Revision != empty.Revision {
				t.Fatalf("current source set=%#v exists=%v err=%v", current, exists, err)
			}
		})
	}
}

func TestPostgresProcessCapabilitySessionLossRetiresAndRejectsNonterminalSuccessor(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	req := testStartupAcquireRequest("lost-session-owner")
	capability, err := selected.AcquireProcessCapability(ctx, req)
	if err != nil {
		t.Fatalf("AcquireProcessCapability: %v", err)
	}
	authority, err := capability.Evidence()
	if err != nil {
		t.Fatalf("Evidence: %v", err)
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: testCanonicalBundleHash, BundleSource: "ephemeral"}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatalf("InstallCompleteSourceSet: %v", err)
	}
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: coordinate.BundleHash, BundleSource: coordinate.BundleSource,
		RuntimeInstanceID: req.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("IssueGenerationGrant: %v", err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatalf("MarkProbesSettled: %v", err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatalf("AdmitExecution: %v", err)
	}

	var retainedPID int
	if err := db.QueryRowContext(ctx, `
		SELECT pid FROM pg_locks
		WHERE locktype = 'advisory' AND granted
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
		ORDER BY pid LIMIT 1
	`).Scan(&retainedPID); err != nil {
		t.Fatalf("find retained process session: %v", err)
	}
	var terminated bool
	if err := db.QueryRowContext(ctx, `SELECT pg_terminate_backend($1)`, retainedPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate retained process session pid=%d terminated=%v err=%v", retainedPID, terminated, err)
	}
	if err := grant.ProveCurrent(ctx); err == nil {
		t.Fatal("lost retained session still proved current")
	}
	select {
	case <-capability.Done():
	default:
		t.Fatal("process capability was not terminal before loss returned")
	}
	select {
	case <-grant.Done():
	default:
		t.Fatal("generation grant was not terminal before loss returned")
	}

	successor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("successor"))
	if successor != nil {
		t.Fatalf("nonterminal predecessor admitted successor %#v", successor)
	}
	var acquisitionErr *runtimestartupownership.AcquisitionError
	if !errors.As(err, &acquisitionErr) || acquisitionErr.Failure != runtimestartupownership.AcquisitionTakeoverRequired {
		t.Fatalf("successor acquisition error=%v, want typed takeover_required", err)
	}
	var currentRevision string
	if err := db.QueryRowContext(ctx, `SELECT revision FROM agent_topology_source_set_head WHERE singleton_id = 1`).Scan(&currentRevision); err != nil {
		t.Fatalf("read source-set head after denied successor: %v", err)
	}
	if currentRevision != plan.Revision {
		t.Fatalf("source-set revision after denied successor=%q, want %q", currentRevision, plan.Revision)
	}
	var lifecycleRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents`).Scan(&lifecycleRows); err != nil || lifecycleRows != 0 {
		t.Fatalf("lifecycle rows after denied successor=%d err=%v, want zero", lifecycleRows, err)
	}
	var headState string
	if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_startup_authority_facts WHERE authority_id=$1::uuid ORDER BY transition_ordinal DESC LIMIT 1`, authority.AuthorityID).Scan(&headState); err != nil || headState != string(runtimestartupownership.StateActive) {
		t.Fatalf("durable predecessor state=%q err=%v, want active", headState, err)
	}
}

func TestPostgresProcessCapabilityRejectsAmbiguousPriorOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	first, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("prior-owner"))
	if err != nil {
		t.Fatalf("AcquireProcessCapability(prior): %v", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("Release(prior): %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE runtime_startup_authority_facts SET snapshot='{}'::jsonb WHERE transition_ordinal=(SELECT MAX(transition_ordinal) FROM runtime_startup_authority_facts)`); err != nil {
		t.Fatalf("corrupt prior durable head: %v", err)
	}
	capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("ambiguous-successor"))
	if capability != nil {
		t.Fatalf("ambiguous prior owner admitted capability %#v", capability)
	}
	var acquisitionErr *runtimestartupownership.AcquisitionError
	if !errors.As(err, &acquisitionErr) || acquisitionErr.Failure != runtimestartupownership.AcquisitionPriorOwnerAmbiguous {
		t.Fatalf("ambiguous prior acquisition error=%v, want typed prior_owner_ambiguous", err)
	}
}

func hasAgentTopologyChange(changes []runtimeagenttopology.DesiredAgentChange, kind runtimeagenttopology.AgentChangeKind) bool {
	for _, change := range changes {
		if change.Kind == kind {
			return true
		}
	}
	return false
}
