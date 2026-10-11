package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func testStartupAcquireRequest(ownerID string) runtimestartupownership.AcquireRequest {
	return runtimestartupownership.AcquireRequest{
		OwnerID: ownerID, BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	}
}

type startupAuthorityParityStore interface {
	runtimestartupownership.Store
	InspectAuthority(context.Context) (runtimestartupownership.AuthorityInspection, error)
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
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: testCanonicalBundleHash}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatalf("build registration source set: %v", err)
	}
	if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatalf("install registration source set: %v", err)
	}
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash:        coordinate.BundleHash,
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
		BundleHash:        previous.BundleHash,
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
				if _, err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err == nil {
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
						if _, err := store.MarkExternalAttemptLaunched(ctx, first, time.Now().UTC()); err != nil {
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
				if _, err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err != nil {
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
			if _, err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err != nil {
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
			if _, err := store.MarkExternalAttemptLaunched(successorCtx, uncertain, time.Now().UTC().Add(-time.Hour)); err != nil {
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
			if err != nil || authority.Validate() != nil {
				t.Fatalf("active capability = %#v err=%v", authority, err)
			}
			coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: testCanonicalBundleHash}
			plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
				t.Fatalf("InstallCompleteSourceSet: %v", err)
			}
			grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
				BundleHash:        coordinate.BundleHash,
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
			stamp, err := generationauthority.ReadProcessGeneration(ctx, db, tc.name == "sqlite")
			if err != nil || stamp != authority.LeaseStamp() {
				t.Fatalf("joined release changed the last-admitted stamp: %+v, %v", stamp, err)
			}
			var generationRows int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_process_generation").Scan(&generationRows); err != nil || generationRows != 1 {
				t.Fatalf("generation rows=%d, err=%v, want one", generationRows, err)
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

func TestProcessGenerationIgnoresDiagnosticMetadataClockParity(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var selected startupAuthorityParityStore
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				selected = admitTestPostgresStore(t, db)
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				selected = store
				db = store.backend.ConstructionHandle()
			}
			first, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("diagnostic-time-predecessor"))
			if err != nil {
				t.Fatalf("acquire predecessor: %v", err)
			}
			if err := first.Release(ctx); err != nil {
				t.Fatalf("release predecessor: %v", err)
			}
			diagnosticTime := time.Now().UTC().Add(24 * time.Hour)
			if backend == "postgres" {
				_, err = db.ExecContext(ctx, `UPDATE runtime_store_metadata SET created_at=$1 WHERE id=1`, diagnosticTime)
			} else {
				_, err = db.ExecContext(ctx, `UPDATE runtime_store_metadata SET created_at=? WHERE id=1`, diagnosticTime.Format(time.RFC3339Nano))
			}
			if err != nil {
				t.Fatalf("change diagnostic authority timestamp: %v", err)
			}
			inspection, err := selected.InspectAuthority(ctx)
			if err != nil || inspection.Status != runtimestartupownership.AuthorityInspectionValid {
				t.Fatalf("inspection after diagnostic timestamp drift=%#v err=%v", inspection, err)
			}
			successor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("diagnostic-time-successor"))
			if err != nil {
				t.Fatalf("acquire successor after diagnostic timestamp drift: %v", err)
			}
			if err := successor.Release(ctx); err != nil {
				t.Fatalf("release successor: %v", err)
			}
		})
	}
}

func TestRuntimeProcessCapabilityClosedSourceSetOperationsPersistWithBackendParity(t *testing.T) {
	const secondBundleHash = "bundle-v2:sha256:2222222222222222222222222222222222222222222222222222222222222222"
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

			firstSource := runtimeagenttopology.SourceCoordinate{BundleHash: testCanonicalBundleHash}
			secondSource := runtimeagenttopology.SourceCoordinate{BundleHash: secondBundleHash}
			firstIdentity, err := testAgentIdentity(t, "source-agent", "").Plan()
			if err != nil {
				t.Fatal(err)
			}
			secondIdentity, err := testAgentIdentity(t, "second-agent", "").Plan()
			if err != nil {
				t.Fatal(err)
			}
			firstAgent := runtimeagenttopology.DesiredAgent{Identity: firstIdentity, Source: firstSource, ConfigRevision: "config-v1"}
			secondAgent := runtimeagenttopology.DesiredAgent{Identity: secondIdentity, Source: secondSource, ConfigRevision: "config-v1"}
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
			replaced, err := capability.RestoreSourceSet(ctx, replaceReq)
			if err != nil || replaced.Operation != runtimeagenttopology.OperationRestoreSourceSet || !hasAgentTopologyChange(replaced.Changes, runtimeagenttopology.AgentChanged) || !hasAgentTopologyChange(replaced.Changes, runtimeagenttopology.AgentAdded) {
				t.Fatalf("replace result=%#v err=%v", replaced, err)
			}
			replayed, err := capability.RestoreSourceSet(ctx, replaceReq)
			if err != nil || !replayed.Replayed || replayed.CurrentRevision != replacement.Revision {
				t.Fatalf("replace replay result=%#v err=%v", replayed, err)
			}

			restored, err := capability.RestoreSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
				OperationID: uuid.NewString(), ExpectedRevision: replacement.Revision, Plan: initial,
			})
			if err != nil || restored.Operation != runtimeagenttopology.OperationRestoreSourceSet || !hasAgentTopologyChange(restored.Changes, runtimeagenttopology.AgentChanged) || !hasAgentTopologyChange(restored.Changes, runtimeagenttopology.AgentRemoved) {
				t.Fatalf("restore result=%#v err=%v", restored, err)
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

func TestPostgresProcessCapabilitySessionLossRetiresAndAdmitsAtomicSuccessor(t *testing.T) {
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
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: testCanonicalBundleHash}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatalf("InstallCompleteSourceSet: %v", err)
	}
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash:        coordinate.BundleHash,
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
	grantEvidence, err := grant.Evidence()
	if err != nil {
		t.Fatalf("generation grant evidence before session loss: %v", err)
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
	if err != nil || successor == nil {
		t.Fatalf("dead predecessor successor=%#v err=%v", successor, err)
	}
	t.Cleanup(func() { _ = successor.Release(context.Background()) })
	var currentRevision string
	if err := db.QueryRowContext(ctx, `SELECT revision FROM agent_topology_source_set_head WHERE singleton_id = 1`).Scan(&currentRevision); err != nil {
		t.Fatalf("read source-set head after successor takeover: %v", err)
	}
	if currentRevision != plan.Revision {
		t.Fatalf("source-set revision after successor takeover=%q, want %q", currentRevision, plan.Revision)
	}
	var lifecycleRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents`).Scan(&lifecycleRows); err != nil || lifecycleRows != 0 {
		t.Fatalf("lifecycle rows after denied successor=%d err=%v, want zero", lifecycleRows, err)
	}
	next, err := successor.Evidence()
	if err != nil || next.Validate() != nil || next.AuthorityID == authority.AuthorityID || next.AuthorityGeneration != authority.AuthorityGeneration+1 {
		t.Fatalf("native successor stamp=%+v, err=%v", next, err)
	}
	stamp, err := generationauthority.ReadProcessGeneration(ctx, db, false)
	if err != nil || stamp != next.LeaseStamp() {
		t.Fatalf("singleton does not identify the admitted native successor: %+v, %v", stamp, err)
	}
	var grantState string
	if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_generation_grants WHERE grant_id=$1::uuid ORDER BY state_version DESC LIMIT 1`, grantEvidence.GrantID).Scan(&grantState); err != nil || grantState != string(runtimestartupownership.GrantRetired) {
		t.Fatalf("durable predecessor grant state=%q err=%v, want retired", grantState, err)
	}
}

func TestPostgresProcessCapabilityIdleSessionLossTerminalizesWithoutForegroundMutation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := admitTestPostgresStore(t, db)
	capability, err := selected.AcquireProcessCapability(context.Background(), testStartupAcquireRequest("idle-session-owner"))
	if err != nil {
		t.Fatalf("AcquireProcessCapability: %v", err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	var retainedPID int
	if err := db.QueryRowContext(context.Background(), `
		SELECT pid FROM pg_locks
		WHERE locktype = 'advisory' AND granted
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
		ORDER BY pid LIMIT 1
	`).Scan(&retainedPID); err != nil {
		t.Fatalf("find retained process session: %v", err)
	}
	var terminated bool
	if err := db.QueryRowContext(context.Background(), `SELECT pg_terminate_backend($1)`, retainedPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate retained process session pid=%d terminated=%v err=%v", retainedPID, terminated, err)
	}
	select {
	case <-capability.Done():
	case <-time.After(4 * time.Second):
		t.Fatal("idle retained-session loss did not terminalize the process capability")
	}
	result, ok := capability.TerminalResult()
	if !ok || result.Cause != runtimestartupownership.TerminalOwnershipUnprovable || result.SuccessorAuthorityID != "" {
		t.Fatalf("terminal result=%#v ok=%v, want ownership_unprovable", result, ok)
	}
}

func TestSQLiteProcessCapabilityFileReplacementTerminalizesIdleOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	selected := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	capability, err := selected.AcquireProcessCapability(context.Background(), testStartupAcquireRequest("sqlite-replaced-owner"))
	if err != nil {
		t.Fatalf("AcquireProcessCapability: %v", err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	if err := os.Rename(path, path+".retired"); err != nil {
		t.Fatalf("retire selected-store file identity: %v", err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("write replacement selected-store identity: %v", err)
	}
	select {
	case <-capability.Done():
	case <-time.After(4 * time.Second):
		t.Fatal("SQLite file replacement did not terminalize the idle process capability")
	}
	result, ok := capability.TerminalResult()
	if !ok || result.Cause != runtimestartupownership.TerminalOwnershipUnprovable || result.SuccessorAuthorityID != "" {
		t.Fatalf("terminal result=%#v ok=%v, want ownership_unprovable", result, ok)
	}
}

func TestSQLiteProcessCapabilityCoordinateReplacementTerminalizesIdleOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	selected := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	capability, err := selected.AcquireProcessCapability(context.Background(), testStartupAcquireRequest("sqlite-coordinate-replaced-owner"))
	if err != nil {
		t.Fatalf("AcquireProcessCapability: %v", err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	coordinatePath := path + ".possession"
	if err := os.Rename(coordinatePath, coordinatePath+".retired"); err != nil {
		t.Fatalf("retire possession coordinate: %v", err)
	}
	if err := os.WriteFile(coordinatePath, nil, 0o600); err != nil {
		t.Fatalf("write replacement possession coordinate: %v", err)
	}
	select {
	case <-capability.Done():
	case <-time.After(4 * time.Second):
		t.Fatal("SQLite possession-coordinate replacement did not terminalize the idle process capability")
	}
	result, ok := capability.TerminalResult()
	if !ok || result.Cause != runtimestartupownership.TerminalOwnershipUnprovable || result.SuccessorAuthorityID != "" {
		t.Fatalf("terminal result=%#v ok=%v, want ownership_unprovable", result, ok)
	}
}

const sqliteForcedDeathChildMarker = "SWARM_TEST_SQLITE_FORCED_DEATH_CHILD"

type sqliteForcedDeathEvidence struct {
	Authority     runtimestartupownership.Authority      `json:"authority"`
	Grant         runtimestartupownership.GrantEvidence  `json:"grant"`
	Revision      string                                 `json:"revision"`
	SelectedGrant *runtimestartupownership.GrantEvidence `json:"selected_grant,omitempty"`
}

func runSQLiteForcedDeathChild(t *testing.T) bool {
	t.Helper()
	if os.Getenv(sqliteForcedDeathChildMarker) == "1" {
		path := os.Getenv("SWARM_TEST_SQLITE_FORCED_DEATH_PATH")
		evidencePath := os.Getenv("SWARM_TEST_SQLITE_FORCED_DEATH_EVIDENCE")
		selected := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
		ctx := testAuthorActivityContext()
		req := testStartupAcquireRequest("forced-death-predecessor")
		capability, err := selected.AcquireProcessCapability(ctx, req)
		if err != nil {
			t.Fatalf("acquire child process capability: %v", err)
		}
		authority, err := capability.Evidence()
		if err != nil {
			t.Fatalf("read child process authority: %v", err)
		}
		coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: testCanonicalBundleHash}
		plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
			t.Fatalf("install child source set: %v", err)
		}
		grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
			BundleHash:        coordinate.BundleHash,
			RuntimeInstanceID: req.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
		})
		if err != nil {
			t.Fatalf("issue child generation grant: %v", err)
		}
		if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
			t.Fatalf("settle child generation probes: %v", err)
		}
		grantEvidence, err := grant.AdmitExecution(ctx)
		if err != nil {
			t.Fatalf("admit child generation: %v", err)
		}
		evidence := sqliteForcedDeathEvidence{Authority: authority, Grant: grantEvidence, Revision: plan.Revision}
		if os.Getenv("SWARM_TEST_FORCED_DEATH_SELECTED_GRANT") == "1" {
			evidence.SelectedGrant = selectedGrantForAbandonedProcess(t, selected, selected.backend.ConstructionHandle(), true, capability)
		}
		raw, err := json.Marshal(evidence)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(evidencePath, raw, 0o600); err != nil {
			t.Fatalf("write child crash evidence: %v", err)
		}
		os.Exit(0)
	}
	return false
}

func spawnSQLiteForcedDeathPredecessor(t *testing.T, path string) sqliteForcedDeathEvidence {
	return spawnSQLiteForcedDeathPredecessorWithSelection(t, path, false)
}

func spawnSQLiteForcedDeathPredecessorWithSelection(t *testing.T, path string, selectedGrant bool) sqliteForcedDeathEvidence {
	t.Helper()
	evidencePath := path + ".crash-evidence.json"
	cmd := exec.Command(os.Args[0], "-test.run=^TestSQLiteProcessCapabilityForcedDeathTakeover$")
	cmd.Env = append(os.Environ(),
		sqliteForcedDeathChildMarker+"=1",
		"SWARM_TEST_SQLITE_FORCED_DEATH_PATH="+path,
		"SWARM_TEST_SQLITE_FORCED_DEATH_EVIDENCE="+evidencePath,
	)
	if selectedGrant {
		cmd.Env = append(cmd.Env, "SWARM_TEST_FORCED_DEATH_SELECTED_GRANT=1")
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("forced-death child: %v\n%s", err, output)
	}
	raw, err := os.ReadFile(evidencePath)
	if err != nil {
		t.Fatalf("read child crash evidence: %v", err)
	}
	var crashed sqliteForcedDeathEvidence
	if err := json.Unmarshal(raw, &crashed); err != nil {
		t.Fatalf("decode child crash evidence: %v", err)
	}
	return crashed
}

func TestSQLiteProcessCapabilityForcedDeathTakeover(t *testing.T) {
	if runSQLiteForcedDeathChild(t) {
		return
	}
	path := filepath.Join(t.TempDir(), "runtime.db")
	selected := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	crashed := spawnSQLiteForcedDeathPredecessor(t, path)

	successor, err := selected.AcquireProcessCapability(testAuthorActivityContext(), testStartupAcquireRequest("forced-death-successor"))
	if err != nil {
		t.Fatalf("acquire forced-death successor: %v", err)
	}
	t.Cleanup(func() { _ = successor.Release(context.Background()) })
	successorEvidence, err := successor.Evidence()
	if err != nil {
		t.Fatalf("read successor evidence: %v", err)
	}
	if successorEvidence.Validate() != nil || successorEvidence.AuthorityID == crashed.Authority.AuthorityID ||
		successorEvidence.AuthorityGeneration != crashed.Authority.AuthorityGeneration+1 {
		t.Fatalf("successor evidence=%#v, want exact monotonic crash takeover of %s", successorEvidence, crashed.Authority.AuthorityID)
	}
	plan, exists, err := successor.CurrentSourceSet(testAuthorActivityContext())
	if err != nil || !exists || plan.Revision != crashed.Revision {
		t.Fatalf("successor source set=%#v exists=%v err=%v, want preserved revision %q", plan, exists, err, crashed.Revision)
	}
	stamp, err := generationauthority.ReadProcessGeneration(context.Background(), selected.backend, true)
	if err != nil || stamp != successorEvidence.LeaseStamp() {
		t.Fatalf("native successor and persisted singleton disagree: %+v, %v", stamp, err)
	}
	var grantState string
	if err := selected.backend.QueryRowContext(context.Background(), `SELECT state FROM runtime_generation_grants WHERE grant_id=? ORDER BY state_version DESC LIMIT 1`, crashed.Grant.GrantID).Scan(&grantState); err != nil {
		t.Fatalf("read crashed generation grant state: %v", err)
	}
	if grantState != string(runtimestartupownership.GrantRetired) {
		t.Fatalf("takeover predecessor grant state=%q, want retired", grantState)
	}
}

func TestProcessCapabilityGracefulAndCrashSuccessionParity(t *testing.T) {
	for _, tc := range []struct {
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
	} {
		t.Run(tc.name+"/graceful_release", func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected := tc.store(t)
			predecessor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("graceful-predecessor"))
			if err != nil {
				t.Fatalf("acquire graceful predecessor: %v", err)
			}
			predecessorEvidence, err := predecessor.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			if err := predecessor.Release(ctx); err != nil {
				t.Fatalf("release graceful predecessor: %v", err)
			}
			successor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("graceful-successor"))
			if err != nil {
				t.Fatalf("acquire graceful successor: %v", err)
			}
			t.Cleanup(func() { _ = successor.Release(context.Background()) })
			evidence, err := successor.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			if evidence.Validate() != nil || evidence.AuthorityID == predecessorEvidence.AuthorityID ||
				evidence.AuthorityGeneration != predecessorEvidence.AuthorityGeneration+1 {
				t.Fatalf("graceful successor=%#v, want exact clean handoff", evidence)
			}
		})
	}

	t.Run("postgres/abandoned_active", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		selected := admitTestPostgresStore(t, db)
		ctx := testAuthorActivityContext()
		predecessor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("postgres-abandoned-predecessor"))
		if err != nil {
			t.Fatalf("acquire abandoned predecessor: %v", err)
		}
		predecessorEvidence, err := predecessor.Evidence()
		if err != nil {
			t.Fatal(err)
		}
		terminatePostgresRetainedProcessSession(t, ctx, db)
		select {
		case <-predecessor.Done():
		case <-time.After(4 * time.Second):
			t.Fatal("terminated predecessor session did not terminalize capability")
		}
		successor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("postgres-abandoned-successor"))
		if err != nil {
			t.Fatalf("acquire abandoned successor: %v", err)
		}
		t.Cleanup(func() { _ = successor.Release(context.Background()) })
		assertProcessLeaseSuccessor(t, successor, predecessorEvidence)
	})

	t.Run("sqlite/abandoned_active", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "runtime.db")
		selected := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
		crashed := spawnSQLiteForcedDeathPredecessor(t, path)
		successor, err := selected.AcquireProcessCapability(testAuthorActivityContext(), testStartupAcquireRequest("sqlite-abandoned-successor"))
		if err != nil {
			t.Fatalf("acquire abandoned successor: %v", err)
		}
		t.Cleanup(func() { _ = successor.Release(context.Background()) })
		assertProcessLeaseSuccessor(t, successor, crashed.Authority)
	})
}

func TestProcessCapabilityReacquisitionMintsFreshLeaseParity(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, abandoned := abandonedProcessCapabilityFixture(t, backend)
			ctx := testAuthorActivityContext()
			exact := runtimestartupownership.AcquireRequest{
				OwnerID: abandoned.Authority.OwnerID, BootID: abandoned.Authority.BootID,
				RuntimeInstanceID: abandoned.Authority.RuntimeInstanceID,
			}
			previous := abandoned.Authority
			for _, owner := range []string{exact.OwnerID, exact.OwnerID + "-changed"} {
				exact.OwnerID = owner
				capability, err := selected.AcquireProcessCapability(ctx, exact)
				if err != nil {
					t.Fatalf("fresh native reacquisition: %v", err)
				}
				evidence, err := capability.Evidence()
				if err != nil || evidence.Validate() != nil || evidence.AuthorityID == previous.AuthorityID ||
					evidence.AuthorityGeneration != previous.AuthorityGeneration+1 ||
					evidence.OwnerID != exact.OwnerID || evidence.BootID != exact.BootID || evidence.RuntimeInstanceID != exact.RuntimeInstanceID {
					t.Fatalf("reacquisition reused an old native lease: %+v, %v", evidence, err)
				}
				if err := capability.ProveCurrent(ctx); err != nil {
					t.Fatalf("new native lease/current-generation proof: %v", err)
				}
				if err := capability.Release(ctx); err != nil {
					t.Fatal(err)
				}
				stamp, err := generationauthority.ReadProcessGeneration(ctx, db, backend == "sqlite")
				if err != nil || stamp != evidence.LeaseStamp() {
					t.Fatalf("release changed the admitted stamp: %+v, %v", stamp, err)
				}
				var rows int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_process_generation").Scan(&rows); err != nil || rows != 1 {
					t.Fatalf("reacquisition grew authority storage: rows=%d err=%v", rows, err)
				}
				previous = evidence
			}
		})
	}
}

func TestProcessCapabilityLiveOwnerWaitPreservesGenerationParity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(*testing.T) (startupAuthorityParityStore, *sql.DB)
	}{
		{name: "postgres", store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
			_, db, _ := testutil.StartPostgres(t)
			return admitTestPostgresStore(t, db), db
		}},
		{name: "sqlite", store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
			selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
			return selected, selected.backend.ConstructionHandle()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected, db := tc.store(t)
			live, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("live-owner"))
			if err != nil {
				t.Fatalf("acquire live owner: %v", err)
			}
			t.Cleanup(func() { _ = live.Release(context.Background()) })
			before := snapshotForkHistoricalExecutionTables(t, db, tc.name == "postgres")
			waiting, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				competing, err := selected.AcquireProcessCapability(waiting, testStartupAcquireRequest("competing-owner"))
				if competing != nil {
					err = errors.Join(errors.New("standby published a competing capability"), competing.Release(context.Background()))
				}
				done <- err
			}()
			<-waiting.Done()
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, tc.name == "postgres")) {
				t.Fatal("standby changed store facts before native ownership")
			}
			if err := live.ProveCurrent(ctx); err != nil {
				t.Fatalf("cancelled standby disturbed the live native owner: %v", err)
			}
			if err := live.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("standby cancellation/join lost its cause: %v", err)
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, tc.name == "postgres")) {
				t.Fatal("cancelled standby wrote a late generation after holder release")
			}
		})
	}
}

func TestProcessAuthorityMonotonicGenerationParity(t *testing.T) {
	for _, tc := range []struct {
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected := tc.store(t)
			var predecessor string
			for generation := uint64(1); generation <= 3; generation++ {
				capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest(fmt.Sprintf("owner-%d", generation)))
				if err != nil {
					t.Fatalf("acquire generation %d: %v", generation, err)
				}
				evidence, err := capability.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				if evidence.Validate() != nil || evidence.AuthorityGeneration != generation || evidence.AuthorityID == predecessor {
					t.Fatalf("authority generation %d evidence=%#v previous lease=%q", generation, evidence, predecessor)
				}
				if err := capability.ProveCurrent(ctx); err != nil {
					t.Fatalf("native possession/current-stamp proof for generation %d: %v", generation, err)
				}
				predecessor = evidence.AuthorityID
				if err := capability.Release(ctx); err != nil {
					t.Fatalf("release generation %d: %v", generation, err)
				}
			}
		})
	}
}

func terminatePostgresRetainedProcessSession(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
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
}

func assertProcessLeaseSuccessor(t *testing.T, successor runtimestartupownership.ProcessCapability, predecessor runtimestartupownership.Authority) {
	t.Helper()
	evidence, err := successor.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Validate() != nil || evidence.AuthorityID == predecessor.AuthorityID ||
		evidence.AuthorityGeneration != predecessor.AuthorityGeneration+1 || evidence.Backend != predecessor.Backend {
		t.Fatalf("successor evidence=%#v, want exact crash takeover of %#v", evidence, predecessor)
	}
	if err := successor.ProveCurrent(testAuthorActivityContext()); err != nil {
		t.Fatalf("native successor/current-generation proof: %v", err)
	}
}

func TestProcessCapabilityTakeoverRollbackParity(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, abandoned := abandonedProcessCapabilityFixture(t, backend)
			ctx := testAuthorActivityContext()
			if backend == "postgres" {
				if _, err := db.ExecContext(ctx, `UPDATE runtime_generation_grants SET snapshot='{}'::jsonb WHERE grant_id=$1::uuid AND state_version=$2`, abandoned.Grant.GrantID, abandoned.Grant.StateVersion); err != nil {
					t.Fatalf("corrupt predecessor grant: %v", err)
				}
			} else if _, err := db.ExecContext(ctx, `UPDATE runtime_generation_grants SET snapshot='{}' WHERE grant_id=? AND state_version=?`, abandoned.Grant.GrantID, abandoned.Grant.StateVersion); err != nil {
				t.Fatalf("corrupt predecessor grant: %v", err)
			}

			successor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("rollback-successor"))
			if successor != nil {
				t.Fatalf("failed takeover returned successor %#v", successor)
			}
			var acquisitionErr *runtimestartupownership.AcquisitionError
			if !errors.As(err, &acquisitionErr) || acquisitionErr.Failure != runtimestartupownership.AcquisitionPriorOwnerAmbiguous {
				t.Fatalf("failed takeover error=%v, want prior_owner_ambiguous", err)
			}

			stamp, err := generationauthority.ReadProcessGeneration(ctx, db, backend == "sqlite")
			if err != nil || stamp != abandoned.Authority.LeaseStamp() {
				t.Fatalf("takeover rollback changed current generation: %+v, %v", stamp, err)
			}
			var rows int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_process_generation").Scan(&rows); err != nil || rows != 1 {
				t.Fatalf("failed takeover leaked generation rows=%d, err=%v", rows, err)
			}
		})
	}
}

func TestProcessCapabilityTakeoverRetiresNewWorkGrantsParity(t *testing.T) {
	proveProcessCapabilityTakeoverRetiresNewWorkGrants(t, false)
}

func TestSelectedGrantBulkTakeoverBothStores(t *testing.T) {
	proveProcessCapabilityTakeoverRetiresNewWorkGrants(t, true)
}

func proveProcessCapabilityTakeoverRetiresNewWorkGrants(t *testing.T, selectedGrant bool) {
	t.Helper()
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			selected, mutationStore, db, abandoned := abandonedProcessCapabilityFixtureWithSelection(t, backend, selectedGrant)
			if selectedGrant && abandoned.SelectedGrant == nil {
				t.Fatal("abandoned process omitted its selected grant")
			}
			proveBulkRetirementWaitsForMutation(t, mutationStore, db, backend, abandoned.Grant, func(ctx context.Context) error {
				capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("blocked-grant-retirement-successor"))
				if capability != nil {
					t.Cleanup(func() { _ = capability.Release(context.Background()) })
				}
				return err
			})
			assertAbandonedSelectedGrantHead(t, db, abandoned, false)
			successor, err := selected.AcquireProcessCapability(testAuthorActivityContext(), testStartupAcquireRequest("grant-retirement-successor"))
			if err != nil {
				t.Fatalf("acquire successor: %v", err)
			}
			t.Cleanup(func() { _ = successor.Release(context.Background()) })
			assertProcessLeaseSuccessor(t, successor, abandoned.Authority)
			placeholder := "?"
			if backend == "postgres" {
				placeholder = "$1::uuid"
			}
			var state string
			if err := db.QueryRowContext(context.Background(), `SELECT state FROM runtime_generation_grants WHERE grant_id=`+placeholder+` ORDER BY state_version DESC LIMIT 1`, abandoned.Grant.GrantID).Scan(&state); err != nil {
				t.Fatalf("read retired predecessor grant: %v", err)
			}
			if state != string(runtimestartupownership.GrantRetired) {
				t.Fatalf("predecessor grant state=%q, want retired", state)
			}
			assertAbandonedSelectedGrantHead(t, db, abandoned, true)
		})
	}
}

func TestProcessCapabilityTakeoverPreservesDurableObligationsParity(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, abandoned := abandonedProcessCapabilityFixture(t, backend)
			ctx := testAuthorActivityContext()

			registration, ok := runtimeeffects.RegistrationFor("provider_registration")
			if !ok {
				t.Fatal("provider_registration effect is not registered")
			}
			attempt, err := selected.AuthorizeExternalAttempt(ctx, testServeRegistrationAuthority(abandoned.Grant), runtimeeffects.AuthorizeRequest{
				OperationID: uuid.NewString(), AttemptID: uuid.NewString(), Kind: registration.Kind,
				Class: registration.Class, Adapter: registration.Adapter, Transport: registration.Transport,
			})
			if err != nil {
				t.Fatalf("authorize preserved provider drain: %v", err)
			}
			attemptState := selectedStoreAttemptState(t, ctx, db, attempt.AttemptID)

			runID := uuid.NewString()
			eventID := uuid.NewString()
			event := eventtest.RunCreatingRootIngress(
				eventID, events.EventType("test.takeover_obligation"), "takeover-proof", "",
				json.RawMessage(`{"proof":"preserve"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			route := testEntitylessNodeDeliveryRoute("takeover-proof-node")
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("commit preserved delivery continuation: %v", err)
			}
			beforeDelivery := loadTakeoverDeliverySnapshot(t, ctx, db, backend, eventID)

			successor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("obligation-preservation-successor"))
			if err != nil {
				t.Fatalf("acquire obligation-preserving successor: %v", err)
			}
			t.Cleanup(func() { _ = successor.Release(context.Background()) })
			assertProcessLeaseSuccessor(t, successor, abandoned.Authority)

			if after := selectedStoreAttemptState(t, ctx, db, attempt.AttemptID); after != attemptState {
				t.Fatalf("provider drain state changed across takeover: before=%q after=%q", attemptState, after)
			}
			if after := loadTakeoverDeliverySnapshot(t, ctx, db, backend, eventID); after != beforeDelivery {
				t.Fatalf("delivery continuation changed across takeover: before=%#v after=%#v", beforeDelivery, after)
			}
		})
	}
}

type takeoverDeliverySnapshot struct {
	DeliveryID       string
	Status           string
	AuthorityKind    string
	AuthorityID      string
	AuthorityVersion uint64
}

func loadTakeoverDeliverySnapshot(t *testing.T, ctx context.Context, db *sql.DB, backend, eventID string) takeoverDeliverySnapshot {
	t.Helper()
	query := `SELECT delivery_id,status,execution_authority_kind,execution_authority_id,execution_authority_generation FROM event_deliveries WHERE event_id=?`
	if backend == "postgres" {
		query = `SELECT delivery_id::text,status,execution_authority_kind,execution_authority_id,execution_authority_generation FROM event_deliveries WHERE event_id=$1::uuid`
	}
	var snapshot takeoverDeliverySnapshot
	if err := db.QueryRowContext(ctx, query, eventID).Scan(&snapshot.DeliveryID, &snapshot.Status, &snapshot.AuthorityKind, &snapshot.AuthorityID, &snapshot.AuthorityVersion); err != nil {
		t.Fatalf("read delivery continuation snapshot: %v", err)
	}
	return snapshot
}

func abandonedProcessCapabilityFixture(t *testing.T, backend string) (startupAuthorityParityStore, *sql.DB, sqliteForcedDeathEvidence) {
	selected, _, db, evidence := abandonedProcessCapabilityFixtureWithSelection(t, backend, false)
	return selected, db, evidence
}

func abandonedProcessCapabilityFixtureWithSelection(t *testing.T, backend string, selectedGrant bool) (startupAuthorityParityStore, selectedFanOutOwner, *sql.DB, sqliteForcedDeathEvidence) {
	t.Helper()
	first, mutationStore, db, _ := newFanOutOwnerPairForTest(t, backend)
	selected := first.(startupAuthorityParityStore)
	if backend == "sqlite" {
		path := first.(interface{ Path() string }).Path()
		return selected, mutationStore, db, spawnSQLiteForcedDeathPredecessorWithSelection(t, path, selectedGrant)
	}
	ctx := testAuthorActivityContext()
	capability, _, grant := admitRegistrationTestGeneration(t, ctx, selected, "abandoned-postgres-owner")
	authority, err := capability.Evidence()
	if err != nil {
		t.Fatalf("read PostgreSQL predecessor authority: %v", err)
	}
	evidence := sqliteForcedDeathEvidence{Authority: authority, Grant: grant, Revision: grant.SourceSetRevision}
	if selectedGrant {
		evidence.SelectedGrant = selectedGrantForAbandonedProcess(t, first.(selectedCompletionAuthorityStore), db, false, capability)
	}
	terminatePostgresRetainedProcessSession(t, ctx, db)
	select {
	case <-capability.Done():
	case <-time.After(4 * time.Second):
		t.Fatal("terminated PostgreSQL predecessor did not stop")
	}
	return selected, mutationStore, db, evidence
}

func selectedGrantForAbandonedProcess(t *testing.T, store selectedCompletionAuthorityStore, db *sql.DB, sqlite bool, process runtimestartupownership.ProcessCapability) *runtimestartupownership.GrantEvidence {
	t.Helper()
	fixture := newSelectedCompletionFixtureWithProcess(t, store, db, sqlite, process)
	grant := selectedMutationFenceGrant(t, testAuthorActivityContext(), fixture)
	evidence, err := grant.Evidence()
	if err != nil || evidence.SelectedFork == nil {
		t.Fatalf("prepare abandoned selected grant: %+v err=%v", evidence, err)
	}
	return &evidence
}

func assertAbandonedSelectedGrantHead(t *testing.T, db *sql.DB, abandoned sqliteForcedDeathEvidence, retired bool) {
	t.Helper()
	if abandoned.SelectedGrant == nil {
		return
	}
	grant := *abandoned.SelectedGrant
	wantState, wantVersion := string(grant.State), grant.StateVersion
	if retired {
		wantState, wantVersion = string(runtimestartupownership.GrantRetired), wantVersion+1
	}
	var state, execution string
	var version uint64
	if err := db.QueryRowContext(testAuthorActivityContext(), `SELECT state,state_version,selected_execution_id FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, grant.GrantID).Scan(&state, &version, &execution); err != nil || state != wantState || version != wantVersion || execution != grant.SelectedFork.ExecutionID {
		t.Fatalf("selected bulk retirement head=%s/%d execution=%s want=%s/%d execution=%s err=%v", state, version, execution, wantState, wantVersion, grant.SelectedFork.ExecutionID, err)
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
	if _, err := db.ExecContext(ctx, `UPDATE runtime_process_generation SET owner_id='' WHERE singleton_id=1`); err != nil {
		t.Fatalf("corrupt prior durable head: %v", err)
	}
	capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("ambiguous-successor"))
	if capability != nil {
		t.Fatalf("ambiguous prior owner admitted capability %#v", capability)
	}
	if err == nil || !strings.Contains(err.Error(), "decode process generation") {
		t.Fatalf("malformed current generation error=%v", err)
	}
}

func TestProcessGenerationMissingRefusesReseedingParity(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (startupAuthorityParityStore, *sql.DB)
	}{
		{name: "postgres", store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
			_, db, _ := testutil.StartPostgres(t)
			return admitTestPostgresStore(t, db), db
		}},
		{name: "sqlite", store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
			selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
			return selected, selected.backend.ConstructionHandle()
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected, db := tc.store(t)
			capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("repair-predecessor"))
			if err != nil {
				t.Fatalf("acquire predecessor: %v", err)
			}
			coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: testCanonicalBundleHash}
			plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
				t.Fatalf("install source set: %v", err)
			}
			if err := capability.Release(ctx); err != nil {
				t.Fatalf("release predecessor: %v", err)
			}
			if _, err := db.ExecContext(ctx, "DELETE FROM runtime_process_generation WHERE singleton_id=1"); err != nil {
				t.Fatal(err)
			}
			before := snapshotForkHistoricalExecutionTables(t, db, tc.name == "postgres")
			if _, err := selected.InspectAuthority(ctx); !errors.Is(err, generationauthority.ErrProcessGenerationMissing) {
				t.Fatalf("initialized missing generation inspection=%v", err)
			}
			for i := 0; i < 2; i++ {
				successor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("no-repair-or-reseed"))
				if successor != nil || !errors.Is(err, generationauthority.ErrProcessGenerationMissing) {
					t.Fatalf("initialized missing generation admitted or reseeded: cap=%v err=%v", successor, err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, tc.name == "postgres")) {
					t.Fatal("refused acquisition repaired generation or changed user/grant/source facts")
				}
			}
			var revision string
			if err := db.QueryRowContext(ctx, "SELECT revision FROM agent_topology_source_set_head WHERE singleton_id=1").Scan(&revision); err != nil || revision != plan.Revision {
				t.Fatalf("refused acquisition changed source revision=%q err=%v", revision, err)
			}
		})
	}
}

func TestProcessGenerationPublicationRetiresEveryCurrentGrantParity(t *testing.T) {
	proveProcessGenerationPublicationRetiresEveryCurrentGrant(t, false)
}

func TestSelectedGrantBulkPublicationBothStores(t *testing.T) {
	proveProcessGenerationPublicationRetiresEveryCurrentGrant(t, true)
}

func proveProcessGenerationPublicationRetiresEveryCurrentGrant(t *testing.T, selectedGrant bool) {
	t.Helper()
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			selected, mutationStore, db, abandoned := abandonedProcessCapabilityFixtureWithSelection(t, backend, selectedGrant)
			if selectedGrant && abandoned.SelectedGrant == nil {
				t.Fatal("abandoned process omitted its selected grant")
			}
			ctx := testAuthorActivityContext()

			registration, ok := runtimeeffects.RegistrationFor("provider_registration")
			if !ok {
				t.Fatal("provider_registration effect is not registered")
			}
			staleRegistration := testServeRegistrationAuthority(abandoned.Grant)
			preservedAttempt, err := selected.AuthorizeExternalAttempt(ctx, staleRegistration, runtimeeffects.AuthorizeRequest{
				OperationID: uuid.NewString(), AttemptID: uuid.NewString(), Kind: registration.Kind,
				Class: registration.Class, Adapter: registration.Adapter, Transport: registration.Transport,
			})
			if err != nil {
				t.Fatalf("authorize predecessor provider attempt: %v", err)
			}
			attemptState := selectedStoreAttemptState(t, ctx, db, preservedAttempt.AttemptID)

			runID := uuid.NewString()
			eventID := uuid.NewString()
			event := eventtest.RunCreatingRootIngress(
				eventID, events.EventType("test.repair_obligation"), "repair-proof", "",
				json.RawMessage(`{"proof":"preserve"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{testEntitylessNodeDeliveryRoute("repair-proof-node")}); err != nil {
				t.Fatalf("commit publication-preserved delivery continuation: %v", err)
			}
			deliveryBefore := loadTakeoverDeliverySnapshot(t, ctx, db, backend, eventID)

			successorRequest := testStartupAcquireRequest("atomic-process-publication")
			proveBulkRetirementWaitsForMutation(t, mutationStore, db, backend, abandoned.Grant, func(ctx context.Context) error {
				capability, err := selected.AcquireProcessCapability(ctx, successorRequest)
				if capability != nil {
					err = errors.Join(err, capability.Release(context.Background()))
				}
				return err
			})
			assertAbandonedSelectedGrantHead(t, db, abandoned, false)
			before := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
			removeFault := installProcessGenerationAdvanceFault(t, db, backend == "postgres")
			failed, err := selected.AcquireProcessCapability(ctx, successorRequest)
			if failed != nil || err == nil || !strings.Contains(err.Error(), "injected process generation failure") {
				t.Fatalf("faulted generation publication: capability=%v err=%v", failed, err)
			}
			removeFault()
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
				t.Fatal("rolled-back generation publication changed any table")
			}
			assertAbandonedSelectedGrantHead(t, db, abandoned, false)
			successor, err := selected.AcquireProcessCapability(ctx, successorRequest)
			if err != nil {
				t.Fatalf("native successor after rollback: %v", err)
			}
			t.Cleanup(func() { _ = successor.Release(context.Background()) })
			assertProcessLeaseSuccessor(t, successor, abandoned.Authority)
			var grantState string
			if err := db.QueryRowContext(ctx, "SELECT state FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1", abandoned.Grant.GrantID).Scan(&grantState); err != nil || grantState != string(runtimestartupownership.GrantRetired) {
				t.Fatalf("published successor did not retire old grant: state=%s err=%v", grantState, err)
			}
			assertAbandonedSelectedGrantHead(t, db, abandoned, true)
			if after := selectedStoreAttemptState(t, ctx, db, preservedAttempt.AttemptID); after != attemptState {
				t.Fatalf("provider attempt changed across publication: before=%q after=%q", attemptState, after)
			}
			if after := loadTakeoverDeliverySnapshot(t, ctx, db, backend, eventID); after != deliveryBefore {
				t.Fatalf("delivery continuation changed across publication: before=%#v after=%#v", deliveryBefore, after)
			}

			for _, stale := range []struct {
				name         string
				authority    runtimeeffects.Authority
				registration runtimeeffects.Registration
			}{
				{name: "serve registration", authority: staleRegistration, registration: registration},
				{name: "startup probe", authority: testStartupProbeAuthority(abandoned.Grant), registration: mustEffectRegistration(t, "claude_cli_startup_probe")},
			} {
				t.Run(stale.name, func(t *testing.T) {
					if _, err := selected.AuthorizeExternalAttempt(ctx, stale.authority, runtimeeffects.AuthorizeRequest{
						OperationID: uuid.NewString(), AttemptID: uuid.NewString(), Kind: stale.registration.Kind,
						Class: stale.registration.Class, Adapter: stale.registration.Adapter, Transport: stale.registration.Transport,
					}); err == nil {
						t.Fatalf("retired predecessor %s authority admitted new work", stale.name)
					}
				})
			}

			grant, err := successor.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
				BundleHash:        abandoned.Grant.BundleHash,
				RuntimeInstanceID: successorRequest.RuntimeInstanceID, RuntimeGeneration: abandoned.Grant.RuntimeGeneration + 1,
				SourceSetRevision: abandoned.Revision,
			})
			if err != nil {
				t.Fatalf("issue successor grant after publication: %v", err)
			}
			if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatalf("settle successor probes after publication: %v", err)
			}
			if _, err := grant.AdmitExecution(ctx); err != nil {
				t.Fatalf("admit successor execution after publication: %v", err)
			}
			if err := successor.Release(ctx); err != nil {
				t.Fatalf("release successor after publication: %v", err)
			}
		})
	}
}

func mustEffectRegistration(t *testing.T, adapter string) runtimeeffects.Registration {
	t.Helper()
	registration, ok := runtimeeffects.RegistrationFor(adapter)
	if !ok {
		t.Fatalf("effect adapter %q is not registered", adapter)
	}
	return registration
}

func testStartupProbeAuthority(startup runtimestartupownership.GrantEvidence) runtimeeffects.Authority {
	probeID := uuid.NewString()
	stateVersion := startup.StateVersion
	if stateVersion > 1 {
		stateVersion--
	}
	return runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityStartupProbe, ID: probeID,
		ExecutionOwner: startup.ProcessOwnerID, LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
		FenceGeneration: startup.RuntimeGeneration, ExecutionMode: runtimeeffects.ExecutionModeLive,
		StartupProbe: runtimeeffects.StartupProbeAuthority{
			ProbeID: probeID, StartupAuthorityID: startup.GrantID, StartupStateVersion: stateVersion,
			ActorID: "repair-proof-actor", ExecutionKind: "provider_startup_probe", ExecutionAuthorityID: uuid.NewString(),
		},
	}
}

func TestProcessCapabilityOperationCancellationPreservesPossessionParity(t *testing.T) {
	tests := []struct {
		name   string
		store  func(*testing.T) (startupAuthorityParityStore, *sql.DB)
		sqlite bool
	}{
		{name: "postgres", store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
			_, db, _ := testutil.StartPostgres(t)
			return admitTestPostgresStore(t, db), db
		}},
		{name: "sqlite", sqlite: true, store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
			selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
			return selected, selected.backend.ConstructionHandle()
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected, db := tc.store(t)
			capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("cancellation-owner"))
			if err != nil {
				t.Fatalf("acquire process capability: %v", err)
			}
			t.Cleanup(func() { _ = capability.Release(context.Background()) })

			cancelledBeforeProof, cancelBeforeProof := context.WithCancel(ctx)
			cancelBeforeProof()
			if _, _, err := capability.CurrentSourceSet(cancelledBeforeProof); !errors.Is(err, context.Canceled) {
				t.Fatalf("pre-proof cancellation error=%v, want context.Canceled", err)
			}
			if result, terminal := capability.TerminalResult(); terminal {
				t.Fatalf("pre-proof cancellation terminalized capability: %#v", result)
			}

			lockTx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin backend I/O barrier: %v", err)
			}
			lockReleased := false
			t.Cleanup(func() {
				if !lockReleased {
					_ = lockTx.Rollback()
				}
			})
			if tc.sqlite {
				_, err = lockTx.ExecContext(ctx, `UPDATE runtime_store_metadata SET created_at=created_at WHERE id=1`)
			} else {
				_, err = lockTx.ExecContext(ctx, `LOCK TABLE agent_topology_source_set_head IN ACCESS EXCLUSIVE MODE`)
			}
			if err != nil {
				t.Fatalf("establish backend I/O barrier: %v", err)
			}

			plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{{
				BundleHash: testCanonicalBundleHash,
			}}, nil)
			if err != nil {
				t.Fatalf("construct cancellation source set: %v", err)
			}
			operationCtx, cancelOperation := context.WithTimeout(ctx, 500*time.Millisecond)
			installed := make(chan error, 1)
			go func() {
				_, err := capability.InstallCompleteSourceSet(operationCtx, runtimeagenttopology.SourceSetCommitRequest{
					OperationID: uuid.NewString(), Plan: plan,
				})
				installed <- err
			}()
			<-operationCtx.Done()
			if err := lockTx.Rollback(); err != nil {
				t.Fatalf("release backend I/O barrier for admitted drain: %v", err)
			}
			lockReleased = true
			err = <-installed
			cancelOperation()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("backend I/O cancellation error=%v, want context.DeadlineExceeded", err)
			}
			if result, terminal := capability.TerminalResult(); terminal {
				t.Fatalf("backend I/O cancellation terminalized capability: %#v", result)
			}

			waiting, stopWaiting := context.WithTimeout(ctx, 50*time.Millisecond)
			defer stopWaiting()
			contenderDone := make(chan error, 1)
			go func() {
				contender, err := selected.AcquireProcessCapability(waiting, testStartupAcquireRequest("cancelled-operation-contender"))
				if contender != nil {
					err = errors.Join(errors.New("contender acquired live possession"), contender.Release(context.Background()))
				}
				contenderDone <- err
			}()
			<-waiting.Done()
			if _, _, err := capability.CurrentSourceSet(ctx); err != nil {
				t.Fatalf("process capability after caller cancellation: %v", err)
			}
			if err := capability.Release(ctx); err != nil {
				t.Fatalf("clean release after caller cancellation: %v", err)
			}
			if err := <-contenderDone; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("cancelled contender did not join with its cause: %v", err)
			}
			if result, terminal := capability.TerminalResult(); !terminal || result.Cause != runtimestartupownership.TerminalReleased {
				t.Fatalf("clean release terminal result=%#v terminal=%v", result, terminal)
			}
			successor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("post-cancellation-successor"))
			if err != nil {
				t.Fatalf("acquire successor after clean release: %v", err)
			}
			if err := successor.Release(ctx); err != nil {
				t.Fatalf("release successor after caller cancellation proof: %v", err)
			}
		})
	}
}

func TestProcessCapabilityReleaseFailurePreservesPossessionParity(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		newStore := func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
			t.Helper()
			if backend == "postgres" {
				_, db, _ := testutil.StartPostgres(t)
				return admitTestPostgresStore(t, db), db
			}
			selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
			return selected, selected.backend.ConstructionHandle()
		}
		t.Run(backend+"/pre_cancelled", func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected, _ := newStore(t)
			capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("pre-cancelled-release-owner"))
			if err != nil {
				t.Fatalf("acquire process capability: %v", err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := capability.Release(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatalf("pre-cancelled Release error = %v, want context.Canceled", err)
			}
			proveFailedReleaseRetainsPossession(t, ctx, selected, capability, "pre-cancelled")
		})
		t.Run(backend+"/transition_failure", func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected, db := newStore(t)
			capability, _, _ := admitRegistrationTestGeneration(t, ctx, selected, "failed-transition-release-owner")
			removeFault := installGrantRetirementFault(t, db, backend == "postgres")
			if err := capability.Release(ctx); err == nil || !strings.Contains(err.Error(), "injected grant retirement failure") {
				t.Fatalf("faulted Release error = %v, want injected transition failure", err)
			}
			removeFault()
			proveFailedReleaseRetainsPossession(t, ctx, selected, capability, "transition-failure")
		})
	}
}

func proveFailedReleaseRetainsPossession(
	t *testing.T,
	ctx context.Context,
	selected startupAuthorityParityStore,
	capability runtimestartupownership.ProcessCapability,
	label string,
) {
	t.Helper()
	if result, terminal := capability.TerminalResult(); terminal {
		t.Fatalf("%s release failure terminalized capability: %#v", label, result)
	}
	if _, err := capability.Evidence(); err != nil {
		t.Fatalf("%s release failure lost current capability evidence: %v", label, err)
	}
	waiting, stopWaiting := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stopWaiting()
	contenderDone := make(chan error, 1)
	go func() {
		contender, err := selected.AcquireProcessCapability(waiting, testStartupAcquireRequest(label+"-contender"))
		if contender != nil {
			err = errors.Join(errors.New("failed release admitted a competing native lease"), contender.Release(context.Background()))
		}
		contenderDone <- err
	}()
	<-waiting.Done()
	if err := capability.Release(ctx); err != nil {
		t.Fatalf("%s retry clean release: %v", label, err)
	}
	if err := <-contenderDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%s contender cancellation/join: %v", label, err)
	}
	if result, terminal := capability.TerminalResult(); !terminal || result.Cause != runtimestartupownership.TerminalReleased {
		t.Fatalf("%s clean retry terminal result=%#v terminal=%v, want released", label, result, terminal)
	}
	successor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest(label+"-successor"))
	if err != nil {
		t.Fatalf("%s acquire successor after clean release: %v", label, err)
	}
	if err := successor.Release(ctx); err != nil {
		t.Fatalf("%s release successor: %v", label, err)
	}
}

func installProcessGenerationAdvanceFault(t *testing.T, db *sql.DB, postgres bool) func() {
	t.Helper()
	name := "fail_process_generation_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if postgres {
		function := name + "_fn"
		if _, err := db.Exec(`CREATE FUNCTION ` + function + `() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'injected process generation failure'; END; $$ LANGUAGE plpgsql`); err != nil {
			t.Fatalf("create PostgreSQL process generation fault function: %v", err)
		}
		if _, err := db.Exec(`CREATE TRIGGER ` + name + ` BEFORE UPDATE ON runtime_process_generation FOR EACH ROW EXECUTE FUNCTION ` + function + `()`); err != nil {
			_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + function + `()`)
			t.Fatalf("create PostgreSQL process generation fault trigger: %v", err)
		}
		removed := false
		remove := func() {
			if removed {
				return
			}
			removed = true
			_, _ = db.Exec(`DROP TRIGGER IF EXISTS ` + name + ` ON runtime_process_generation`)
			_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + function + `()`)
		}
		t.Cleanup(remove)
		return remove
	}
	if _, err := db.Exec(`CREATE TRIGGER ` + name + ` BEFORE UPDATE ON runtime_process_generation BEGIN SELECT RAISE(ABORT, 'injected process generation failure'); END`); err != nil {
		t.Fatalf("create SQLite process generation fault trigger: %v", err)
	}
	removed := false
	remove := func() {
		if removed {
			return
		}
		removed = true
		_, _ = db.Exec(`DROP TRIGGER IF EXISTS ` + name)
	}
	t.Cleanup(remove)
	return remove
}

func installGrantRetirementFault(t *testing.T, db *sql.DB, postgres bool) func() {
	t.Helper()
	name := "fail_grant_retirement_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if postgres {
		function := name + "_fn"
		if _, err := db.Exec(`CREATE FUNCTION ` + function + `() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'injected grant retirement failure'; END; $$ LANGUAGE plpgsql`); err != nil {
			t.Fatalf("create PostgreSQL grant retirement fault function: %v", err)
		}
		if _, err := db.Exec(`CREATE TRIGGER ` + name + ` BEFORE INSERT ON runtime_generation_grants FOR EACH ROW WHEN (NEW.state = 'retired') EXECUTE FUNCTION ` + function + `()`); err != nil {
			_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + function + `()`)
			t.Fatalf("create PostgreSQL grant retirement fault trigger: %v", err)
		}
		removed := false
		remove := func() {
			if removed {
				return
			}
			removed = true
			_, _ = db.Exec(`DROP TRIGGER IF EXISTS ` + name + ` ON runtime_generation_grants`)
			_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + function + `()`)
		}
		t.Cleanup(remove)
		return remove
	}
	if _, err := db.Exec(`CREATE TRIGGER ` + name + ` BEFORE INSERT ON runtime_generation_grants WHEN NEW.state = 'retired' BEGIN SELECT RAISE(ABORT, 'injected grant retirement failure'); END`); err != nil {
		t.Fatalf("create SQLite grant retirement fault trigger: %v", err)
	}
	removed := false
	remove := func() {
		if removed {
			return
		}
		removed = true
		_, _ = db.Exec(`DROP TRIGGER IF EXISTS ` + name)
	}
	t.Cleanup(remove)
	return remove
}

func TestProcessCapabilityRejectsChangedCanonicalStampParity(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		for _, field := range []string{"lease", "owner", "boot", "runtime", "backend", "generation"} {
			t.Run(backend+"/"+field, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				var selected startupAuthorityParityStore
				var db *sql.DB
				if backend == "postgres" {
					_, db, _ = testutil.StartPostgres(t)
					selected = admitTestPostgresStore(t, db)
				} else {
					owner := newBootstrappedSQLiteRuntimeStoreForTest(t)
					selected, db = owner, owner.backend.ConstructionHandle()
				}
				capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("canonical-stamp-"+field))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = capability.Release(context.Background()) })
				forged, err := capability.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				switch field {
				case "lease":
					forged.AuthorityID = uuid.NewString()
				case "owner":
					forged.OwnerID += "-crossed"
				case "boot":
					forged.BootID = uuid.NewString()
				case "runtime":
					forged.RuntimeInstanceID = uuid.NewString()
				case "backend":
					forged.Backend = processbinding.SQLiteLeaseBackend
					if backend == "sqlite" {
						forged.Backend = processbinding.PostgresLeaseBackend
					}
				case "generation":
					forged.AuthorityGeneration += 10
				}
				if err := forged.Validate(); err != nil {
					t.Fatalf("negative control must be internally valid: %v", err)
				}
				if _, err := db.ExecContext(ctx, "UPDATE runtime_process_generation SET lease_id=$1,generation=$2,owner_id=$3,boot_id=$4,runtime_instance_id=$5,backend=$6 WHERE singleton_id=1",
					forged.AuthorityID, int64(forged.AuthorityGeneration), forged.OwnerID, forged.BootID, forged.RuntimeInstanceID, forged.Backend); err != nil {
					t.Fatal(err)
				}
				before := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
				inspection, readErr := selected.InspectAuthority(ctx)
				if field == "backend" {
					if readErr == nil || !strings.Contains(readErr.Error(), "process-generation backend differs") {
						t.Fatalf("crossed backend cause=%v", readErr)
					}
				} else if readErr != nil || inspection.Authority == nil || *inspection.Authority != forged {
					t.Fatalf("last-admitted readback invented lineage/native possession: %+v, %v", inspection, readErr)
				}
				wantCause := "PostgreSQL process generation changed"
				if backend == "sqlite" {
					wantCause = "SQLite process capability generation changed"
				}
				if err := capability.ProveCurrent(ctx); err == nil || (field != "backend" && !strings.Contains(err.Error(), wantCause)) ||
					(field == "backend" && !strings.Contains(err.Error(), "process-generation backend differs")) {
					t.Fatalf("%s retained permission or failed for a different cause: %v", field, err)
				}
				select {
				case <-capability.Done():
				default:
					t.Fatal("changed stamp left process permission live")
				}
				if _, err := capability.Evidence(); err == nil {
					t.Fatal("fenced process exposed executable evidence")
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
					t.Fatal("refusal repaired identity or changed another table")
				}
			})
		}
	}
}

func TestSQLiteProcessGenerationRejectsMalformedFieldClass(t *testing.T) {
	for _, malformed := range []struct{ name, assignment, cause string }{
		{"lease", "lease_id='not-a-uuid'", "decode process generation"},
		{"owner", "owner_id=''", "decode process generation"},
		{"boot", "boot_id='not-a-uuid'", "decode process generation"},
		{"runtime", "runtime_instance_id='not-a-uuid'", "decode process generation"},
		{"backend", "backend='postgres_retained_session'", "process-generation backend differs"},
		{"fractional generation", "generation=1.5", "read process generation"},
		{"noncanonical lease", "lease_id=upper(lease_id)", "decode process generation"},
	} {
		t.Run(malformed.name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
			db := selected.backend.ConstructionHandle()
			capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("malformed-generation"))
			if err != nil {
				t.Fatal(err)
			}
			if err := capability.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "UPDATE runtime_process_generation SET "+malformed.assignment+" WHERE singleton_id=1"); err != nil {
				t.Fatalf("negative-control injection: %v", err)
			}
			before := snapshotForkHistoricalExecutionTables(t, db, false)
			if _, err := selected.InspectAuthority(ctx); err == nil || !strings.Contains(err.Error(), malformed.cause) {
				t.Fatalf("malformed %s readback error=%v, want %s", malformed.name, err, malformed.cause)
			}
			successor, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("no-malformed-repair"))
			if successor != nil || err == nil || !strings.Contains(err.Error(), malformed.cause) {
				t.Fatalf("malformed %s cap=%v error=%v, want %s", malformed.name, successor, err, malformed.cause)
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, false)) {
				t.Fatal("malformed refusal repaired identity, reseeded or changed any table")
			}
		})
	}
}

func TestProcessInspectionGrantsNoServingAuthorityParity(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var selected startupAuthorityParityStore
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				selected = admitTestPostgresStore(t, db)
			} else {
				owner := newBootstrappedSQLiteRuntimeStoreForTest(t)
				selected, db = owner, owner.backend.ConstructionHandle()
			}
			capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("inspection-holder"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = capability.Release(context.Background()) })
			before := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
			if _, present := before["runtime_startup_authority_facts"]; present {
				t.Fatal("retired journal is live")
			}
			if _, present := before["runtime_startup_authority_repairs"]; present {
				t.Fatal("retired repair is live")
			}
			for i := 0; i < 2; i++ {
				inspection, err := selected.InspectAuthority(ctx)
				evidence, proofErr := capability.Evidence()
				if err != nil || proofErr != nil || inspection.Authority == nil || *inspection.Authority != evidence || inspection.Possession != nil {
					t.Fatalf("row inspection claimed native permission: %+v, %v, %v", inspection, err, proofErr)
				}
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
				t.Fatal("inspection changed any table")
			}
			if err := capability.ProveCurrent(ctx); err != nil {
				t.Fatalf("inspection disturbed native possession: %v", err)
			}
			if err := capability.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
				t.Fatal("release appended a journal or rewrote the admitted stamp")
			}
		})
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
