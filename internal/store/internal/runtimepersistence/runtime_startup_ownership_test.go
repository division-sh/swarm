package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
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
	intentID := uuid.NewString()
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
