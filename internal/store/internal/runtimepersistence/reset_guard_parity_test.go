package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestResetRetainedCleanupProviderAuthorityBothStores(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		backend := "postgres"
		if sqlite {
			backend = "sqlite"
		}
		for _, condition := range []string{"pending_current", "pending_drain", "terminal_current", "terminal_drain"} {
			t.Run(backend+"/"+condition, func(t *testing.T) {
				fixture := newCompletionReviewFixture(t, sqlite)
				ctx := providerDrainContext(t, fixture, "reset-guard-"+condition)
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", condition)
				drained, terminal := strings.HasSuffix(condition, "drain"), strings.HasPrefix(condition, "terminal")
				if drained {
					if transition := supersedeProviderDrainFixture(t, fixture, manager.AgentLifecycleTerminated); transition.ProviderDrainCount != 1 {
						t.Fatalf("provider transition has no drain: %+v", transition)
					}
				}
				if terminal {
					settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "provider-head-current", "provider-head-after-reset")
					if drained {
						settlement.ProviderHead = nil
					}
					if _, err := handle.SettleCompletion(ctx, settlement); err != nil {
						t.Fatal(err)
					}
					if !drained {
						if _, err := fixture.store.(deliverylifecycle.Store).SettleSuccess(testAuthorActivityContext(), fixture.origin, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
							t.Fatal(err)
						}
					}
				}
				capability, err := agentfixture.ProcessCapability(t, ctx, fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				request := admitRetainedResetCleanupProof(t, capability, fixture.store.(destructivereset.QuiescenceStore), fixture.authority.Target.RunID, false)
				_, err = capability.ApplyDestructiveResetCleanup(testAuthorActivityContext(), request, nil)
				if !terminal {
					if !errors.Is(err, destructivereset.ErrInvalidRequest) || !strings.Contains(err.Error(), "nonterminal provider authority") {
						t.Fatalf("pending provider cleanup = %v", err)
					}
					requireExternalAttemptState(t, fixture.db, sqlite, handle.Attempt().AttemptID, effects.StateResponseObserved)
					if drained {
						requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "pending")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if drained {
						requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
					}
				}
				var count int
				if err := fixture.db.QueryRow("SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE attempt_id = $1", handle.Attempt().AttemptID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("reset lost provider authority: count=%d err=%v", count, err)
				}
				wantRuns := 1
				if terminal {
					wantRuns = 0
				}
				if err := fixture.db.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", fixture.authority.Target.RunID).Scan(&count); err != nil || count != wantRuns {
					t.Fatalf("reset run boundary: count=%d want=%d err=%v", count, wantRuns, err)
				}
			})
		}
	}
}

func TestResetRetainedCleanupDirectiveAuthorityBothStores(t *testing.T) {
	for _, state := range []string{"prepared", "executing", "indeterminate", "succeeded", "failed", "expired_succeeded", "expired_failed"} {
		t.Run(state, func(t *testing.T) {
			forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
				ctx := testAuthorActivityContext()
				seedDirectiveOperationRun(t, backend.db, backend.name == "postgres")
				now := time.Now().UTC()
				if strings.HasPrefix(state, "expired_") {
					now = now.Add(-48 * time.Hour)
				}
				reserved, err := backend.store.ReserveDirectiveOperation(ctx, directiveOperationReservationForTest(t, uuid.NewString(), uuid.NewString(), "reset-guard", "reset-guard", now))
				if err != nil {
					t.Fatal(err)
				}
				const owner = "reset-guard-owner"
				if state != "prepared" {
					if _, err := admitDirectiveExecutionForTest(ctx, backend.store, reserved.Operation.OperationID, owner, now, time.Minute); err != nil {
						t.Fatal(err)
					}
				}
				switch {
				case strings.HasSuffix(state, "succeeded"):
					if _, err := backend.store.RecordDirectiveExecuted(ctx, reserved.Operation.OperationID, owner, directiveOperationResponseForTest(reserved.Operation), now); err != nil {
						t.Fatal(err)
					}
					_, err = backend.store.FinalizeDirectiveSuccess(ctx, reserved.Operation.OperationID, now, 24*time.Hour)
				case strings.HasSuffix(state, "failed"):
					_, err = backend.store.FinalizeDirectiveFailure(ctx, reserved.Operation.OperationID, owner, agentcontrol.DirectiveBoardStepFailure(errors.New("fixture failure")), now, 24*time.Hour)
				case state == "indeterminate":
					_, _, err = backend.store.ReconcileDirectiveOperation(ctx, reserved.Operation.OperationID, now.Add(2*time.Minute), 24*time.Hour)
				}
				if err != nil {
					t.Fatal(err)
				}
				capability, err := backend.store.(startupownership.Store).AcquireProcessCapability(ctx, testStartupAcquireRequest("reset-directive-guard"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := capability.Release(context.Background()); err != nil {
						t.Error(err)
					}
				})
				request := admitRetainedResetCleanupProof(t, capability, backend.store.(destructivereset.QuiescenceStore), directiveOperationTestRunID, false)
				_, err = capability.ApplyDestructiveResetCleanup(ctx, request, nil)
				expired := strings.HasPrefix(state, "expired_")
				if expired {
					if err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, destructivereset.ErrInvalidRequest) || !strings.Contains(err.Error(), "retained agent directive authority") {
					t.Fatalf("unexpired directive cleanup = %v", err)
				}
				_, exists, err := backend.store.LoadDirectiveOperation(ctx, reserved.Operation.OperationID)
				if err != nil || exists == expired {
					t.Fatalf("directive preservation exists=%t expired=%t err=%v", exists, expired, err)
				}
				var count int
				want := 1
				if expired {
					want = 0
				}
				if err := backend.db.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", directiveOperationTestRunID).Scan(&count); err != nil || count != want {
					t.Fatalf("directive run preservation count=%d want=%d err=%v", count, want, err)
				}
			})
		})
	}
}

func TestResetRetainedCleanupScopeAndRollbackBothStores(t *testing.T) {
	for _, condition := range []string{"foreign_key", "late_source_run", "late_retained_run", "forged_quiescence"} {
		t.Run(condition, func(t *testing.T) {
			forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
				ctx := testAuthorActivityContext()
				artifact := sourceartifactfixture.Artifact()
				sourceartifactfixture.RequireArtifact(t, ctx, backend.store.(sourceartifactfixture.Writer), artifact)
				seedRun := func(id string) {
					t.Helper()
					fixture := runlifecyclefixture.Fixture{RunID: id, BundleHash: artifact.BundleHash(), Origin: runlifecyclefixture.ScenarioSetupOrigin()}
					if backend.name == "sqlite" {
						runlifecyclefixture.RequireSQLite(t, ctx, backend.db, fixture)
					} else {
						runlifecyclefixture.RequirePostgres(t, ctx, backend.db, fixture)
					}
				}
				runID := uuid.NewString()
				seedRun(runID)
				capability, err := backend.store.(startupownership.Store).AcquireProcessCapability(ctx, testStartupAcquireRequest("reset-scope-guard"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := capability.Release(context.Background()); err != nil {
						t.Error(err)
					}
				})
				request := admitRetainedResetCleanupProof(t, capability, backend.store.(destructivereset.QuiescenceStore), runID, condition == "late_source_run")
				laterID := uuid.NewString()
				seedRun(laterID)
				if condition == "foreign_key" {
					keyType := "TEXT"
					if backend.name == "postgres" {
						keyType = "UUID"
					}
					if _, err := backend.db.Exec("CREATE TABLE reset_unknown_reference (run_id " + keyType + " NOT NULL REFERENCES runs(run_id))"); err != nil {
						t.Fatal(err)
					}
					if _, err := backend.db.Exec("INSERT INTO reset_unknown_reference (run_id) VALUES ($1)", runID); err != nil {
						t.Fatal(err)
					}
				}
				if condition == "forged_quiescence" {
					request.Quiescence.AppliedAt = time.Time{}
				}
				result, err := capability.ApplyDestructiveResetCleanup(ctx, request, nil)
				wantOriginal := 1
				if condition == "late_retained_run" {
					if err != nil {
						t.Fatal(err)
					}
					wantOriginal = 0
					// Every canonical physical platform table is visited, including
					// zero-row families. Existing lineage fixtures prove rich rows.
					seen := map[string]bool{}
					for _, row := range result.Tables {
						seen[row.Table] = true
					}
					for _, entry := range destructivereset.DefaultPlatformCleanupCatalog() {
						if !seen[entry.Table] {
							t.Fatalf("cleanup omitted catalog table %s", entry.Table)
						}
					}
				} else if err == nil {
					t.Fatal("unsafe cleanup succeeded")
				}
				switch condition {
				case "foreign_key":
					if !strings.Contains(strings.ToLower(err.Error()), "foreign key") {
						t.Fatalf("cleanup did not reach foreign-key rollback: %v", err)
					}
				case "late_source_run":
					if !errors.Is(err, destructivereset.ErrInvalidRequest) || !strings.Contains(err.Error(), "runs outside the cleanup plan") {
						t.Fatalf("cleanup did not reject the late source run: %v", err)
					}
				case "forged_quiescence":
					if !strings.Contains(err.Error(), "cleanup does not match durable reset evidence") {
						t.Fatalf("cleanup did not reject forged quiescence: %v", err)
					}
				}
				for id, want := range map[string]int{runID: wantOriginal, laterID: 1} {
					var count int
					if err := backend.db.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", id).Scan(&count); err != nil || count != want {
						t.Fatalf("scope/rollback run %s count=%d want=%d err=%v", id, count, want, err)
					}
				}
				if wantOriginal == 1 {
					op, err := capability.ReadResetOperation(ctx, request.OperationID)
					if err != nil || op.Phase != destructivereset.PhaseQuiesced || op.Cleanup != nil {
						t.Fatalf("failed cleanup advanced receipt: %+v, %v", op, err)
					}
				}
			})
		})
	}
}

func admitRetainedResetCleanupProof(t *testing.T, capability startupownership.ProcessCapability, quiescer destructivereset.QuiescenceStore, runID string, includeSources bool) destructivereset.CleanupRequest {
	t.Helper()
	ctx := testAuthorActivityContext()
	now := time.Now().UTC().Add(time.Minute)
	operation, err := capability.AdmitResetOperation(ctx, destructivereset.Request{
		OperationID: uuid.NewString(), ActorTokenID: "operator-token", RequestHash: "cleanup-guard",
		RequestedAt: now, IncludeSourceArtifacts: includeSources, IncludeSourceArtifactsSet: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	planned := operation
	planned.Phase, planned.Revision = destructivereset.PhasePlanned, operation.Revision+1
	planned.Plan = &destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: now, Plan: cleanupPlanForRunIDs(runID)}
	planned.Plan.IncludeSourceArtifacts, planned.Plan.Plan.IncludeSourceArtifacts = includeSources, includeSources
	planned.Plan.Plan.ActiveRuns = []destructivereset.RunRef{{RunID: runID, Status: "running"}}
	if err := capability.AdvanceResetOperation(ctx, operation, planned); err != nil {
		t.Fatal(err)
	}
	quiescence, err := quiescer.ApplyDestructiveResetQuiescence(ctx, destructivereset.QuiescenceRequest{OperationID: operation.Request.OperationID, ActorTokenID: operation.Request.ActorTokenID, RequestedAt: now, Result: *planned.Plan})
	if err != nil {
		t.Fatal(err)
	}
	return destructivereset.CleanupRequest{OperationID: operation.Request.OperationID, ActorTokenID: operation.Request.ActorTokenID, RequestedAt: now, Result: *planned.Plan, Quiescence: quiescence}
}
