package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestSelectedForkGenerationGrantExactAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedCompletionFixture(t, store, db, sqlite)
			ctx := testAuthorActivityContext()
			fixture.request.ContainerPlanFingerprint = "sha256:" + strings.Repeat("1", 64)
			fixture.request.ActorCensusFingerprint = "sha256:" + strings.Repeat("2", 64)
			fixture.request.EffectiveConfigFingerprint = "sha256:" + strings.Repeat("3", 64)
			issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "selected-grant-worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			ports := store.(interface {
				startupownership.Store
				RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
			})
			binding, err := ports.RequireRunForkSelectedContractBinding(ctx, fixture.forkRun)
			if err != nil {
				t.Fatal(err)
			}
			acquire := testStartupAcquireRequest("selected-grant-process")
			capability, err := ports.AcquireProcessCapability(ctx, acquire)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := capability.Release(context.Background()); err != nil {
					t.Errorf("release process: %v", err)
				}
			})
			req := startupownership.SelectedForkGrantRequest{
				RuntimeInstanceID: acquire.RuntimeInstanceID,
				Binding: startupownership.SelectedForkGrantBinding{
					BindingID: binding.BindingID, ForkRunID: fixture.forkRun, ExecutionID: issued.ExecutionID,
					ExecutionGeneration: issued.Generation, FenceGeneration: authority.FenceGeneration,
					ExecutionOwner: authority.ExecutionOwner, AdmissionFingerprint: issued.AdmissionFingerprint,
					ContainerPlanFingerprint: issued.ContainerPlanFingerprint, ActorCensusFingerprint: issued.ActorCensusFingerprint,
					EffectiveConfigFingerprint: issued.EffectiveConfigFingerprint,
					DeclarationPlanFingerprint: issued.DeclarationPlanFingerprint,
				},
			}
			if err := db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, fixture.forkRun).Scan(&req.BundleHash); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name   string
				mutate func(*startupownership.SelectedForkGrantRequest)
			}{
				{"binding", func(r *startupownership.SelectedForkGrantRequest) { r.Binding.BindingID = uuid.NewString() }},
				{"run", func(r *startupownership.SelectedForkGrantRequest) { r.Binding.ForkRunID = fixture.sourceRun }},
				{"execution", func(r *startupownership.SelectedForkGrantRequest) { r.Binding.ExecutionID = uuid.NewString() }},
				{"generation", func(r *startupownership.SelectedForkGrantRequest) { r.Binding.ExecutionGeneration++ }},
				{"fence", func(r *startupownership.SelectedForkGrantRequest) { r.Binding.FenceGeneration++ }},
				{"owner", func(r *startupownership.SelectedForkGrantRequest) { r.Binding.ExecutionOwner += "-other" }},
				{"admission", func(r *startupownership.SelectedForkGrantRequest) {
					r.Binding.AdmissionFingerprint = "sha256:" + strings.Repeat("4", 64)
				}},
				{"container", func(r *startupownership.SelectedForkGrantRequest) {
					r.Binding.ContainerPlanFingerprint = "sha256:" + strings.Repeat("4", 64)
				}},
				{"actors", func(r *startupownership.SelectedForkGrantRequest) {
					r.Binding.ActorCensusFingerprint = "sha256:" + strings.Repeat("4", 64)
				}},
				{"config", func(r *startupownership.SelectedForkGrantRequest) {
					r.Binding.EffectiveConfigFingerprint = "sha256:" + strings.Repeat("4", 64)
				}},
				{"declarations", func(r *startupownership.SelectedForkGrantRequest) {
					r.Binding.DeclarationPlanFingerprint = "sha256:" + strings.Repeat("4", 64)
				}},
				{"bundle", func(r *startupownership.SelectedForkGrantRequest) {
					r.BundleHash = "bundle-v2:sha256:" + strings.Repeat("4", 64)
				}},
				{"runtime", func(r *startupownership.SelectedForkGrantRequest) { r.RuntimeInstanceID = uuid.NewString() }},
			} {
				t.Run("reject_"+tc.name, func(t *testing.T) {
					bad := req
					tc.mutate(&bad)
					if _, err := capability.IssueSelectedForkGenerationGrant(ctx, bad); err == nil {
						t.Fatal("accepted mismatched authority")
					}
					var count int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generation_grants`).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatalf("rejected issuance persisted %d grant facts", count)
					}
				})
			}
			if _, exists, err := capability.CurrentSourceSet(ctx); err != nil || exists {
				t.Fatalf("expected no live source set: exists=%v err=%v", exists, err)
			}
			grant, err := capability.IssueSelectedForkGenerationGrant(ctx, req)
			if err != nil {
				t.Fatalf("issue without live source set: %v", err)
			}
			if _, live := grant.(startupownership.LiveGenerationGrant); live {
				t.Fatal("selected grant exposes live source-set authority")
			}
			if _, err := capability.IssueSelectedForkGenerationGrant(ctx, req); err == nil {
				t.Fatal("issued competing grant for the same selected execution")
			}
			for _, tc := range []struct {
				name      string
				statement string
				args      []any
			}{
				{"dual authority", `UPDATE runtime_generation_grants SET source_set_revision='live-revision'`, nil},
				{"missing authority", `UPDATE runtime_generation_grants SET selected_binding_id=NULL,selected_fork_run_id=NULL,selected_execution_id=NULL`, nil},
				{"missing binding", `UPDATE runtime_generation_grants SET selected_binding_id=$1`, []any{uuid.NewString()}},
				{"different fork", `UPDATE runtime_generation_grants SET selected_fork_run_id=$1`, []any{fixture.sourceRun}},
				{"different generation", `UPDATE runtime_generation_grants SET runtime_generation=runtime_generation+1`, nil},
			} {
				t.Run("ddl_rejects_"+tc.name, func(t *testing.T) {
					if _, err := db.ExecContext(ctx, tc.statement, tc.args...); err == nil {
						t.Fatal("DDL accepted inconsistent grant authority")
					}
					if err := grant.ProveCurrent(ctx); err != nil {
						t.Fatalf("rejected DDL mutation changed authority: %v", err)
					}
				})
			}
			for _, tc := range []struct {
				column string
				value  any
			}{
				{"fence_generation", req.Binding.FenceGeneration + 1},
				{"execution_owner", "another-owner"},
				{"admission_fingerprint", "sha256:" + strings.Repeat("7", 64)},
				{"container_plan_fingerprint", "sha256:" + strings.Repeat("7", 64)},
				{"actor_census_fingerprint", "sha256:" + strings.Repeat("7", 64)},
				{"effective_config_fingerprint", "sha256:" + strings.Repeat("7", 64)},
				{"declaration_plan_fingerprint", "sha256:" + strings.Repeat("7", 64)},
				{"declaration_plan", `{}`},
				{"lease_expires_at", time.Unix(1, 0).UTC()},
				{"state", "prepared"},
			} {
				t.Run("use_rechecks_"+tc.column, func(t *testing.T) {
					var original any
					if err := db.QueryRowContext(ctx, `SELECT `+tc.column+` FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, issued.ExecutionID).Scan(&original); err != nil {
						t.Fatal(err)
					}
					query := `UPDATE run_fork_selected_contract_runtime_executions SET ` + tc.column + `=$1 WHERE execution_id=$2`
					if _, err := db.ExecContext(ctx, query, tc.value, issued.ExecutionID); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if _, err := db.ExecContext(ctx, query, original, issued.ExecutionID); err != nil {
							t.Errorf("restore hostile fixture: %v", err)
						}
					})
					if err := grant.ProveCurrent(ctx); err == nil {
						t.Fatal("grant use ignored changed durable authority")
					}
					if _, err := grant.MarkProbesSettled(ctx, nil); err == nil {
						t.Fatal("grant transition ignored changed durable authority")
					}
					var count int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generation_grants`).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 1 {
						t.Fatalf("rejected use or competing issuance persisted %d grant facts, want 1", count)
					}
				})
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := grant.MarkProbesSettled(cancelled, nil); err == nil {
				t.Fatal("cancelled grant transition succeeded")
			}
			evidence, err := grant.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			evidence.SelectedFork.FenceGeneration++
			if err := grant.ProveCurrent(ctx); err != nil {
				t.Fatalf("copied evidence mutated capability: %v", err)
			}
			plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64)}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
				t.Fatal(err)
			}
			if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatalf("settle with unrelated live source set: %v", err)
			}
			if _, err := grant.AdmitExecution(ctx); err != nil {
				t.Fatalf("admit with unrelated live source set: %v", err)
			}
			if err := grant.ProveCurrent(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET fence_generation=fence_generation+1 WHERE execution_id=$1`, issued.ExecutionID); err != nil {
				t.Fatal(err)
			}
			if err := grant.ProveCurrent(ctx); err == nil {
				t.Fatal("stale execution fence retained authority")
			}
			if err := grant.Retire(ctx); err != nil {
				t.Fatalf("retire after execution fence changed: %v", err)
			}
			if err := grant.ProveCurrent(ctx); err == nil {
				t.Fatal("retired grant retained authority")
			}
			var state string
			if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, evidence.GrantID).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if state != "retired" {
				t.Fatalf("grant head = %q", state)
			}
			if _, err := capability.IssueSelectedForkGenerationGrant(ctx, req); err == nil {
				t.Fatal("retired execution grant was adopted by a new grant")
			}
		})
	}
}
