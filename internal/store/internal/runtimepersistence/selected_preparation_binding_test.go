package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestSelectedPreparationExecutionBindingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedCompletionFixture(t, selected, db, sqlite)
			ctx := testAuthorActivityContext()
			name, err := agentidentity.DeclaredName("probe-agent", "selected-target")
			if err != nil {
				t.Fatal(err)
			}
			plan, err := agentidentity.NewPlan(name, agentidentity.RootRoute())
			if err != nil {
				t.Fatal(err)
			}
			planFingerprint, err := plan.Fingerprint()
			if err != nil {
				t.Fatal(err)
			}
			request := fixture.request
			request.ContainerPlanFingerprint = "sha256:" + strings.Repeat("1", 64)
			request.ActorCensusFingerprint = "sha256:" + strings.Repeat("2", 64)
			request.EffectiveConfigFingerprint = "sha256:" + strings.Repeat("3", 64)
			request.DeclarationPlan, err = agenttopology.NewSelectedDeclarationPlan(request.DeclarationPlan.BundleHash, []agenttopology.DesiredAgent{{
				Identity: plan, Source: agenttopology.SourceCoordinate{BundleHash: request.DeclarationPlan.BundleHash}, ConfigRevision: strings.Repeat("f", 64),
			}})
			if err != nil {
				t.Fatal(err)
			}
			request.Preparation.DeclarationPlanFingerprint = request.DeclarationPlan.Revision
			surface, err := managedcapabilities.New(managedcapabilities.Plan{
				ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "claude_cli", Transport: "cli", ProviderContract: "binding-proof", CreatedAt: time.Now().UTC(),
				Authority: managedcapabilities.Authority{
					Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(),
					ExecutionKind: managedcapabilities.ExecutionSelectedForkPreparation, ExecutionAuthorityID: request.Preparation.PreparationID,
					Preparation: &managedcapabilities.PreparedSelectedForkProbeAuthority{
						SelectedForkPreparationCoordinates: request.Preparation.Coordinates, ActorPlanFingerprint: planFingerprint,
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := selected.(managedcapabilities.Persistence).SaveManagedCapabilitySurface(ctx, surface); err != nil {
				t.Fatal(err)
			}
			request.Preparation.Actors = []runfork.SelectedForkPreparedActor{{
				Plan: plan, ConfigurationRevision: strings.Repeat("f", 64), Backend: selection.BackendClaudeCLI, Mode: executionmode.Live,
				SurfaceID: surface.ID, SurfaceIntegrity: surface.IntegrityHash,
			}}
			for _, test := range []struct {
				name   string
				change func(*runfork.SelectedForkPreparationBinding)
			}{
				{"preparation", func(p *runfork.SelectedForkPreparationBinding) { p.PreparationID = uuid.NewString() }},
				{"process", func(p *runfork.SelectedForkPreparationBinding) { p.Coordinates.ProcessAuthorityID = uuid.NewString() }},
				{"owner", func(p *runfork.SelectedForkPreparationBinding) { p.Coordinates.ProcessOwnerID += "-foreign" }},
				{"boot", func(p *runfork.SelectedForkPreparationBinding) { p.Coordinates.ProcessBootID = uuid.NewString() }},
				{"generation", func(p *runfork.SelectedForkPreparationBinding) { p.ProcessGeneration++ }},
				{"bundle", func(p *runfork.SelectedForkPreparationBinding) {
					p.Coordinates.BundleHash = "bundle-v2:sha256:" + strings.Repeat("9", 64)
				}},
				{"source_run", func(p *runfork.SelectedForkPreparationBinding) { p.SourceRunID = uuid.NewString() }},
				{"fork_run", func(p *runfork.SelectedForkPreparationBinding) { p.ForkRunID = uuid.NewString() }},
				{"event", func(p *runfork.SelectedForkPreparationBinding) { p.ForkEventID = uuid.NewString() }},
				{"declarations", func(p *runfork.SelectedForkPreparationBinding) {
					p.DeclarationPlanFingerprint = "sha256:" + strings.Repeat("9", 64)
				}},
				{"source", func(p *runfork.SelectedForkPreparationBinding) {
					p.Coordinates.SourceFingerprint = strings.Repeat("9", 64)
				}},
				{"plan", func(p *runfork.SelectedForkPreparationBinding) {
					p.Coordinates.AdmittedPlanFingerprint = strings.Repeat("9", 64)
				}},
				{"configuration", func(p *runfork.SelectedForkPreparationBinding) {
					p.Coordinates.ConfigurationFingerprint = strings.Repeat("9", 64)
				}},
				{"catalog", func(p *runfork.SelectedForkPreparationBinding) {
					p.Coordinates.CatalogFingerprint = strings.Repeat("9", 64)
				}},
				{"actor_owner", func(p *runfork.SelectedForkPreparationBinding) {
					p.Actors[0].Plan.Name.Owner = "same-name-foreign-owner"
				}},
				{"receipt", func(p *runfork.SelectedForkPreparationBinding) { p.Actors[0].SurfaceID = uuid.NewString() }},
				{"integrity", func(p *runfork.SelectedForkPreparationBinding) {
					p.Actors[0].SurfaceIntegrity = strings.Repeat("9", 64)
				}},
				{"missing_receipt", func(p *runfork.SelectedForkPreparationBinding) { p.Actors[0].SurfaceID = "" }},
				{"missing_census", func(p *runfork.SelectedForkPreparationBinding) { p.Actors = nil }},
				{"duplicate_actor", func(p *runfork.SelectedForkPreparationBinding) { p.Actors = append(p.Actors, p.Actors[0]) }},
				{"native_receipt", func(p *runfork.SelectedForkPreparationBinding) { p.Actors[0].Backend = selection.BackendAnthropic }},
			} {
				t.Run("reject_"+test.name, func(t *testing.T) {
					bad := request
					bad.Preparation.Actors = append([]runfork.SelectedForkPreparedActor{}, request.Preparation.Actors...)
					test.change(&bad.Preparation)
					if _, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, bad); err == nil {
						t.Fatal("crossed preparation admitted")
					}
					var count int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions`).Scan(&count); err != nil || count != 0 {
						t.Fatalf("rejected preparation mutated executions: %d %v", count, err)
					}
				})
			}
			issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			var raw, fingerprint string
			if err := db.QueryRowContext(ctx, `SELECT preparation_binding, preparation_fingerprint FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, issued.ExecutionID).Scan(&raw, &fingerprint); err != nil {
				t.Fatal(err)
			}
			var persisted runfork.SelectedForkPreparationBinding
			if err := json.Unmarshal([]byte(raw), &persisted); err != nil {
				t.Fatal(err)
			}
			want, err := persisted.Fingerprint()
			if err != nil || want != fingerprint || fingerprint != issued.PreparationFingerprint || !reflect.DeepEqual(persisted, request.Preparation) {
				t.Fatalf("preparation readback changed: %v", err)
			}
			for _, invalid := range []any{nil, "", "sha256:" + strings.Repeat("a", 63), "sha256:" + strings.Repeat("G", 64), "sha256:" + strings.Repeat("A", 64)} {
				if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET preparation_fingerprint=$1 WHERE execution_id=$2`, invalid, issued.ExecutionID); err == nil {
					t.Fatalf("DDL accepted %v", invalid)
				}
			}
			badClaim := issued
			badClaim.PreparationFingerprint = "sha256:" + strings.Repeat("9", 64)
			if _, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, badClaim, "wrong-preparation", time.Minute); err == nil {
				t.Fatal("claim ignored preparation identity")
			}
			if _, err := db.ExecContext(ctx, `DELETE FROM managed_agent_capability_surfaces WHERE surface_id=$1`, surface.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "missing-receipt", time.Minute); err == nil {
				t.Fatal("claim retained executable authority after losing its preparation receipt")
			}
			var refusedState, refusedOwner string
			if err := db.QueryRowContext(ctx, `SELECT state, execution_owner FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, issued.ExecutionID).Scan(&refusedState, &refusedOwner); err != nil || refusedState != "prepared" || refusedOwner != issued.ExecutionOwner {
				t.Fatalf("refused claim changed execution: %s %s %v", refusedState, refusedOwner, err)
			}
			if err := selected.(managedcapabilities.Persistence).SaveManagedCapabilitySurface(ctx, surface); err != nil {
				t.Fatal(err)
			}
			authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "bound-preparation", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			ports := selected.(interface {
				RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
			})
			binding, err := ports.RequireRunForkSelectedContractBinding(ctx, fixture.forkRun)
			if err != nil {
				t.Fatal(err)
			}
			process, err := fixture.process.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			grant, err := fixture.process.IssueSelectedForkGenerationGrant(ctx, startupownership.SelectedForkGrantRequest{
				RuntimeInstanceID: process.RuntimeInstanceID, BundleHash: request.DeclarationPlan.BundleHash,
				Binding: startupownership.SelectedForkGrantBinding{
					BindingID: binding.BindingID, ForkRunID: fixture.forkRun, ExecutionID: issued.ExecutionID,
					ExecutionGeneration: issued.Generation, FenceGeneration: authority.FenceGeneration, ExecutionOwner: authority.ExecutionOwner,
					AdmissionFingerprint: issued.AdmissionFingerprint, ContainerPlanFingerprint: issued.ContainerPlanFingerprint,
					ActorCensusFingerprint: issued.ActorCensusFingerprint, EffectiveConfigFingerprint: issued.EffectiveConfigFingerprint,
					DeclarationPlanFingerprint: issued.DeclarationPlanFingerprint, PreparationFingerprint: issued.PreparationFingerprint,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, ids := range [][]string{nil, {uuid.NewString()}, {surface.ID, surface.ID}, {" " + surface.ID}} {
				if _, err := grant.MarkProbesSettled(ctx, ids); err == nil {
					t.Fatalf("settled nonexact receipts %v", ids)
				}
				evidence, err := grant.Evidence()
				if err != nil || evidence.State != startupownership.GrantPrepared {
					t.Fatalf("refusal advanced grant: %+v %v", evidence, err)
				}
			}
			if _, err := grant.MarkProbesSettled(ctx, []string{surface.ID}); err != nil {
				t.Fatal(err)
			}
			if _, err := grant.AdmitExecution(ctx); err != nil {
				t.Fatal(err)
			}
			if err := grant.ProveCurrent(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET preparation_binding='{}' WHERE execution_id=$1`, issued.ExecutionID); err != nil {
				t.Fatal(err)
			}
			if err := grant.ProveCurrent(ctx); err == nil {
				t.Fatal("grant use ignored corrupt preparation")
			}
			if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET preparation_binding=$1 WHERE execution_id=$2`, raw, issued.ExecutionID); err != nil {
				t.Fatal(err)
			}
			if err := grant.ProveCurrent(ctx); err != nil {
				t.Fatal(err)
			}
			// Diagnostic deletion is hostile fixture corruption, not a supported
			// operation. Every authority use must still re-read original evidence.
			if _, err := db.ExecContext(ctx, `DELETE FROM managed_agent_capability_surfaces WHERE surface_id=$1`, surface.ID); err != nil {
				t.Fatal(err)
			}
			if err := grant.ProveCurrent(ctx); err == nil {
				t.Fatal("grant use retained missing startup evidence")
			}
			if err := grant.Retire(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
