package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	actors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestRunExecutionOwnershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedCompletionFixture(t, store, db, sqlite)
			ctx := testAuthorActivityContext()
			ports := store.(interface {
				startupownership.Store
				manager.AgentLifecycleStateReader
				RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
			})
			var hash string
			if err := db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, fixture.forkRun).Scan(&hash); err != nil {
				t.Fatal(err)
			}
			acquire := testStartupAcquireRequest("run-ownership")
			capability, err := ports.AcquireProcessCapability(ctx, acquire)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := capability.Release(context.Background()); err != nil {
					t.Error(err)
				}
			})
			plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: hash}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
				t.Fatal(err)
			}
			ordinary, err := capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{
				BundleHash: hash, RuntimeInstanceID: acquire.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
			})
			if err != nil {
				t.Fatal(err)
			}
			assertOwnership := func(owner manager.RunExecutionOwner, runID string, expected manager.RunExecutionOwnership) {
				t.Helper()
				actual, err := owner.InspectRunExecutionOwnership(ctx, runID)
				if err != nil || actual != expected {
					t.Fatalf("run %s ownership=%v want=%v err=%v", runID, actual, expected, err)
				}
			}
			assertOwnership(ordinary, fixture.sourceRun, manager.RunExecutionOwned)
			assertOwnership(ordinary, fixture.forkRun, manager.RunExecutionForeign)
			for _, runID := range []string{"", uuid.NewString(), " " + fixture.sourceRun} {
				if disposition, err := ordinary.InspectRunExecutionOwnership(ctx, runID); err == nil || disposition != 0 {
					t.Fatalf("missing/malformed run granted ownership: %v %v", disposition, err)
				}
			}

			// A valid ordinary grant must not adopt the selected child's existing
			// lifecycle cell, including before the selected execution is issued.
			identity := mustTestAgentIdentityForRun(fixture.forkRun, "selected-agent", "selected-test")
			before, found, err := ports.LoadAgentLifecycleState(ctx, identity)
			if err != nil || !found {
				t.Fatalf("load original lifecycle: %v %v", found, err)
			}
			topology, err := agenttopology.StaticAdmission(plan.Revision, hash, agenttopology.LifetimeDurableManaged)
			if err != nil {
				t.Fatal(err)
			}
			for _, operation := range []string{"spawn", "process_takeover", "source_set_rebind", "source_set_retire", "teardown"} {
				target := manager.AgentLifecycleRegistered
				if operation == "source_set_retire" || operation == "process_takeover" {
					target = manager.AgentLifecycleTerminated
				}
				_, err := ordinary.CommitAgentLifecycleTransition(ctx, manager.AgentLifecycleTransition{
					OperationID: uuid.NewString(), OperationKind: operation, RequestHash: uuid.NewString(),
					Identity: identity, AgentID: identity.AgentID(), Trigger: "ownership-test",
					ExpectedEpoch: before.RuntimeEpoch, ExpectedGeneration: before.Generation, ExpectedPhase: before.Phase,
					TargetEpoch: 1, TargetGeneration: before.Generation + 1, TargetPhase: target,
					ConfigRevision: strings.Repeat("a", 64), RunMode: manager.AgentRunModeStopped,
					Topology: topology, Now: time.Now().UTC(),
				})
				if !errors.Is(err, manager.ErrRunExecutionNotOwned) {
					t.Fatalf("%s bypass did not reach exact ownership refusal: %v", operation, err)
				}
				after, found, err := ports.LoadAgentLifecycleState(ctx, identity)
				if err != nil || !found || !reflect.DeepEqual(before, after) {
					t.Fatalf("%s changed selected lifecycle: before=%+v after=%+v err=%v", operation, before, after, err)
				}
			}

			fixture.request.ContainerPlanFingerprint = "sha256:" + strings.Repeat("1", 64)
			fixture.request.ActorCensusFingerprint = "sha256:" + strings.Repeat("2", 64)
			fixture.request.EffectiveConfigFingerprint = "sha256:" + strings.Repeat("3", 64)
			declaredIdentity := mustTestAgentIdentityForRun(fixture.forkRun, "declared-agent", "")
			config := withRuntimePersistenceTestIntent(t, actors.AgentConfig{
				ExecutionMode: "live", ID: declaredIdentity.AgentID(), Identity: declaredIdentity,
				Role: "worker", Type: "sonnet", Model: "regular", Memory: agentmemory.PlatformDefault(),
			})
			declaredIdentityPlan, err := declaredIdentity.Plan()
			if err != nil {
				t.Fatal(err)
			}
			configRevision, err := manager.AgentConfigPlanRevision(config, declaredIdentityPlan)
			if err != nil {
				t.Fatal(err)
			}
			fixture.request.DeclarationPlan, err = agenttopology.NewSelectedDeclarationPlan(hash, []agenttopology.DesiredAgent{{
				Identity: declaredIdentityPlan, ConfigRevision: configRevision, Source: agenttopology.SourceCoordinate{BundleHash: hash},
			}})
			if err != nil {
				t.Fatal(err)
			}
			issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "ownership-worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			binding, err := ports.RequireRunForkSelectedContractBinding(ctx, fixture.forkRun)
			if err != nil {
				t.Fatal(err)
			}
			selected, err := capability.IssueSelectedForkGenerationGrant(ctx, startupownership.SelectedForkGrantRequest{
				BundleHash: hash, RuntimeInstanceID: acquire.RuntimeInstanceID,
				Binding: startupownership.SelectedForkGrantBinding{
					BindingID: binding.BindingID, ForkRunID: fixture.forkRun, ExecutionID: issued.ExecutionID,
					ExecutionGeneration: issued.Generation, FenceGeneration: authority.FenceGeneration,
					ExecutionOwner: authority.ExecutionOwner, AdmissionFingerprint: issued.AdmissionFingerprint,
					ContainerPlanFingerprint: issued.ContainerPlanFingerprint, ActorCensusFingerprint: issued.ActorCensusFingerprint,
					EffectiveConfigFingerprint: issued.EffectiveConfigFingerprint,
					DeclarationPlanFingerprint: issued.DeclarationPlanFingerprint,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			assertOwnership(selected, fixture.forkRun, manager.RunExecutionOwned)
			assertOwnership(selected, fixture.sourceRun, manager.RunExecutionForeign)
			assertOwnership(ordinary, fixture.forkRun, manager.RunExecutionForeign)
			assertOwnership(ordinary, fixture.sourceRun, manager.RunExecutionOwned)
			selectedTopology, err := agenttopology.SelectedDeclarationAdmission(fixture.forkRun, fixture.request.DeclarationPlan)
			if err != nil {
				t.Fatal(err)
			}
			record := manager.PersistedAgent{Config: config, Topology: selectedTopology, Status: "active", HiredBy: "ownership-test"}
			mutation := manager.AgentLifecycleTransition{
				OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: uuid.NewString(),
				Identity: declaredIdentity, AgentID: declaredIdentity.AgentID(), Trigger: "selected-declaration-test",
				TargetEpoch: 1, TargetGeneration: 1, TargetPhase: manager.AgentLifecycleRegistered,
				ConfigRevision: configRevision, RunMode: manager.AgentRunModeStopped,
				Topology: selectedTopology, Agent: &record, Now: time.Now().UTC(),
			}
			if _, err := ordinary.CommitAgentLifecycleTransition(ctx, mutation); err == nil {
				t.Fatal("ordinary grant accepted selected declaration")
			}
			badRevision := mutation
			badRevision.ConfigRevision = strings.Repeat("f", 64)
			if _, err := selected.CommitAgentLifecycleTransition(ctx, badRevision); err == nil || !strings.Contains(err.Error(), "selected_declaration_config_mismatch") {
				t.Fatalf("wrong selected config revision: %v", err)
			}
			if _, found, err := ports.LoadAgentLifecycleState(ctx, declaredIdentity); err != nil || found {
				t.Fatalf("rejected selected declaration created lifecycle: %v %v", found, err)
			}
			if _, err := selected.CommitAgentLifecycleTransition(ctx, mutation); err != nil {
				t.Fatalf("selected declaration lifecycle: %v", err)
			}
			persisted, found, err := ports.LoadAgentLifecycleState(ctx, declaredIdentity)
			if err != nil || !found || !persisted.Topology.Equal(selectedTopology) || persisted.ConfigRevision != configRevision {
				t.Fatalf("selected declaration readback: %+v %v %v", persisted, found, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if disposition, err := selected.InspectRunExecutionOwnership(cancelled, fixture.forkRun); err == nil || disposition != 0 {
				t.Fatalf("cancelled inspection = %v %v", disposition, err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET fence_generation=fence_generation+1 WHERE execution_id=$1`, issued.ExecutionID); err != nil {
				t.Fatal(err)
			}
			if disposition, err := selected.InspectRunExecutionOwnership(ctx, fixture.forkRun); err == nil || disposition != 0 {
				t.Fatalf("fenced inspection = %v %v", disposition, err)
			}
			assertOwnership(ordinary, fixture.forkRun, manager.RunExecutionForeign)
			assertOwnership(ordinary, fixture.sourceRun, manager.RunExecutionOwned)
			if err := selected.Retire(ctx); err != nil {
				t.Fatal(err)
			}
			if disposition, err := selected.InspectRunExecutionOwnership(ctx, fixture.forkRun); err == nil || disposition != 0 {
				t.Fatalf("retired inspection = %v %v", disposition, err)
			}
		})
	}
}

func emptySelectedDeclarationForTest(t *testing.T, db *sql.DB, runID string) agenttopology.SelectedDeclarationPlan {
	t.Helper()
	var hash string
	if err := db.QueryRow(`SELECT bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSelectedDeclarationPlan(hash, nil)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
