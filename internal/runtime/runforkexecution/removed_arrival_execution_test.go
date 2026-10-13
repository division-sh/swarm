package runforkexecution

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestIssue642RemovedPreparedArrivalRuleSettlesDependentsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			t.Setenv(publishedJoinFlowEnv, "orders")
			var selected startupownership.Store
			var owner SelectedContractExecutionOwner
			var construct func() SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
				construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
			} else {
				_, db, _ := testutil.StartPostgres(t)
				s := storetest.AdmitPostgresRuntimeStore(t, db)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
				construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
			}
			ctx := runForkTestContext(t)
			process, _ := worklifetime.ProcessFromContext(ctx)
			baseline := process.ActiveCount()
			t.Cleanup(func() {
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Error(err)
				}
			})
			repo := runForkExecutionRepoRoot(t)
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: armedArrivalFixture(t), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
			original, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			ctx = correlation.WithSourceArtifactFact(ctx, original.SourceArtifactFact)
			scope, err := authoractivity.BundleScopeForTarget(ctx, original.SourceArtifactFact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			ctx = authoractivity.WithScope(ctx, scope)
			descriptors, err := rootruntime.AuthorActivityEventDescriptors(original.Source)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := owner.ports.fork.RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(catalog.Release)
			sourceRun := uuid.NewString()
			initial, armed, sourceJoin := seedArmedArrivalSource(t, ctx, selected, owner, original, sourceRun)
			marker, sourceSchedule := prepareArmedArrivalSourceAtCut(t, ctx, selected, owner, original, sourceRun, initial, armed, sourceJoin)
			entry := sourceJoin.JoinRef().StageEntry()
			sourceOwner := flowidentity.RunScopedFlowInstance{RunID: sourceRun, Route: flowidentity.StoredRoute(entry.FlowScope, entry.InstanceID, entry.InstancePath)}
			receipt, err := owner.ports.workflow.LoadFlowConstructionPublication(ctx, sourceOwner, entry.EntityID)
			if err != nil || receipt.Identity.ParentEntityID != sourceRun || receipt.CreatingInput.EventID == "" {
				t.Fatalf("removed-rule source lacks genuine constructor/parent: %+v err=%v", receipt, err)
			}
			plan, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: marker})
			if err != nil || len(plan.JoinSchedules) != 1 || plan.JoinSchedules[0].CurrentEventID != sourceSchedule.CurrentEventID ||
				plan.JoinSchedules[0].Status != genericschedule.StatusActive || len(plan.PendingWork) != 0 {
				t.Fatalf("removed-rule source lost exact prepared arm: %+v err=%v", plan, err)
			}
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
			if err != nil {
				t.Fatal(err)
			}
			// The selected contract removes the declaring rule, not the receiver.
			// Its retained current stage is final so no other work masks settlement.
			if err := os.Remove(filepath.Join(loader.SourceRoot, "orders", "nodes.yaml")); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(loader.SourceRoot, "orders", "schema.yaml")
			raw, err := os.ReadFile(path)
			if err != nil || strings.Count(string(raw), "  awaiting: {}\n") != 1 {
				t.Fatalf("removed-rule fixture lost its exact retained stage: %v", err)
			}
			raw = []byte(strings.Replace(string(raw), "  awaiting: {}\n", "  awaiting: {final: true}\n", 1))
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			target, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			if target.SourceArtifactFact.BundleHash() == original.SourceArtifactFact.BundleHash() || len(target.Source.WorkflowJoins()) != 0 {
				t.Fatal("selected contract did not remove the declaring join")
			}
			bundle, found := semanticview.Bundle(target.Source)
			if !found || bundle.SourceArtifact == nil {
				t.Fatal("removed rule requires a genuine selected artifact")
			}
			storetest.RequireBundleDataCatalog(t, correlation.WithSourceArtifactFact(ctx, target.SourceArtifactFact), selected.(storetest.DurableDataCatalogStore), bundle)
			selection := runforkadmission.SelectedContractSelection(target.Source)
			operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "removed-arrival", IdempotencyKey: "fixed-cut",
				TransportHash: "removed-arrival-transport", SourceRunID: sourceRun, ForkEventID: marker,
				TargetBundleHash: target.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: sourceRun, At: marker, AllowSourceFreeze: true, Owner: owner, ForkOperation: &operation,
				SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability},
			})
			if err != nil || !result.Activation.Activated {
				t.Fatalf("removed arrival rule must cancel, not refuse the fork: %+v err=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			operations := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			ack, found, err := operations.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || ack.Status != runfork.ForkOperationActivated || ack.ForkRunID != child || ack.Result == nil {
				t.Fatalf("removed-rule fork lacks permanent acknowledgment: %+v found=%v err=%v", ack, found, err)
			}
			wait, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			reader := selected.(interface {
				LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
			})
			for {
				header, err := reader.LoadRunHeader(wait, child)
				if err != nil || header.BundleHash != target.SourceArtifactFact.BundleHash() || header.Failure != nil {
					t.Fatalf("removed-rule child failed: %+v err=%v", header, err)
				}
				owner.ports.contexts.mu.Lock()
				retained := len(owner.ports.contexts.entries)
				owner.ports.contexts.mu.Unlock()
				if header.Status == "completed" && header.EndedAt != nil && process.ActiveCount() == baseline && retained == 0 {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("removed rule left dependent work alive: %+v contexts=%d leases=%d baseline=%d", header, retained, process.ActiveCount(), baseline)
				case <-time.After(10 * time.Millisecond):
				}
			}
			projected, err := runfork.ProjectConstructionIdentity(sourceRun, child, receipt.Identity)
			if err != nil {
				t.Fatal(err)
			}
			header, found, err := owner.ports.workflow.LoadWorkflowInstance(wait, flowidentity.RunScopedFlowInstance{RunID: child, Route: projected.Route()})
			if err != nil || !found || header.CurrentState != "awaiting" || header.EntityID != projected.EntityID ||
				header.ParentFlowInstance != child || header.ParentEntityID != child || header.WorkflowVersion != target.Source.WorkflowVersion() {
				t.Fatalf("rule removal changed the retained receiver/parent: %+v found=%v err=%v", header, found, err)
			}
			buckets, err := joinruntime.PersistedBuckets(header.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			joins, err := joinruntime.List(buckets)
			if err != nil || len(joins) != 1 {
				t.Fatalf("rule removal lost retained arm evidence: %+v err=%v", joins, err)
			}
			arm := joins[0]
			if arm.Status != joinruntime.StatusClosed || string(arm.CloseReason) != "rule_removed" || !arm.TimerCancelled ||
				arm.OutcomePending || arm.OutcomeFired || !reflect.DeepEqual(arm.Members, sourceJoin.Members) ||
				!arm.ArmedAt.Equal(sourceJoin.ArmedAt) || !arm.DeadlineAt.Equal(sourceJoin.DeadlineAt) || arm.TransferredPublication != nil {
				t.Fatalf("removed rule lacks dependent typed settlement: %+v", arm)
			}
			// No child business event is invented for cancellation. Its born revision
			// is therefore addressed through the existing exclusive start cut.
			childPlan, err := owner.ports.fork.PlanRunFork(wait, runfork.RunForkPlanRequest{SourceRunID: child, AtStart: true})
			if err != nil || len(childPlan.JoinSchedules) != 1 || len(childPlan.PendingWork) != 0 || len(childPlan.TransferredJoins) != 0 {
				t.Fatalf("removed rule cannot reconstruct its settled cut: %+v err=%v", childPlan, err)
			}
			canceled := childPlan.JoinSchedules[0]
			if canceled.Status != genericschedule.StatusCancelled || canceled.CancelCause != "rule_removed" || canceled.CurrentEventID != "" ||
				!canceled.CurrentEventAdmittedAt.IsZero() || !canceled.CancelledAt.Equal(canceled.AdmittedAt) || !canceled.CurrentDueAt.Equal(sourceSchedule.CurrentDueAt) ||
				canceled.ForkJoinOrigin == nil || canceled.ForkJoinOrigin.SourceActivationID != sourceSchedule.ID {
				t.Fatalf("removed rule lost its canceled schedule/cut: %+v", canceled)
			}
			for _, eventType := range []string{"platform.join_timeout", "platform.join_complete"} {
				if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, child, eventType); err != nil || count != 0 {
					t.Fatalf("removed rule emitted %s: count=%d err=%v", eventType, count, err)
				}
			}
			settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, child)
			if err != nil || settlement.Total != 0 {
				t.Fatalf("removed rule invented a delivery: %+v err=%v", settlement, err)
			}
			source, found, err := selected.(genericschedule.Store).LoadGenericScheduleActivation(correlation.WithRunID(ctx, sourceRun), sourceSchedule.ID)
			if err != nil || !found || !reflect.DeepEqual(source.Canonical(), sourceSchedule.Canonical()) {
				t.Fatalf("child removal changed source reservation: %+v found=%v err=%v", source, found, err)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(wait, selected, sourceRun)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("child removal changed source business evidence: %v", err)
			}
			repeated, found, err := operations.LoadForkOperation(wait, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || !reflect.DeepEqual(ack, repeated) {
				t.Fatalf("dependent settlement rewrote acknowledgment: %+v found=%v err=%v", repeated, found, err)
			}
			stateBefore, err := storetest.ReadSelectedExecutionStorage(wait, selected, child)
			if err != nil {
				t.Fatal(err)
			}
			terminalOwner := construct()
			if err := terminalOwner.BindSelectedProcess(ctx, process, owner.ports.contexts.capability); err != nil {
				t.Fatal(err)
			}
			terminal, err := terminalOwner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{})
			if err != nil || len(terminal) != 1 || terminal[0].RunID != child || terminal[0].Disposition != runfork.SelectedForkRecoveryTerminal {
				t.Fatalf("removed-rule terminal recovery reopened work: %+v err=%v", terminal, err)
			}
			stateAfter, err := storetest.ReadSelectedExecutionStorage(wait, selected, child)
			if err != nil || !reflect.DeepEqual(stateBefore, stateAfter) || process.ActiveCount() != baseline {
				t.Fatalf("removed-rule terminal recovery mutated state or retained resources: err=%v", err)
			}
		})
	}
}
