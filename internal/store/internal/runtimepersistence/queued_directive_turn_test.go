package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestQueuedDirectiveTerminationBothStores(t *testing.T) {
	for _, mode := range []string{"pending", "rollback", "admission_wins", "isolated", "root"} {
		t.Run(mode, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				if mode == "root" {
					fixture = newCompletionSettlementFixtureForFlow(t, fixture.store, fixture.db, fixture.sqlite, agentmemory.Plan{}, "")
				}
				store := requireProviderDirectiveStore(t, fixture)
				op, event := reserveProviderDirectiveOperation(t, fixture, store, "queued-terminate")
				ctx := providerDirectiveBaseContext(t, fixture, event, "queued-terminate")
				scope, instance, path, err := fixture.authority.BusinessTurnCoordinates()
				if err != nil {
					t.Fatal(err)
				}
				owner := flowidentity.RunScopedFlowInstance{RunID: fixture.authority.Target.RunID, Route: flowidentity.StoredRoute(scope, instance, path)}
				var untouched []agentcontrol.DirectiveOperation
				if mode == "isolated" {
					sibling := mustTestAgentIdentityForRun(owner.RunID, fixture.agentID+"-sibling", path+"/sibling")
					if err := agentfixture.UpsertStatic(t, ctx, fixture.store, agentFixtureStaticRecord(t, sibling)); err != nil {
						t.Fatal(err)
					}
					siblingFixture := fixture
					siblingFixture.authority.Normal.Identity = sibling
					siblingFixture.authority.Target.AgentID, siblingFixture.authority.Target.AgentIdentity = sibling.AgentID(), sibling
					siblingFixture.authority.Target.FlowInstance = sibling.FlowInstance()
					other := newCompletionSettlementFixtureForFlow(t, fixture.store, fixture.db, fixture.sqlite, agentmemory.Plan{}, "completion")
					for _, untouchedFixture := range []completionSettlementFixture{siblingFixture, other} {
						reserved, _ := reserveProviderDirectiveOperation(t, untouchedFixture, store, "unrelated")
						before, found, err := store.LoadDirectiveOperation(ctx, reserved.OperationID)
						if err != nil || !found {
							t.Fatalf("load unrelated queued directive: found=%t err=%v", found, err)
						}
						untouched = append(untouched, before)
					}
				}
				req := agentcontrol.DirectiveExecutionAdmissionRequest{OperationID: op.OperationID, OwnerID: uuid.NewString(), Now: time.Now().UTC(), Lease: time.Minute, ExecutionPosture: executionposture.Live}
				if mode == "admission_wins" {
					if _, err := store.AdmitDirectiveExecution(ctx, req); err != nil {
						t.Fatal(err)
					}
				}
				committed, err := commitUnstartedDirectiveTermination(t, ctx, fixture, owner, mode == "rollback")
				if mode == "rollback" {
					if err == nil || committed.Committed || len(committed.Lifecycle.QueuedDirectiveCancellations) != 0 {
						t.Fatalf("rolled-back stage retained queued directive settlement: %+v err=%v", committed, err)
					}
					admitted, err := store.AdmitDirectiveExecution(ctx, req)
					if err != nil || admitted.Operation.State != agentcontrol.DirectiveOperationExecuting {
						t.Fatalf("rollback blocked queued directive: %+v err=%v", admitted, err)
					}
					return
				}
				if err != nil || !committed.Committed || committed.Lifecycle.Validate() != nil {
					t.Fatalf("queued terminate commit: %+v err=%v", committed, err)
				}
				for _, before := range untouched {
					after, found, err := store.LoadDirectiveOperation(ctx, before.OperationID)
					if err != nil || !found || !reflect.DeepEqual(before, after) {
						t.Fatalf("termination changed another instance/run's directive: before=%+v after=%+v err=%v", before, after, err)
					}
				}
				if mode == "admission_wins" {
					if len(committed.Lifecycle.QueuedDirectiveCancellations) != 0 {
						t.Fatal("admitted directive was settled as unexecuted")
					}
					found := false
					for _, intent := range committed.Lifecycle.TurnCancellations {
						if intent.Origin.Kind == effects.CompletionOriginDirective && intent.Origin.Directive.OperationID == op.OperationID && intent.Origin.Directive.ExecutionOwnerID == req.OwnerID {
							found = intent.ValidateIntent() == nil
						}
					}
					if !found {
						t.Fatal("admission winner lost exact cancellation/cleanup responsibility")
					}
					return
				}
				if len(committed.Lifecycle.QueuedDirectiveCancellations) != 1 {
					t.Fatalf("prepared directive was omitted: %+v", committed.Lifecycle)
				}
				canceled := committed.Lifecycle.QueuedDirectiveCancellations[0]
				if !canceled.Acknowledged || canceled.OperationID != op.OperationID || canceled.State != agentcontrol.DirectiveOperationCanceled || canceled.CancellationReason != deliverylifecycle.CancellationTerminate || canceled.ExecutionOwnerID != "" || !canceled.ExecutionAdmittedAt.IsZero() || canceled.Failure != nil || len(canceled.Response) != 0 {
					t.Fatalf("prepared cancellation invented execution or failure evidence: %+v", canceled)
				}
				if _, err := store.AdmitDirectiveExecution(ctx, req); err == nil {
					t.Fatal("canceled prepared directive admitted execution")
				}
				actual, found, err := store.LoadDirectiveOperation(context.WithoutCancel(ctx), op.OperationID)
				if err != nil || !found || actual.State != canceled.State || !actual.CompletedAt.Equal(canceled.CompletedAt) || actual.ExecutionOwnerID != "" {
					t.Fatalf("post-admission readback changed cancellation evidence: %+v err=%v", actual, err)
				}
			})
		})
	}
}
