package pipeline_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Real recursive construction and both selected-store readers are exercised;
// this is admission proof, not installed-topology or public execution proof.
func TestWorkflowJoinConstructedDescendantAdmissionOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			runID := uuid.NewString()
			files := a2ActivationJoinFiles(1)
			files["orders/child/schema.yaml"] = "name: child\nstages:\n  awaiting: {}\n"
			files["orders/child/entities.yaml"] = "child_state:\n  final_count: {type: integer, initial: -1}\n"
			files["orders/child/events.yaml"] = "item.completed:\n  member_id: text\n  result: text\n"
			files["orders/child/nodes.yaml"] = files["orders/nodes.yaml"]
			source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, files))
			bundle, _ := semanticview.Bundle(source)
			fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContextForSource(t, context.Background(), fact), runID))
			runOwner, ok := selected.events.(storetest.RunFixtureStore)
			if !ok {
				t.Fatal("descendant native fixture requires the original selected run owner")
			}
			if err := storetest.MaterializeRun(ctx, runOwner, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact, BundleHash: fact.BundleHash()}); err != nil {
				t.Fatal(err)
			}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, WorkOwner: pipelineExternalTestWorkOwnerForSource(t, fact)}, "platform.join_complete", "platform.join_timeout")
			if err != nil {
				t.Fatal(err)
			}
			pc := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: proposedEffectProofModule{source: source}, SourceArtifactFact: fact})
			bus.SetInterceptors(pc)
			newManager, _ := a2ActivationJoinManagerFactory(t, ctx, selected, source)
			am := newManager(pc, bus)
			root := flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, runID, "")
			parent, err := flowidentity.KeyedChild(source, root, "orders", "one")
			if err != nil {
				t.Fatal(err)
			}
			at := time.Now().UTC().Truncate(time.Microsecond)
			constructA2StructuralRoot(t, ctx, am, bus, source, root, at)
			plan, err := am.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
				ContractBundle: source, Instance: parent, ConstructorInput: "order.created", ResolvedKey: "one",
				OccurredAt:   at,
				TriggerEvent: eventtest.ExistingRunRootIngress(uuid.NewString(), "order.created", "operator", "", []byte(`{"order_id":"one"}`), 0, runID, events.EventEnvelope{}, at),
			})
			if err != nil {
				t.Fatal(err)
			}
			committed, err := bus.CommitFlowInstanceActivation(ctx, plan)
			if err != nil || !committed.Acknowledged || !committed.Created || len(committed.Children) != 1 {
				t.Fatalf("real recursive construction: result=%#v err=%v", committed, err)
			}
			child := committed.Children[0].Plan.Identity
			if child.InstancePath != "orders/one/child" || child.ScopeKey != "orders/child" || child.ParentRoute.FlowInstance != parent.InstancePath {
				t.Fatalf("constructor lost concrete ancestor: %#v", child)
			}
			target := events.RouteIdentity{FlowID: child.TemplateID, FlowInstance: child.InstancePath, EntityID: child.EntityID}
			owner, err := pipeline.WorkflowJoinAdmissionOwner(source, runID, target)
			if err != nil || owner.Route != child.Route() {
				t.Fatalf("exact join lookup: owner=%#v err=%v", owner, err)
			}
			load := func() pipeline.WorkflowInstance {
				t.Helper()
				item, found, err := pc.LoadConstructedFlowInstance(ctx, owner, identity.EntityID(child.EntityID))
				if err != nil || !found {
					t.Fatalf("complete constructed reader: found=%v err=%v", found, err)
				}
				return item
			}
			before := load()
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(externalPipelineSourceNode(t, source, "orders/child", "collector")), Target: events.MustExistingEntityTarget(target)}
			receipts, fence, err := pipeline.PrepareWorkflowJoinAdmission(source, runID, "item.completed", route, &before)
			if err != nil || len(receipts) != 1 || receipts[0].Disposition != events.JoinAdmissionBound || fence == nil ||
				fence.Owner != owner || receipts[0].Ref.StageEntry().InstancePath != child.InstancePath ||
				receipts[0].Ref.StageEntry().FlowScope != child.ScopeKey || receipts[0].Ref.StageEntry().RunID != runID {
				t.Fatalf("constructed descendant admission: receipts=%#v fence=%#v err=%v", receipts, fence, err)
			}
			retained := route
			retained.Context.Joins = receipts
			if replay, fence, err := pipeline.PrepareWorkflowJoinAdmission(source, runID, "item.completed", retained, nil); err != nil || fence != nil || !reflect.DeepEqual(replay, receipts) {
				t.Fatalf("retained entry rebound: receipts=%#v fence=%#v err=%v", replay, fence, err)
			}
			corrupt := before
			corrupt.ParentFlowInstance = "orders/two"
			if _, _, err := pipeline.PrepareWorkflowJoinAdmission(source, runID, "item.completed", route, &corrupt); err == nil {
				t.Fatal("foreign construction parent admitted")
			}
			corrupt = before
			corrupt.EntityID = parent.EntityID
			if _, _, err := pipeline.PrepareWorkflowJoinAdmission(source, runID, "item.completed", route, &corrupt); err == nil {
				t.Fatal("foreign entity admitted")
			}
			if !reflect.DeepEqual(before, load()) {
				t.Fatal("read/admission mutated constructed receiver")
			}
		})
	}
}
