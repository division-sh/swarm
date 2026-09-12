package pipeline

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestEntityResetProjectionOwnershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, engine := range []string{"bridge", "declarative"} {
			for _, variant := range []string{"normal", "immutable", "nonempty"} {
				t.Run(backend+"/"+engine+"/"+variant, func(t *testing.T) {
					db, store := openHandlerEntityRequirementStore(t, backend)
					base := handlerEntityRequirementExecutionSource().(handlerEntityRequirementSemanticSource)
					bundle, _ := semanticview.Bundle(base.Source)
					bundle.RootTypes = rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"Item": {Fields: map[string]rc.TypeFieldSpec{"id": {Type: "text"}}}}}
					first := rc.EntityFieldDecl{Type: "[Item]", MaterializeFrom: "node-a.first_items"}
					if variant == "immutable" {
						first.Immutable = true
					}
					if variant == "nonempty" {
						// Reuse the authored refinement decoder, not a runtime-only constraint.
						loaded := loadWorkflowTempSource(t, map[string]string{"schema.yaml": "name: nonempty-reset\n", "types.yaml": "types:\n  Item:\n    id: text\n", "entities.yaml": "test_entity:\n  first:\n    type: '[Item]'\n    length: {min: 1}\n    materialize_from: node-a.first_items\n"})
						loadedBundle, _ := semanticview.Bundle(loaded)
						first = loadedBundle.RootEntities["test_entity"].Fields["first"]
					}
					bundle.RootEntities = rc.EntityContractsDocument{"test_entity": {Fields: map[string]rc.EntityFieldDecl{
						"first":  first,
						"second": {Type: "[Item]", IsOptional: true, MaterializeFrom: "node-a.second_items"},
						"peer":   {Type: "[Item]", MaterializeFrom: "peer.peer_items"},
					}}}
					bundle.FlowTree.Root.Nodes = map[string]rc.SystemNodeContract{
						"node-a": {ExecutionType: rc.SystemNodeExecutionType, StateSchema: rc.NodeStateSchema{Fields: []rc.NodeStateField{{Name: "first_items", Type: "[Item]"}, {Name: "second_items", Type: "[Item]"}}}, EventHandlers: map[string]rc.SystemNodeEventHandler{
							"work.ready":  {Accumulate: &rc.AccumulateSpec{Into: "first_items"}},
							"work.second": {Accumulate: &rc.AccumulateSpec{Into: "second_items"}},
						}},
						"peer": {ExecutionType: rc.SystemNodeExecutionType, StateSchema: rc.NodeStateSchema{Fields: []rc.NodeStateField{{Name: "peer_items", Type: "[Item]"}}}, EventHandlers: map[string]rc.SystemNodeEventHandler{"work.peer": {Accumulate: &rc.AccumulateSpec{Into: "peer_items"}}}},
					}
					bundle.FlowTree.Root.Events["work.emitted"] = rc.EventCatalogEntry{Payload: rc.EventPayloadSpec{
						Properties: map[string]rc.EventFieldSpec{"first": {Type: "[Item]"}, "second": {Type: "[Item]"}},
						Required:   []string{"first", "second"},
					}}
					for _, event := range []string{"work.ready", "work.second", "work.peer"} {
						bundle.FlowTree.Root.Events[event] = rc.EventCatalogEntry{Payload: rc.EventPayloadSpec{
							Properties: map[string]rc.EventFieldSpec{"id": {Type: "text"}}, Required: []string{"id"},
						}}
					}
					source := handlerEntityRequirementSemanticSource{Source: semanticview.Wrap(bundle)}
					bus := &recordingPipelineBus{}
					pc := newDurablePipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store), PipelineObligations: unavailablePipelineTestObligationOwner{}})
					configureWorkflowLifecycleForTest(t, pc)
					configurePipelineTestDeliveryOwner(t, pc)
					var ctx context.Context
					if backend == "sqlite" {
						ctx = sqliteExactOnceRunContext(t, db)
					} else {
						ctx = testPipelineRunContext(t, db)
					}
					initial := map[string]any{
						"first":  []any{map[string]any{"id": "first"}},
						"second": []any{map[string]any{"id": "second"}},
						"peer":   []any{map[string]any{"id": "peer"}},
					}
					instance, _ := executeExistingOwnerBehavior(t, ctx, pc, engine, "seed-reset", rc.SystemNodeEventHandler{Guard: &rc.GuardSpec{Check: "true"}}, nil, initial, nil)
					owner := testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef)
					node := pipelineNode(t, ".", "node-a")
					target := events.RouteIdentity{FlowID: ".", FlowInstance: instance.StorageRef, EntityID: instance.EntityID}
					evt := handlerTestRootIngress(uuid.NewString(), "work.ready", "", "", []byte(`{"id":"reset"}`), 0, runtimeRunID(ctx), "", handlerTestWorkflowEnvelope(".", instance.StorageRef, instance.EntityID), time.Now().UTC())
					seedExactOnceEvent(t, store, ctx, evt)
					evt = eventtest.TargetRouted(evt, target)
					deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)})
					handler := rc.SystemNodeEventHandler{
						Clear: &rc.ClearSpec{Targets: []string{"accumulator_state"}}, AdvancesTo: "killed",
						Emit: rc.EmitSpec{Event: "work.emitted", Fields: map[string]rc.ExpressionValue{"first": rc.RefExpression("entity.first"), "second": rc.RefExpression("entity.second")}},
					}
					var err error
					if engine == "bridge" {
						_, err = pc.executeNodeContractHandler(deliveryCtx, node, handler, workflowTriggerContext{Event: evt, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(instance.StorageRef), instance.EntityID)}, false)
					} else {
						_, err = newCoordinatorHandlerExecutionEngine(pc, node).ExecuteHandlerSteps(deliveryCtx, handler, evt, "work.ready")
					}
					after, found, loadErr := store.Load(ctx, owner)
					if loadErr != nil || !found {
						t.Fatalf("load reset: found=%v err=%v", found, loadErr)
					}
					if variant != "normal" {
						wantError := "immutable"
						if variant == "nonempty" {
							wantError = "length must be >= 1"
						}
						if err == nil || !strings.Contains(err.Error(), wantError) {
							t.Fatalf("reset error=%v, want %s", err, wantError)
						}
						if err == nil || !reflect.DeepEqual(after.Fields, instance.Fields) || after.Revision != instance.Revision || after.CurrentState != instance.CurrentState || bus.outboxCount() != 0 {
							t.Fatalf("rejected reset escaped: err=%v before=%#v after=%#v publications=%d", err, instance, after, bus.outboxCount())
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					want := map[string]any{"first": []any{}, "second": []any{}, "peer": initial["peer"]}
					if !reflect.DeepEqual(after.Fields, want) || after.CurrentState != "killed" || bus.outboxCount() != 1 {
						t.Fatalf("reset state=%#v stage=%s publications=%d", after.Fields, after.CurrentState, bus.outboxCount())
					}
					var emitted map[string]any
					if err := json.Unmarshal(bus.outboxIntent(0).Event.Payload(), &emitted); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(emitted, map[string]any{"first": initial["first"], "second": initial["second"]}) {
						t.Fatalf("emit did not preserve pre-reset values: %#v", emitted)
					}
				})
			}
		}
	}
}
