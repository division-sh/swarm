package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestExecutorFanOutCapturesReceiverWithoutPublicationAuthority(t *testing.T) {
	for _, flowOwned := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "flow"}[flowOwned], func(t *testing.T) {
			exec, err := NewExecutor(RuntimeDependencies{
				Source: sourceWithFixtureStages(fanOutPayloadSource(t, "task.completed"), "flow-1", "pending", "pending"), StateRepo: stubStateRepo{},
				MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{}, PayloadShaper: stubPayloadShaper{},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			node := testFlowExecutableNode(t, "flow-1", "node-1")
			event := eventtest.ExistingRunRootIngress(eventtest.UUID("fan-out-receiver-event"), "task.completed", "operator", "", json.RawMessage(`{"items":["a","b"]}`), 0, eventtest.UUID("fan-out-receiver-run"), events.EventEnvelope{}, time.Now())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "flow-1", FlowInstance: "flow-1", EntityID: "entity-1"})}
			if flowOwned {
				route.Initialization, err = events.AdmitFlowReceiverInitialization(event, route.Target)
			} else {
				route.Initialization, err = events.AdmitNodeReceiverInitialization(event, route.Target, node)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(route)
			ctx := runtimedelivery.WithRoute(context.Background(), route)
			result, err := exec.ExecuteSemanticFixture(ctx, ExecutionRequest{
				EntityID: "entity-1", Node: node, Route: flowidentity.StoredRoute("flow-1", "flow-1", "flow-1"), Event: event,
				Handler: runtimecontracts.SystemNodeEventHandler{FanOut: &runtimecontracts.FanOutSpec{ItemsFrom: "payload.items", As: "fan_item", Identity: "fan_item", Emit: runtimecontracts.EmitSpec{Event: "item.process"}}},
				State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
			})
			if err != nil || result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 2 || len(result.EmitIntents) != 0 {
				t.Fatalf("real handler did not capture deferred work: %+v %v", result, err)
			}
			capsule := result.FanOutIntent.Capsule
			if capsule.Receiver == nil || capsule.Receiver.Node != node || capsule.Receiver.Target != route.Target || capsule.Validate() != nil || capsule.Lineage.ParentEventID != event.ID() {
				t.Fatalf("receiver capture changed ownership or lineage: %+v", capsule)
			}
			raw, err := json.Marshal(capsule)
			if err != nil || bytes.Contains(raw, []byte(`"receiver_initialization"`)) || bytes.Contains(raw, []byte(`"delivery_route"`)) {
				t.Fatalf("capture retained publication authority: %s %v", raw, err)
			}
			retained, ok := runtimedelivery.RouteFromContext(ctx)
			after, _ := json.Marshal(retained)
			if !ok || !bytes.Equal(before, after) {
				t.Fatal("capture changed original delivery context")
			}
		})
	}
}
