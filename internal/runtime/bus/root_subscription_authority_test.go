package bus

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/google/uuid"
)

func TestRootInputSourceLoadedConsumerCardinality(t *testing.T) {
	for _, mode := range []string{"static"} {
		for _, connected := range [][]string{nil, {"first"}, {"first", "second"}, {"second"}} {
			for _, local := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/edges=%s/local=%t", mode, strings.Join(connected, "+"), local), func(t *testing.T) {
					schema := "name: root\npins:\n  inputs:\n    - thing.created\n  outputs:\n    - thing.created\n"
					if len(connected) > 0 {
						schema += "connect:\n"
						for _, child := range connected {
							schema += "  - {event: thing.created, from: ., to: " + child + "}\n"
						}
					}
					node := "observer:\n  execution_type: system_node\n  subscribes_to: [thing.created]\n  event_handlers:\n    thing.created:\n      guard: {id: admit, check: true}\n"
					files := map[string]string{"schema.yaml": schema, "events.yaml": "thing.created:\n"}
					for _, child := range []string{"first", "second"} {
						files[child+"/schema.yaml"] = "name: " + child + "\n"
						for _, wired := range connected {
							if wired == child {
								files[child+"/schema.yaml"] += "pins:\n  inputs:\n    - thing.created\n"
								files[child+"/nodes.yaml"] = node
							}
						}
					}
					if local {
						files["nodes.yaml"] = node
					}
					source := semanticview.Wrap(loadTargetRouteTempBundle(t, files))
					store := newConnectRoutePlanStaticStore()
					store.setTargetOwnerRoutes(
						events.RouteIdentity{FlowID: ".", FlowInstance: busInternalTestRunID, EntityID: runtimeflowidentity.EntityID(busInternalTestRunID)},
						events.RouteIdentity{FlowID: "first", FlowInstance: "first", EntityID: runtimeflowidentity.EntityID("first")},
						events.RouteIdentity{FlowID: "second", FlowInstance: "second", EntityID: runtimeflowidentity.EntityID("second")},
					)
					eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
					if err != nil {
						t.Fatal(err)
					}
					installConnectionSourceConstruction(t, eb, source, ".")
					evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "thing.created", "", "", []byte("{}"), 0, busInternalTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
					plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
					if err != nil {
						t.Fatal(err)
					}
					want := len(connected)
					if local {
						want++
					}
					if len(plan.DeliveryRoutes) != want {
						t.Fatalf("got %d routes, want %d: %#v", len(plan.DeliveryRoutes), want, plan)
					}
					for _, route := range plan.DeliveryRoutes {
						node, _ := route.Recipient.Node()
						if (node.FlowPath() == ".") != route.ConnectClaim.Empty() {
							t.Fatalf("local/connected authority confused: %#v", route)
						}
					}
					if err := eb.Publish(context.Background(), evt); err != nil {
						t.Fatal(err)
					}
					if len(store.routes[evt.ID()]) != want {
						t.Fatalf("persisted cardinality=%d want=%d", len(store.routes[evt.ID()]), want)
					}
				})
			}
		}
	}
}

func TestProviderLocalConsumptionUsesOnlyExactSameInstanceOwner(t *testing.T) {
	bundle := routedRootInputFlowNodeBundle()
	bundle.RootSchema.Pins.Inputs.EventPins = nil
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	source := semanticviewtest.WithProviderIngress(semanticview.Wrap(bundle), map[string][]string{"validation": {"thing.created"}})
	routes, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	consumers := routes.ResolveForRun(busInternalTestRunID, "validation/thing.created")
	if len(consumers) != 1 || consumers[0].RouteSourceCode() != "subscription" {
		t.Fatalf("provider has competing subscription authority: %#v", consumers)
	}
	for _, origin := range []string{"validation", "other"} {
		evt := eventtest.RunCreatingRootIngressWithRoutingSource(uuid.NewString(), "validation/thing.created", "provider", "", nil, 0, busInternalTestRunID, "", events.EventEnvelope{}, eventtest.StaticFlowRoutingSource(origin, origin, eventtest.UUID(origin)), time.Now().UTC())
		intents := routedExactSameInstanceNoTargetNodeDeliveryIntents(source, evt, consumers)
		want := 0
		if origin == "validation" {
			want = 1
		}
		if len(intents) != want {
			t.Fatalf("origin=%s intents=%#v want=%d", origin, intents, want)
		}
	}
}
