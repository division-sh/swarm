package bus

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestRootInputSourceLoadedConsumerCardinality(t *testing.T) {
	for _, mode := range []string{"static"} {
		for _, connected := range [][]string{nil, {"first"}, {"first", "second"}, {"second"}} {
			for _, local := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/edges=%s/local=%t", mode, strings.Join(connected, "+"), local), func(t *testing.T) {
					schema := "stages: []\npins:\n  inputs:\n    - thing.created\n  outputs:\n    - thing.created\n"
					if len(connected) > 0 {
						schema += "connect:\n"
						for _, child := range connected {
							schema += "  - {event: thing.created, from: ., to: " + child + "}\n"
						}
					}
					node := "observer:\n  execution_type: system_node\n  subscribes_to: [thing.created]\n  event_handlers:\n    thing.created:\n      guard: {id: admit, check: true}\n"
					files := map[string]string{
						"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
						"schema.yaml":   schema,
						"events.yaml":   "thing.created:\n",
					}
					for _, child := range []string{"first", "second"} {
						files[child+"/manifest.yaml"] = "version: 1.0.0\nplatform_version: '*'\n"
						files[child+"/schema.yaml"] = "stages: []\n"
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
					eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source,
						Durable: DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: busInternalTestRunID}},
					})
					if err != nil {
						t.Fatal(err)
					}
					for _, flow := range []string{".", "first", "second"} {
						installConnectionSourceConstruction(t, eb, source, flow)
					}
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
