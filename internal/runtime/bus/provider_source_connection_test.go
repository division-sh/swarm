package bus

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// This isolates routing with existing selected owners. Gateway authentication and
// standing activation are exercised by the serveapp two-store matrix.
func TestExternalIngressConnectionOnlyChangesAuthorizedBranch(t *testing.T) {
	authorization := inboundBatchCurrentAuthorization()
	eventName := authorization.Event()
	for _, origin := range []string{".", "alpha"} {
		for _, receiver := range []string{"", "first", "second", "."} {
			if origin == receiver {
				continue
			}
			t.Run(origin+"->"+receiver, func(t *testing.T) {
				var flows []connectRoutePlanTestFlow
				for _, flow := range []string{".", "alpha", "first", "second"} {
					flows = append(flows, connectRoutePlanTestFlow{
						id: flow, mode: "static",
						inputs:  []runtimecontracts.FlowInputEventPin{{Event: eventName}},
						outputs: []runtimecontracts.FlowOutputEventPin{{Event: eventName}},
						nodes: map[string]runtimecontracts.SystemNodeContract{"observer": {
							SubscribesTo:  []string{eventName},
							EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{eventName: {}},
						}},
					})
				}
				// Only this edge changes. All declared consumers, pins, and owners stay fixed.
				var connections []runtimecontracts.FlowConnect
				if receiver != "" {
					connections = []runtimecontracts.FlowConnect{{Event: eventName, From: origin, To: receiver}}
				}
				source := providerOutputAuthorizedTestSource{
					Source:        semanticview.Wrap(connectRoutePlanTestBundle(t, flows, connections)),
					declaringFlow: origin, generation: authorization.Generation(),
					authorizations: []runtimeprovideroutput.Authorization{authorization},
				}
				runID := eventtest.UUID("provider-connection-run")
				store := newTargetRouteMemoryStore()
				var owners []ActiveTargetDescriptor
				for _, flow := range []string{".", "alpha", "first", "second"} {
					instance := flow
					if flow == "." {
						instance = runID
					}
					owners = append(owners, ActiveTargetDescriptor{ID: flow, FlowInstance: instance, EntityID: eventtest.UUID(flow)})
				}
				store.setTargetOwners(owners...)
				eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
				if err != nil {
					t.Fatal(err)
				}
				routing, err := events.NewExternalIngressRoutingSource(origin, eventtest.UUID(origin), events.RoutingSourceAuthorityProviderAdmissionPlan)
				if err != nil {
					t.Fatal(err)
				}
				evt := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID(t.Name()), events.EventType(eventName), "gateway", "", []byte("{}"), 0, runID, events.EventEnvelope{}, routing, time.Now())
				ctx := withProviderOutputAuthorization(context.Background(), authorization)
				plan, err := eb.CheckPublishRecipientPlan(ctx, evt)
				if err != nil {
					t.Fatal(err)
				}
				want := []string{origin}
				if receiver != "" {
					want = append(want, receiver)
				}
				assertRoutes := func(routes []events.DeliveryRoute) {
					t.Helper()
					var got []string
					for _, route := range routes {
						node, ok := route.Recipient.Node()
						if !ok {
							t.Fatalf("not a node route: %#v", route)
						}
						got = append(got, node.FlowPath())
						if route.ConnectClaim.Empty() != (node.FlowPath() == origin) {
							t.Fatalf("ordinary/compiled authority changed: %#v", route)
						}
						if target := route.Target.Route(); target.EntityID != eventtest.UUID(node.FlowPath()) {
							t.Fatalf("receiver reused source entity: %#v", target)
						}
					}
					sort.Strings(got)
					sort.Strings(want)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("recipients=%v want=%v", got, want)
					}
				}
				assertRoutes(plan.DeliveryRoutes)
				if err := eb.Publish(ctx, evt); err != nil {
					t.Fatal(err)
				}
				assertRoutes(store.routes[evt.ID()])
			})
		}
	}
}
