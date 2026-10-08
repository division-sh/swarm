package bus

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
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
				base := semanticviewtest.WithProviderIngress(semanticview.Wrap(connectRoutePlanTestBundle(t, flows, connections)), map[string][]string{origin: {"inbound.telegram"}})
				catalog, batch := authenticatedTelegramBatchForSource(t, base, origin, true)
				authorization := batch.Events[1].Authorization
				source := providerOutputAuthorizedTestSource{Source: base, declaringFlow: origin, generation: catalog.Generation(), authorizations: []runtimeprovideroutput.Authorization{authorization}}
				runID := batch.Events[1].Event.RunID()
				store := newTargetRouteMemoryStore()
				var owners []ActiveTargetDescriptor
				for _, flow := range []string{".", "alpha", "first", "second"} {
					instance := ConstructedFlowInstanceIdentityFixture(source, flow, "", runID)
					owners = append(owners, ActiveTargetDescriptor{ID: flow, FlowInstance: instance.InstancePath, EntityID: instance.EntityID})
				}
				store.setTargetOwners(owners...)
				eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, ProviderOutputVerifier: catalog})
				if err != nil {
					t.Fatal(err)
				}
				for _, flow := range []string{".", "alpha", "first", "second"} {
					installConnectionSourceConstructionForRun(t, eb, source, flow, runID)
				}
				evt := batch.Events[1].Event
				ctx := testAuthorActivityContext(context.Background())
				plan, err := eb.PrepareInboundDeliveryBatch(ctx, batch)
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
						path := node.FlowPath()
						if path == "." {
							path = runID
						}
						if target := route.Target.Route(); target.EntityID != runtimeflowidentity.EntityID(path) {
							t.Fatalf("receiver reused source entity: %#v", target)
						}
					}
					sort.Strings(got)
					sort.Strings(want)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("recipients=%v want=%v", got, want)
					}
				}
				prepared := plan.PreparedPublications()
				if len(prepared) != 2 || len(prepared[0].plan.DeliveryRoutes()) != 0 {
					t.Fatalf("raw/normalized outputs changed: %+v", prepared)
				}
				assertRoutes(prepared[1].plan.DeliveryRoutes())
				var committed []CommittedPublication
				for _, command := range plan.CommitCommands() {
					result, err := store.CommitPublication(ctx, command)
					if err != nil {
						t.Fatal(err)
					}
					committed = append(committed, result)
				}
				prepared, err = eb.ApplyInboundDeliveryCommit(ctx, plan, committed)
				if err != nil {
					t.Fatal(err)
				}
				for _, publication := range prepared {
					if err := eb.DispatchPreparedPublish(ctx, publication); err != nil {
						t.Fatal(err)
					}
				}
				assertRoutes(store.routes[evt.ID()])
			})
		}
	}
}
