package runtimepersistence

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

// Component proof of strict readback inside the real selected publication
// transaction. Pending construction deliveries do not imply executed actors.
func proveSelectedForkInitialReadbackGeometries(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, geometry := range []string{"materializing_agent_only", "initialized_node_only", "mixed"} {
			t.Run(backend.name+"/"+geometry, func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				req := newSelectedForkAtomicityRequest(t, ctx, fixture, true)
				event := req.Commit.Event.Event()
				agent := testAgentDeliveryRoute(t, event.RunID(), "worker", "")
				target := events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: event.RunID(), EntityID: event.RunID()})
				initialization, err := events.AdmitFlowReceiverInitialization(event, target, agent.AgentIdentity.Route)
				if err != nil {
					t.Fatal(err)
				}
				agent.Target, agent.Initialization = target, initialization
				node := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("worker")), Target: target, Initialization: initialization}
				routes := []events.DeliveryRoute{agent, node}
				if geometry == "materializing_agent_only" {
					routes = routes[:1]
				} else if geometry == "initialized_node_only" {
					routes = routes[1:]
				}
				event, _, err = runtimebus.ResolvePreparedPublishEventTargetProjection(event, routes)
				if err != nil {
					t.Fatal(err)
				}
				req.Commit.Event, err = events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
				if err != nil || events.ValidateReceiverMaterializations(event, routes) != nil {
					t.Fatalf("exact component construction routes: %+v %v", routes, err)
				}
				req.Commit.DeliveryRoutes = routes
				setSelectedForkAtomicitySettlement(&req)
				store := fixture.store.(eventRecordContractStore)
				committed, err := store.CommitSelectedForkEvent(ctx, req)
				if err != nil || committed.AppendOutcome != runtimebus.EventAppendInserted || len(committed.DeliveryHandoffs) != len(routes) {
					t.Fatalf("initial strict %s readback: %+v err=%v", geometry, committed, err)
				}
				stored, found, err := fixture.store.(runtimebus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, event.ID())
				if err != nil || !found || stored.Validate() != nil || !receiverPublicationRoutesEqual(routes, stored.DeliveryRoutes) || !reflect.DeepEqual(event.NormalizedEnvelope(), stored.Event.Event().NormalizedEnvelope()) {
					t.Fatalf("durable strict %s readback: %+v found=%t err=%v", geometry, stored, found, err)
				}
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				duplicate, err := store.CommitSelectedForkEvent(ctx, req)
				if err != nil || duplicate.AppendOutcome != runtimebus.EventAppendExactDuplicate || len(duplicate.DeliveryHandoffs) != 0 {
					t.Fatalf("duplicate repeated %s work: %+v err=%v", geometry, duplicate, err)
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("duplicate %s mutated delivery/lineage/history: %v", geometry, err)
				}
			})
		}
	}
}
