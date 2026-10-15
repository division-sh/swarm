package bus

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestReceiverPublicationPermutationPreservesExactEnvelope(t *testing.T) {
	runID := eventtest.UUID("2677-pure-run")
	eventID := eventtest.UUID("2677-pure-publication")
	target := events.RouteIdentity{FlowID: "review", FlowInstance: "review", EntityID: eventtest.UUID("2677-pure-header")}
	name, err := agentidentity.DeclaredName("worker", "swarm://review/worker")
	if err != nil {
		t.Fatal(err)
	}
	actor, err := agentidentity.New(runID, name, agentidentity.Route{Presence: agentidentity.RoutePresent, ScopeKey: "review", InstanceID: "review", InstancePath: "review"})
	if err != nil {
		t.Fatal(err)
	}
	construction, err := events.AdmitStoredFlowReceiverInitialization(runID, eventtest.UUID("2677-pure-creating"), events.MustExistingEntityTarget(target), actor.Route)
	if err != nil {
		t.Fatal(err)
	}
	actorTarget := target
	actorTarget.EntityID = ""
	agent := events.DeliveryRoute{
		Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: actor,
		Target: events.MustEntitylessReceiverTarget(actorTarget), Initialization: construction,
	}
	node := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "review", "handler")),
		Target:    events.MustExistingEntityTarget(target),
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(eventID, "work.requested", "2677-permutation-control", "", []byte(`{}`), 0, runID,
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), eventtest.RootRoutingSource(runID), time.Unix(1, 0).UTC())
	forward, reverse := []events.DeliveryRoute{agent, node}, []events.DeliveryRoute{node, agent}
	if reflect.DeepEqual(forward, reverse) || !reflect.DeepEqual(forward[0], reverse[1]) || !reflect.DeepEqual(forward[1], reverse[0]) {
		t.Fatal("permutation must change only the two route positions")
	}
	var original events.EventEnvelope
	for index, routes := range [][]events.DeliveryRoute{forward, reverse} {
		if !reflect.DeepEqual(events.NormalizeDeliveryRoutes(routes), routes) {
			t.Fatal("normalization unexpectedly changed the deliberate route order")
		}
		projected, _, err := ResolvePreparedPublishEventTargetProjection(event, routes)
		if err != nil {
			t.Fatal(err)
		}
		admitted, err := events.RevalidatePersistedEvent(projected)
		if err != nil {
			t.Fatal(err)
		}
		prepared := PreparedPublishEvent{Event: admitted, Settlement: exactSiblingDeliverySettlement(t), DeliveryRoutes: routes}
		if err := prepared.Validate(); err != nil {
			t.Fatal(err)
		}
		envelope := projected.NormalizedEnvelope()
		if index == 0 {
			original = envelope
		} else if !reflect.DeepEqual(original, envelope) {
			t.Fatalf("route permutation changed the exact normalized envelope: forward=%#v reverse=%#v", original, envelope)
		}
		if len(envelope.TargetSet) != 2 || !envelope.Target.Empty() {
			t.Fatalf("mixed actor/node projection changed shape: %#v", envelope)
		}
		for position, route := range routes {
			id, err := deliverylifecycle.DeliveryID(eventID, route)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("permutation=%d position=%d delivery=%s route=%#v envelope=%#v", index, position, id, route, envelope)
		}
	}
}
