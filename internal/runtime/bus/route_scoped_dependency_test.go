package bus

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestCompiledConnectionsPreserveCrossInstanceObserverInBothCreationOrders(t *testing.T) {
	for _, observerFirst := range []bool{false, true} {
		name := "sources_first"
		if observerFirst {
			name = "observer_first"
		}
		t.Run(name, func(t *testing.T) {
			source, _ := completeObserverFixture(t)
			table, err := DeriveRouteTable(source)
			if err != nil {
				t.Fatal(err)
			}
			reader := &constructionIndexTestReader{}
			root := ConstructedFlowInstanceIdentityFixture(source, ".", "", busInternalTestRunID)
			reader.observations = append(reader.observations, constructionIndexObservation(t, source, busInternalTestRunID, root, ""))
			observer := ConstructedFlowInstanceIdentityFixture(source, "observer", "one", busInternalTestRunID)
			observed := constructionIndexObservation(t, source, busInternalTestRunID, observer, "one")
			if observerFirst {
				reader.observations = append(reader.observations, observed)
			}
			for _, flow := range []string{"producer", "other"} {
				for _, id := range []string{"old-a", "old-b"} {
					instance := ConstructedFlowInstanceIdentityFixture(source, flow, id, busInternalTestRunID)
					reader.observations = append(reader.observations, constructionIndexObservation(t, source, busInternalTestRunID, instance, id))
				}
			}
			if !observerFirst {
				reader.observations = append(reader.observations, observed)
			}
			resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
			ctx := constructionIndexContext(t, source)
			assertDelivery := func(flow, id, local string) {
				t.Helper()
				instance := ConstructedFlowInstanceIdentityFixture(source, flow, id, busInternalTestRunID)
				event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID(flow+id), events.EventType(flow+"/"+id+"/"+local), "", "", []byte(`{"item_id":"one"}`), 0,
					busInternalTestRunID, events.EventEnvelope{}, eventtest.ConcreteTemplateRoutingSource(flow, instance.InstancePath, instance.EntityID), time.Now().UTC())
				dispatch, err := resolver.Plan(ctx, event)
				if err != nil || !dispatch.Matched || !dispatch.Failure.Empty() || len(dispatch.DeliveryIntents) != 1 || len(dispatch.RoutedRecipients) != 1 {
					t.Fatalf("compiled observer delivery %s/%s: dispatch=%+v err=%v", flow, id, dispatch, err)
				}
				want := events.RouteIdentity{FlowID: observer.TemplateID, FlowInstance: observer.InstancePath, EntityID: observer.EntityID}
				if target := dispatch.DeliveryIntents[0].TargetBlueprint; target != want || dispatch.RoutedRecipients[0].Path != observer.InstancePath {
					t.Fatalf("observer borrowed source identity: target=%+v subscriber=%+v", target, dispatch.RoutedRecipients[0])
				}
			}
			before := append([]pipeline.FlowInstanceObservation(nil), reader.observations...)
			for _, flow := range []string{"producer", "other"} {
				for _, id := range []string{"old-a", "old-b"} {
					local := "work.ready"
					if flow == "other" {
						local = "other.ready"
					}
					assertDelivery(flow, id, local)
				}
			}
			if !reflect.DeepEqual(before, reader.observations) {
				t.Fatal("planning rewrote construction evidence")
			}
			newProducer := ConstructedFlowInstanceIdentityFixture(source, "producer", "new", busInternalTestRunID)
			reader.observations = append(reader.observations, constructionIndexObservation(t, source, busInternalTestRunID, newProducer, "new"))
			assertDelivery("producer", "new", "work.ready")
			assertDelivery("producer", "old-a", "work.ready")
			assertDelivery("producer", "old-b", "work.ready")
			assertDelivery("other", "old-a", "other.ready")
		})
	}
}
