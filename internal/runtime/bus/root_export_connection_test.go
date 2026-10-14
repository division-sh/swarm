package bus

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/google/uuid"
)

func TestRootExportDoesNotMaskConnectedReceiverWithoutRegistration(t *testing.T) {
	source := connectRoutePlanRootProducerStaticSource(t)
	store := newConnectRoutePlanStaticStore()
	// The receiver entity exists, but its executable registration is absent.
	routes := derivedRouteTableFixture(t, source)
	routes.connectDefinitions = nil
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, RouteTable: routes,
		Durable: DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: busInternalTestRunID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	installConnectionSourceConstruction(t, eb, source, ".")
	instance := ConstructedFlowInstanceIdentityFixture(source, "consumer", "", busInternalTestRunID)
	observed := constructionIndexObservation(t, source, busInternalTestRunID, instance, "")
	store.installIndexObservation(observed)
	header, err := observed.WorkflowInstance()
	if err != nil {
		t.Fatal(err)
	}
	store.workflowInstances = append(store.workflowInstances, header)
	store.setTargetOwnerRoutes(events.RouteIdentity{FlowID: instance.TemplateID, FlowInstance: instance.InstancePath, EntityID: instance.EntityID})
	evt := connectRoutePlanRootProducerEvent(uuid.NewString(), "root.ready", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatal(err)
	}
	settlement := store.settlements[evt.ID()]
	if !settlement.NoDelivery() || settlement.Reason() != events.NoDeliveryMatchedNoRecipient {
		t.Fatalf("root export hid missing connected recipient: %#v", settlement)
	}
	if len(store.routes[evt.ID()]) != 0 {
		t.Fatalf("invented delivery: %#v", store.routes[evt.ID()])
	}
	plans := settlement.Ledger().Plans()
	if len(plans) != 1 || plans[0].Resolution() != events.ConnectPlanNoRegistration {
		t.Fatalf("lost connection failure evidence: %#v", plans)
	}
}
