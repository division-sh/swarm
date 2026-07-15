package bus

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
)

func TestSyntheticCarryProjectionIsRouteScopedForMixedDeliveries(t *testing.T) {
	evt := eventtest.RootIngress("projection-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	projection := mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": "case-1"})
	projected, err := projectEventForDeliveryRoute(evt, events.DeliveryRoute{SubscriberType: "node", SubscriberID: "validator", PayloadProjection: projection})
	if err != nil {
		t.Fatalf("project synthetic route: %v", err)
	}
	unprojected, err := projectEventForDeliveryRoute(evt, events.DeliveryRoute{SubscriberType: "node", SubscriberID: "auditor"})
	if err != nil {
		t.Fatalf("project ordinary route: %v", err)
	}
	if got := payloadStringField(t, projected, "validation_case_id"); got != "case-1" {
		t.Fatalf("projected validation_case_id = %q, want case-1", got)
	}
	if got := payloadStringField(t, unprojected, "validation_case_id"); got != "" {
		t.Fatalf("ordinary route leaked validation_case_id = %q", got)
	}
	if string(evt.Payload()) != `{"candidate":"acct-1"}` {
		t.Fatalf("journal payload mutated = %s", evt.Payload())
	}
}

func TestDeliveryRouteProjectionPreservesUntargetedLiveRecipientEnvelope(t *testing.T) {
	want := events.RouteIdentity{FlowInstance: "validation/one", EntityID: "entity-1"}
	evt := eventtest.RootIngress("projection-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{want}), time.Now().UTC())

	projected, err := projectEventForDeliveryRoute(evt, events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "validator"})
	if err != nil {
		t.Fatalf("project untargeted live route: %v", err)
	}
	routes := projected.TargetRoutes()
	if len(routes) != 1 || routes[0] != want {
		t.Fatalf("projected target routes = %#v, want original envelope route %#v", routes, want)
	}
}

func TestPayloadCarriesAreNotPersistedInDeliveryProjection(t *testing.T) {
	projection, err := syntheticDeliveryPayloadProjection(runtimepinrouting.ConnectRoutePlan{
		InstanceKey: &runtimepinrouting.ConnectRoutePlanInstanceKey{Mode: runtimecontracts.FlowInputResolutionModeSelect},
	}, TemplateInstanceLifecycleDecision{
		Action:      templateInstanceLifecycleActionSelectedExisting,
		KeyMaterial: []runtimecontracts.TemplateInstanceKeyValue{{Field: "account_id", Value: "acct-1"}},
	})
	if err != nil {
		t.Fatalf("syntheticDeliveryPayloadProjection: %v", err)
	}
	if !projection.Empty() {
		t.Fatalf("payload-owned select carry entered route projection: %#v", projection.Fields())
	}
}

func TestCreateSyntheticCarryFailsClosedOnDynamicPayloadCollisionBeforeHandler(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.SubscribeInternal("validator")
	evt := eventtest.RootIngress("collision-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"validation_case_id":"producer-value"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{
		SubscriberType:    "node",
		SubscriberID:      "validator",
		PayloadProjection: mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": "synthetic-value"}),
	}
	err = eb.deliverToRecipientsWithRoutes(context.Background(), evt, []string{"validator"}, []events.DeliveryRoute{route})
	if err == nil || !strings.Contains(err.Error(), "producer payload and receiver-owned synthetic carry") {
		t.Fatalf("delivery error = %v, want synthetic carry collision", err)
	}
	select {
	case delivered := <-ch:
		t.Fatalf("handler carrier received colliding event: %#v", delivered)
	default:
	}
}

func TestDeliveryRouteProjectionHasOneProductionOwner(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RootIngress("owner-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{
		SubscriberType:    "node",
		SubscriberID:      "validator",
		Target:            events.RouteIdentity{FlowID: "validation", FlowInstance: "validation/one", EntityID: "entity-1"},
		PayloadProjection: mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": "case-1"}),
	}
	interceptor := &projectionCaptureInterceptor{}
	if _, _, err := eb.runNodeDeliveryRouteInterceptors(context.Background(), evt, []events.DeliveryRoute{route}, []DeliveryRouteInterceptor{interceptor}); err != nil {
		t.Fatalf("run route interceptor: %v", err)
	}
	ch := eb.SubscribeInternal("validator")
	if err := eb.deliverToRecipientsWithRoutes(context.Background(), evt, []string{"validator"}, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("deliver live route: %v", err)
	}
	live := <-ch
	if len(interceptor.events) != 1 || string(interceptor.events[0].Payload()) != string(live.Payload()) || interceptor.events[0].TargetRoute() != live.TargetRoute() {
		t.Fatalf("projector paths diverged: interceptor=%#v live=%#v", interceptor.events, live)
	}
}

type projectionCaptureInterceptor struct {
	events []events.Event
}

func (i *projectionCaptureInterceptor) InterceptDeliveryRoute(_ context.Context, evt events.Event, _ events.DeliveryRoute) (bool, []events.Event, error) {
	i.events = append(i.events, evt)
	return true, nil, nil
}

func payloadStringField(t *testing.T, evt events.Event, field string) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	value, _ := payload[field].(string)
	return value
}
