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
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestSyntheticCarryProjectionIsRouteScopedForMixedDeliveries(t *testing.T) {
	evt := eventtest.RunCreatingRootIngress("projection-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	projection := mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": "case-1"})
	projected, err := projectEventForDeliveryRoute(evt, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("validator"), PayloadProjection: projection})
	if err != nil {
		t.Fatalf("project synthetic route: %v", err)
	}
	unprojected, err := projectEventForDeliveryRoute(evt, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("auditor")})
	if err != nil {
		t.Fatalf("project ordinary route: %v", err)
	}
	if got := payloadStringField(t, projected.Event(), "validation_case_id"); got != "case-1" {
		t.Fatalf("projected validation_case_id = %q, want case-1", got)
	}
	if got := payloadStringField(t, unprojected.Event(), "validation_case_id"); got != "" {
		t.Fatalf("ordinary route leaked validation_case_id = %q", got)
	}
	if string(evt.Payload()) != `{"candidate":"acct-1"}` {
		t.Fatalf("journal payload mutated = %s", evt.Payload())
	}
}

func TestDeliveryRouteProjectionPreservesUntargetedLiveRecipientEnvelope(t *testing.T) {
	want := events.RouteIdentity{FlowInstance: "validation/one", EntityID: "entity-1"}
	evt := eventtest.RunCreatingRootIngress("projection-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{want}), time.Now().UTC())

	projected, err := projectEventForDeliveryRoute(evt, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("validator")})
	if err != nil {
		t.Fatalf("project untargeted live route: %v", err)
	}
	routes := projected.Event().TargetRoutes()
	if len(routes) != 1 || routes[0] != want {
		t.Fatalf("projected target routes = %#v, want original envelope route %#v", routes, want)
	}
}

func TestPayloadCarriesAreNotPersistedInDeliveryProjection(t *testing.T) {
	plan := mustInstanceKeyConnectRoutePlan(t, connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelect))
	projection, err := syntheticDeliveryPayloadProjection(plan, TemplateInstanceLifecycleDecision{
		Action:      templateInstanceLifecycleActionSelectedExisting,
		KeyMaterial: []runtimecontracts.TemplateInstanceKeyValue{{Field: mustBusTemplateInstanceField(t, "account_id"), Value: "acct-1"}},
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
	ch := subscribeInternalDeliveriesForTest(t, eb, "validator")
	evt := eventtest.RunCreatingRootIngress("collision-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"validation_case_id":"producer-value"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{
		Recipient:         events.MustNodeDeliveryRecipient("validator"),
		Target:            events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowInstance: "root"}),
		PayloadProjection: mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": "synthetic-value"}),
	}
	err = eb.deliverToRecipientsWithRoutes(context.Background(), evt, []string{"validator"}, []events.DeliveryRoute{route})
	if err == nil || !strings.Contains(err.Error(), "delivery payload projection conflicts with producer field") {
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
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("validator"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "validation", FlowInstance: "validation/one", EntityID: "entity-1"}),
		PayloadProjection: mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": "case-1"}),
	}
	interceptor := &projectionCaptureInterceptor{}
	if _, _, _, err := eb.runNodeDeliveryRouteInterceptors(context.Background(), evt, []events.DeliveryRoute{route}, []DeliveryRouteInterceptor{interceptor}); err != nil {
		t.Fatalf("run route interceptor: %v", err)
	}
	ch := subscribeInternalDeliveriesForTest(t, eb, "validator")
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

func (i *projectionCaptureInterceptor) InterceptDeliveryRoute(_ context.Context, evt events.DeliveryEvent, _ events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	i.events = append(i.events, evt.Event())
	return true, nil, runtimepipelineobligation.Continue(), nil
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
