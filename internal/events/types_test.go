package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEventEnvelopeOwnsCanonicalIdentity(t *testing.T) {
	evt := NewProjectionEvent(
		"",
		"",
		"",
		"",
		json.RawMessage(`{"entity_id":"payload-ent","flow_instance":"payload-flow"}`),
		0,
		"",
		"",
		EventEnvelope{
			EntityID:     "env-ent",
			FlowInstance: "review/inst-1",
		},
		time.Time{},
	)

	if got := evt.EntityID(); got != "env-ent" {
		t.Fatalf("EntityID() = %q, want env-ent", got)
	}
	if got := evt.FlowInstance(); got != "review/inst-1" {
		t.Fatalf("FlowInstance() = %q, want review/inst-1", got)
	}
	if got := evt.Scope(); got != EventScopeEntity {
		t.Fatalf("Scope() = %q, want %q", got, EventScopeEntity)
	}
}

func TestEventWithoutEnvelopeFailsClosedToGlobalScope(t *testing.T) {
	evt := NewProjectionEvent("", "", "", "", json.RawMessage(`{"entity_id":"payload-ent","flow_instance":"payload-flow"}`), 0, "", "", EventEnvelope{}, time.Time{})

	if got := evt.EntityID(); got != "" {
		t.Fatalf("EntityID() = %q, want empty without envelope metadata", got)
	}
	if got := evt.FlowInstance(); got != "" {
		t.Fatalf("FlowInstance() = %q, want empty without envelope metadata", got)
	}
	if got := evt.Scope(); got != EventScopeGlobal {
		t.Fatalf("Scope() = %q, want %q", got, EventScopeGlobal)
	}
}

func TestEventTargetSetDoesNotMaterializeFirstTargetProjection(t *testing.T) {
	envelope := EnvelopeForSourceRoute(EventEnvelope{}, RouteIdentity{
		EntityID:     "source-ent",
		FlowInstance: "source-flow",
	})
	envelope = EnvelopeForTargetSet(envelope, []RouteIdentity{
		{EntityID: "target-ent-1", FlowInstance: "target-flow-1"},
		{EntityID: "target-ent-2", FlowInstance: "target-flow-2"},
	})
	evt := NewProjectionEvent("", "", "", "", nil, 0, "", "", envelope, time.Time{})

	if got := evt.EntityID(); got != "" {
		t.Fatalf("EntityID() = %q, want empty before per-recipient delivery target", got)
	}
	if got := evt.FlowInstance(); got != "" {
		t.Fatalf("FlowInstance() = %q, want empty before per-recipient delivery target", got)
	}
	if got := evt.Scope(); got != EventScopeGlobal {
		t.Fatalf("Scope() = %q, want %q", got, EventScopeGlobal)
	}
	if got := evt.TargetRoute(); !got.Empty() {
		t.Fatalf("TargetRoute() = %#v, want empty singular target", got)
	}
	if got := evt.TargetRoutes(); len(got) != 2 {
		t.Fatalf("TargetRoutes() count = %d, want 2", len(got))
	}

	delivered := NewProjectionEvent("", "", "", "", nil, 0, "", "", EnvelopeForTargetRoute(evt.NormalizedEnvelope(), RouteIdentity{EntityID: "target-ent-2", FlowInstance: "target-flow-2"}), time.Time{})
	if got := delivered.EntityID(); got != "target-ent-2" {
		t.Fatalf("delivered EntityID() = %q, want target-ent-2", got)
	}
	if got := delivered.FlowInstance(); got != "target-flow-2" {
		t.Fatalf("delivered FlowInstance() = %q, want target-flow-2", got)
	}
	if got := delivered.TargetRoutes(); len(got) != 0 {
		t.Fatalf("delivered TargetRoutes() count = %d, want 0 after singular delivery target", len(got))
	}
}

func TestEventContextMapOmitsLegacyReceiverProjectionFields(t *testing.T) {
	envelope := EnvelopeForSourceRoute(EventEnvelope{
		EntityID:     "legacy-ent",
		FlowInstance: "legacy-flow",
		Scope:        EventScopeEntity,
	}, RouteIdentity{EntityID: "source-ent", FlowInstance: "source-flow", FlowID: "source"})
	envelope = EnvelopeForTargetRoute(envelope, RouteIdentity{EntityID: "target-ent", FlowInstance: "target-flow", FlowID: "target"})
	evt := NewProjectionEvent(
		"evt-1",
		EventType("custom.triggered"),
		"runtime",
		"task-1",
		nil,
		0,
		"run-1",
		"parent-1",
		envelope,
		time.Date(2026, 7, 3, 1, 2, 3, 0, time.UTC),
	)

	got := evt.ContextMap("ready")
	if _, ok := got["entity_id"]; ok {
		t.Fatalf("ContextMap exposed legacy entity_id = %#v", got["entity_id"])
	}
	if _, ok := got["flow_instance"]; ok {
		t.Fatalf("ContextMap exposed legacy flow_instance = %#v", got["flow_instance"])
	}
	for _, field := range []string{"id", "type", "trigger_event_type", "source_agent", "task_id", "source", "target", "source_event_id", "emitted_at", "current_state", "run_id", "scope"} {
		if _, ok := got[field]; !ok {
			t.Fatalf("ContextMap missing supported event field %q in %#v", field, got)
		}
	}
}

func TestValidateEventContextReferenceRejectsLegacyReceiverProjections(t *testing.T) {
	for _, ref := range []string{"entity_id", "flow_instance"} {
		t.Run(ref, func(t *testing.T) {
			err := ValidateEventContextReference(ref)
			if err == nil {
				t.Fatalf("expected %s to be unsupported", ref)
			}
			if !strings.Contains(err.Error(), "_entity.") {
				t.Fatalf("error = %q, want replacement guidance", err.Error())
			}
		})
	}
}

func TestValidateEventContextReferenceAllowsRouteIdentity(t *testing.T) {
	for _, ref := range []string{
		"id",
		"type",
		"source.entity_id",
		"source.flow_instance",
		"source.flow_id",
		"target.entity_id",
		"target.flow_instance",
		"target.flow_id",
		"target_set",
		"source_event_id",
		"emitted_at",
		"trigger_event_type",
		"current_state",
		"run_id",
		"scope",
	} {
		t.Run(ref, func(t *testing.T) {
			if err := ValidateEventContextReference(ref); err != nil {
				t.Fatalf("ValidateEventContextReference(%q) error = %v", ref, err)
			}
		})
	}
}

func TestEventPayloadIsImmutableThroughConstructorAndAccessor(t *testing.T) {
	payload := json.RawMessage(`{"level":"warn"}`)
	evt := NewProjectionEvent(
		"evt-1",
		EventType("diagnostic.emitted"),
		"runtime",
		"task-1",
		payload,
		0,
		"run-1",
		"parent-1",
		EventEnvelope{},
		time.Time{},
	)

	payload[10] = 'e'
	if got := string(evt.Payload()); got != `{"level":"warn"}` {
		t.Fatalf("Payload() after caller mutation = %s, want original payload", got)
	}

	got := evt.Payload()
	got[10] = 'e'
	if again := string(evt.Payload()); again != `{"level":"warn"}` {
		t.Fatalf("Payload() after accessor mutation = %s, want original payload", again)
	}
}

func TestEventProjectionMethodsReturnCopies(t *testing.T) {
	evt := NewProjectionEvent(
		"evt-1",
		EventType("root.started"),
		"runtime",
		"task-1",
		nil,
		0,
		"run-1",
		"parent-1",
		EventEnvelope{},
		time.Time{},
	)

	projected := evt.WithEntityID("entity-1").WithFlowInstance("flow/inst-1")

	if got := evt.EntityID(); got != "" {
		t.Fatalf("original EntityID() = %q, want unchanged empty identity", got)
	}
	if got := evt.FlowInstance(); got != "" {
		t.Fatalf("original FlowInstance() = %q, want unchanged empty identity", got)
	}
	if got := projected.EntityID(); got != "entity-1" {
		t.Fatalf("projected EntityID() = %q, want entity-1", got)
	}
	if got := projected.FlowInstance(); got != "flow/inst-1" {
		t.Fatalf("projected FlowInstance() = %q, want flow/inst-1", got)
	}
}

func TestDeliveryPayloadProjectionIsCanonicalAndIsolated(t *testing.T) {
	input := map[string]string{" validation_case_id ": " case-1 "}
	projection, err := NewDeliveryPayloadProjection(input)
	if err != nil {
		t.Fatalf("NewDeliveryPayloadProjection: %v", err)
	}
	input[" validation_case_id "] = "mutated"
	fields := projection.Fields()
	if fields["validation_case_id"] != "case-1" {
		t.Fatalf("projection fields = %#v, want canonical copied field", fields)
	}
	fields["validation_case_id"] = "mutated-again"
	if got := projection.Fields()["validation_case_id"]; got != "case-1" {
		t.Fatalf("projection accessor mutated owner = %q, want case-1", got)
	}
	raw, err := json.Marshal(projection)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var roundTrip DeliveryPayloadProjection
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if roundTrip != projection {
		t.Fatalf("round trip = %#v, want %#v", roundTrip, projection)
	}
}

func TestValidateDeliveryRouteProjectionsRejectsConflictingFacts(t *testing.T) {
	first, err := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-1"})
	if err != nil {
		t.Fatalf("first projection: %v", err)
	}
	second, err := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-2"})
	if err != nil {
		t.Fatalf("second projection: %v", err)
	}
	route := DeliveryRoute{SubscriberType: "node", SubscriberID: "validator", Target: RouteIdentity{FlowID: "validation", FlowInstance: "validation/one"}}
	left, right := route, route
	left.PayloadProjection = first
	right.PayloadProjection = second
	if err := ValidateDeliveryRouteProjections([]DeliveryRoute{left, right}); err == nil || !strings.Contains(err.Error(), "conflicting synthetic payload projections") {
		t.Fatalf("ValidateDeliveryRouteProjections error = %v, want conflict", err)
	}
	if got := NormalizeDeliveryRoutes([]DeliveryRoute{left, right}); len(got) != 2 {
		t.Fatalf("NormalizeDeliveryRoutes merged conflicting projection facts: %#v", got)
	}
}
