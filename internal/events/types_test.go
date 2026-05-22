package events

import "testing"

func TestEventEnvelopeOwnsCanonicalIdentity(t *testing.T) {
	evt := Event{
		Payload: []byte(`{"entity_id":"payload-ent","flow_instance":"payload-flow"}`),
	}.WithEnvelope(EventEnvelope{
		EntityID:     "env-ent",
		FlowInstance: "review/inst-1",
	})

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
	evt := Event{
		Payload: []byte(`{"entity_id":"payload-ent","flow_instance":"payload-flow"}`),
	}

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
	evt := (Event{}).WithSourceRoute(RouteIdentity{
		EntityID:     "source-ent",
		FlowInstance: "source-flow",
	}).WithTargetSet([]RouteIdentity{
		{EntityID: "target-ent-1", FlowInstance: "target-flow-1"},
		{EntityID: "target-ent-2", FlowInstance: "target-flow-2"},
	})

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

	delivered := evt.WithDeliveryTarget(RouteIdentity{EntityID: "target-ent-2", FlowInstance: "target-flow-2"})
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
