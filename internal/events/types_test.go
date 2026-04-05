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
