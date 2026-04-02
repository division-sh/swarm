package store

import (
	"testing"
	"time"

	"swarm/internal/events"
)

func TestEventStorageEnvelope_PersistsTraceAndParent(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	id, runID, name, entityID, flowInstance, scope, payload, chainDepth, traceID, producedBy, producedByType, sourceEventID, createdAt := eventStorageEnvelope(events.Event{
		ID:            "evt-1",
		RunID:         "33333333-3333-3333-3333-333333333333",
		Type:          events.EventType("demo.event"),
		SourceAgent:   "agent-1",
		TraceID:       "trace-123",
		ParentEventID: "11111111-1111-1111-1111-111111111111",
		ChainDepth:    2,
		CreatedAt:     now,
	}.WithEntityID("22222222-2222-2222-2222-222222222222"))

	if id != "evt-1" || name != "demo.event" {
		t.Fatalf("unexpected envelope identity: id=%q name=%q", id, name)
	}
	if runID != "33333333-3333-3333-3333-333333333333" {
		t.Fatalf("run_id = %q", runID)
	}
	if entityID != "22222222-2222-2222-2222-222222222222" {
		t.Fatalf("entity_id = %q", entityID)
	}
	if flowInstance != "" || scope != "entity" {
		t.Fatalf("flow/scope = %q/%q", flowInstance, scope)
	}
	if chainDepth != 2 {
		t.Fatalf("chain_depth = %d", chainDepth)
	}
	if traceID != "trace-123" {
		t.Fatalf("trace_id = %q", traceID)
	}
	if sourceEventID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("source_event_id = %q", sourceEventID)
	}
	if producedBy != "agent-1" || producedByType != "agent" {
		t.Fatalf("produced_by = %q/%q", producedBy, producedByType)
	}
	if createdAt != now {
		t.Fatalf("created_at = %s", createdAt)
	}
	if string(payload) == "" {
		t.Fatal("expected payload bytes")
	}
}
