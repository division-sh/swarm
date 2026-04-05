package builder

import (
	"testing"

	runtimepkg "swarm/internal/runtime"
)

func TestRunHubToRunEvent_PublishedEntryMapsToEventFired(t *testing.T) {
	hub := &runHub{}
	event := hub.toRunEvent(runtimepkg.RuntimeLogEntry{
		Component: "eventbus",
		Action:    "published",
		EventID:   "evt-1",
		EventType: "workflow.started",
		AgentID:   "node-1",
		EntityID:  "entity-1",
		Detail:    map[string]any{"source": "builder"},
	})

	if event["type"] != "event.fired" {
		t.Fatalf("event.type = %#v", event["type"])
	}
	if event["id"] != "evt-1" {
		t.Fatalf("event.id = %#v", event["id"])
	}
	if event["node_id"] != "node-1" {
		t.Fatalf("event.node_id = %#v", event["node_id"])
	}
	payload, _ := event["payload"].(map[string]any)
	if payload["event_name"] != "workflow.started" {
		t.Fatalf("payload.event_name = %#v", payload["event_name"])
	}
	if payload["source"] != "builder" {
		t.Fatalf("payload.source = %#v", payload["source"])
	}
}

func TestRunHubToRunEvent_RuntimeLogEntryMapsDetailAndFallbackID(t *testing.T) {
	hub := &runHub{}
	event := hub.toRunEvent(runtimepkg.RuntimeLogEntry{
		Level:     "warn",
		Component: "runtime",
		Action:    "retrying",
		EventType: "workflow.started",
		AgentID:   "node-1",
		EntityID:  "entity-1",
		Error:     "boom",
		Detail:    "ignored",
	})

	if event["type"] != "runtime.log" {
		t.Fatalf("event.type = %#v", event["type"])
	}
	if event["id"] == "" {
		t.Fatal("expected generated event id")
	}
	payload, _ := event["payload"].(map[string]any)
	if payload["level"] != "warn" || payload["component"] != "runtime" || payload["action"] != "retrying" {
		t.Fatalf("payload = %#v", payload)
	}
	detail, _ := payload["detail"].(map[string]any)
	if len(detail) != 0 {
		t.Fatalf("detail = %#v, want empty map", detail)
	}
}
