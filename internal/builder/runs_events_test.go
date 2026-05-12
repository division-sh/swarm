package builder

import (
	"testing"
	"time"

	runtimebus "swarm/internal/runtime/bus"
	"swarm/internal/store"
)

func TestProjectCanonicalRunDebugReplay_UsesCanonicalTraceRows(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	snapshot := runtimebus.RunLifecycleSnapshot{
		RunID:     "run-123",
		StartedAt: now,
	}
	traceRows := []store.RunDebugTraceRow{{
		EventID:         "evt-1",
		EventName:       "workflow.started",
		EntityID:        "entity-1",
		EventCreatedAt:  now,
		EventSource:     "builder",
		EventSourceType: "agent",
	}}

	replay, _ := projectCanonicalRunDebugReplay(snapshot, traceRows, nil)
	if len(replay) != 2 {
		t.Fatalf("replay len = %d, want 2", len(replay))
	}
	if replay[0]["type"] != "run.started" {
		t.Fatalf("replay[0].type = %#v, want run.started", replay[0]["type"])
	}
	if replay[1]["type"] != "event.fired" {
		t.Fatalf("replay[1].type = %#v, want event.fired", replay[1]["type"])
	}
	if got := replay[1]["timestamp"]; got != now.Format(time.RFC3339) {
		t.Fatalf("event timestamp = %#v, want %q", got, now.Format(time.RFC3339))
	}
	payload, _ := replay[1]["payload"].(map[string]any)
	if payload["event_name"] != "workflow.started" {
		t.Fatalf("payload.event_name = %#v", payload["event_name"])
	}
	if payload["source"] != "builder" {
		t.Fatalf("payload.source = %#v", payload["source"])
	}
}

func TestProjectCanonicalRunDebugReplay_PreservesCanonicalRuntimeLogDetailAndTimestamp(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	runtimeLogs := []store.OperatorRuntimeLogEntry{{
		LogID:     "evt-log-1",
		Level:     "warn",
		Component: "runtime",
		Source:    "node-1",
		RunID:     "run-123",
		EntityID:  "entity-1",
		ErrorCode: "boom",
		Message:   "retrying",
		Details: map[string]any{
			"component": "runtime",
			"action":    "retrying",
			"error":     "boom",
		},
		TS: now,
	}}

	replay, _ := projectCanonicalRunDebugReplay(runtimebus.RunLifecycleSnapshot{}, nil, runtimeLogs)
	if len(replay) != 1 {
		t.Fatalf("replay len = %d, want 1", len(replay))
	}
	if replay[0]["type"] != "runtime.log" {
		t.Fatalf("replay[0].type = %#v, want runtime.log", replay[0]["type"])
	}
	if got := replay[0]["timestamp"]; got != now.Format(time.RFC3339) {
		t.Fatalf("runtime log timestamp = %#v, want %q", got, now.Format(time.RFC3339))
	}
	payload, _ := replay[0]["payload"].(map[string]any)
	if payload["level"] != "warn" || payload["component"] != "runtime" || payload["action"] != "retrying" {
		t.Fatalf("payload = %#v", payload)
	}
	detail, _ := payload["detail"].(map[string]any)
	if detail["error"] != "boom" {
		t.Fatalf("detail = %#v", detail)
	}
}
