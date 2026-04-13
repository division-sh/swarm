package builder

import (
	"encoding/json"
	"testing"
	"time"

	"swarm/internal/store"
)

func TestProjectCanonicalRunDebugReplay_PreservesCanonicalEventPayloadAndTimestamp(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	report := store.RunDebugReport{
		RunID:      "run-123",
		StartedAt:  now,
		EventCount: 1,
		Events: []store.RunDebugEvent{{
			EventID:    "evt-1",
			EventName:  "workflow.started",
			EntityID:   "entity-1",
			CreatedAt:  now,
			Source:     "builder",
			SourceType: "agent",
			Payload:    json.RawMessage(`{"topic":"sample"}`),
		}},
	}

	replay, _ := projectCanonicalRunDebugReplay(report)
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
	rawPayload, _ := payload["payload"].(map[string]any)
	if rawPayload["topic"] != "sample" {
		t.Fatalf("payload.payload = %#v", rawPayload)
	}
}

func TestProjectCanonicalRunDebugReplay_PreservesCanonicalRuntimeLogDetailAndTimestamp(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	report := store.RunDebugReport{
		RunID: "run-123",
		RuntimeLogs: []store.RunDebugRuntimeLog{{
			EventID:   "evt-log-1",
			Level:     "warn",
			Component: "runtime",
			Action:    "retrying",
			EventType: "workflow.started",
			AgentID:   "node-1",
			EntityID:  "entity-1",
			Error:     "boom",
			Detail:    json.RawMessage(`{"component":"runtime","action":"retrying","error":"boom"}`),
			CreatedAt: now,
		}},
	}

	replay, _ := projectCanonicalRunDebugReplay(report)
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
