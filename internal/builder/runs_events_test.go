package builder

import (
	"reflect"
	"strings"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store"
)

func TestProjectCanonicalRunDebugReplay_UsesCanonicalEventOwnerPayload(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	snapshot := runtimebus.RunLifecycleSnapshot{
		RunID:     "run-123",
		StartedAt: now,
	}
	events := []store.OperatorEventFull{{
		EventID:       "evt-1",
		EventName:     "workflow.started",
		ExecutionMode: "live",
		RunID:         "run-123",
		EntityID:      "entity-1",
		CreatedAt:     now,
		Source:        "builder",
		Payload:       map[string]any{"topic": "sample"},
	}}

	replay, _ := projectCanonicalRunDebugReplay(snapshot, events, nil)
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

func TestProjectCanonicalRunDebugReplay_DoesNotPromotePayloadEntityToInstanceID(t *testing.T) {
	now := time.Unix(1700001200, 0).UTC()
	events := []store.OperatorEventFull{{
		EventID:       "evt-payload-only",
		EventName:     "workflow.payload_only",
		ExecutionMode: "live",
		RunID:         "run-123",
		CreatedAt:     now,
		Source:        "builder",
		Payload:       map[string]any{"entity_id": "payload-entity", "topic": "sample"},
	}}

	replay, _ := projectCanonicalRunDebugReplay(runtimebus.RunLifecycleSnapshot{}, events, nil)
	if len(replay) != 1 {
		t.Fatalf("replay len = %d, want 1", len(replay))
	}
	if replay[0]["type"] != "event.fired" {
		t.Fatalf("replay[0].type = %#v, want event.fired", replay[0]["type"])
	}
	if _, ok := replay[0]["instance_id"]; ok {
		t.Fatalf("payload-only event instance_id = %#v, want absent", replay[0]["instance_id"])
	}
	payload, _ := replay[0]["payload"].(map[string]any)
	rawPayload, _ := payload["payload"].(map[string]any)
	if rawPayload["entity_id"] != "payload-entity" {
		t.Fatalf("payload.payload = %#v, want payload entity_id preserved", rawPayload)
	}
}

func TestProjectCanonicalRunDebugReplay_PreservesCanonicalRuntimeLogDetailAndTimestamp(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassInternalFailure, "runtime_log_failed", "runtime", "retrying", nil), "runtime", "retrying")
	runtimeLogs := []store.OperatorRuntimeLogEntry{{
		LogID:     "evt-log-1",
		Level:     "warn",
		Component: "runtime",
		Source:    "node-1",
		RunID:     "run-123",
		EntityID:  "entity-1",
		ErrorCode: "boom",
		Failure:   &failure,
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
	failureValue, ok := payload["failure"].(map[string]any)
	if !ok || failureValue["class"] != string(runtimefailures.ClassInternalFailure) {
		t.Fatalf("payload.failure = %#v", payload["failure"])
	}
	detail, _ := payload["detail"].(map[string]any)
	if _, exists := detail["error"]; exists {
		t.Fatalf("detail = %#v, retired error key survived", detail)
	}
	if _, exists := payload["error_code"]; exists {
		t.Fatalf("payload = %#v, retired error_code survived", payload)
	}
}

func TestFailedRunTerminalProjectionIsStableAcrossLiveAndReplay(t *testing.T) {
	started := time.Unix(1700000000, 0).UTC()
	ended := started.Add(time.Minute)
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassInternalFailure, "run_quiescence_failed", "builder.run_hub", "wait_for_quiescence", map[string]any{"phase": "settle"}), "builder.run_hub", "wait_for_quiescence")
	snapshot := runtimebus.RunLifecycleSnapshot{
		RunID:     "run-123",
		Status:    "failed",
		Failure:   &failure,
		StartedAt: started,
		EndedAt:   &ended,
	}
	live := canonicalRunTerminalCandidate(snapshot)
	replay, _ := projectCanonicalRunDebugReplay(snapshot, nil, nil)
	if len(replay) != 2 {
		t.Fatalf("replay len = %d, want started + failed", len(replay))
	}
	if live.key == "" || live.event["id"] != replay[1]["id"] || !reflect.DeepEqual(live.event["payload"], replay[1]["payload"]) {
		t.Fatalf("live = %#v, replay = %#v", live.event, replay[1])
	}
	if failure.Message != "" && strings.Contains(live.key, failure.Message) {
		t.Fatalf("terminal key contains presentation text: %s", live.key)
	}
}
