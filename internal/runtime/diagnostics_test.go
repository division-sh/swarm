package runtime

import (
	"context"
	"testing"

	runtimebus "swarm/internal/runtime/bus"
)

func TestRuntimeLogger_Log_AppendsSpecShapedFlightRecorderEntry(t *testing.T) {
	logger := NewRuntimeLogger(nil)
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimebus.WithEmittedEventsRecorder(context.Background(), recorder)

	logger.Log(ctx, RuntimeLogEntry{
		Level:      "warn",
		Message:    "Tool execution was denied for save_entity_field",
		Component:  "tool-executor",
		Action:     "tool_execution_denied",
		EventID:    "evt-1",
		EventType:  "validation/requested",
		AgentID:    "agent-1",
		EntityID:   "entity-1",
		SessionID:  "session-1",
		Error:      "runtime_error code=cross_flow_write_forbidden",
		DurationUS: 1200,
		Detail: map[string]any{
			"tool_name":     "save_entity_field",
			"denial_layer":  "executor",
			"denial_reason": "cross_flow_write_forbidden",
		},
	})

	entries := recorder.SnapshotFlightRecorder()
	if len(entries) != 1 {
		t.Fatalf("flight recorder count = %d, want 1", len(entries))
	}
	entry := entries[0]
	if entry.Kind != "runtime_log" {
		t.Fatalf("kind = %q, want runtime_log", entry.Kind)
	}
	if entry.LogLevel != "warn" {
		t.Fatalf("log_level = %q, want warn", entry.LogLevel)
	}
	if entry.Message != "Tool execution was denied for save_entity_field" {
		t.Fatalf("message = %q", entry.Message)
	}
	details, ok := entry.Details.(map[string]any)
	if !ok {
		t.Fatalf("details type = %T, want map[string]any", entry.Details)
	}
	if details["component"] != "tool-executor" {
		t.Fatalf("details.component = %#v", details["component"])
	}
	if details["action"] != "tool_execution_denied" {
		t.Fatalf("details.action = %#v", details["action"])
	}
	if details["tool_name"] != "save_entity_field" {
		t.Fatalf("details.tool_name = %#v", details["tool_name"])
	}
	if details["denial_layer"] != "executor" {
		t.Fatalf("details.denial_layer = %#v", details["denial_layer"])
	}
	if details["error"] != "runtime_error code=cross_flow_write_forbidden" {
		t.Fatalf("details.error = %#v", details["error"])
	}
}
