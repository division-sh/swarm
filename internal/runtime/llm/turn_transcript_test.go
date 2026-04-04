package llm

import (
	"testing"

	runtimebus "swarm/internal/runtime/bus"
)

func TestBuildTurnBlocks_CorrelatesToolResultsWithToolUse(t *testing.T) {
	rec := AgentTurnRecord{
		ResponseRaw: []byte("{\"type\":\"stream_event\",\"event\":{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"schedule\"}}}\n" +
			"{\"type\":\"stream_event\",\"event\":{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"delay_seconds\\\":1209600}\"}}}\n" +
			"{\"type\":\"stream_event\",\"event\":{\"type\":\"content_block_stop\",\"index\":0}}\n" +
			"{\"type\":\"user\",\"message\":{\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu_1\",\"content\":[{\"type\":\"text\",\"text\":\"{\\\"status\\\":\\\"scheduled\\\"}\"}]}]}}\n" +
			"{\"type\":\"result\",\"result\":\"14-day review scheduled.\"}"),
	}

	blocks := BuildTurnBlocks(rec)
	if len(blocks) < 3 {
		t.Fatalf("expected multiple turn blocks, got %#v", blocks)
	}

	var sawToolUse, sawToolResult, sawOutcome bool
	for _, block := range blocks {
		switch block.Kind {
		case "tool_use":
			if block.ToolName == "schedule" {
				sawToolUse = true
			}
		case "tool_result":
			if block.ToolName != "schedule" {
				t.Fatalf("tool_result tool_name = %q, want schedule", block.ToolName)
			}
			sawToolResult = true
		case "outcome":
			if block.Text == "14-day review scheduled." {
				sawOutcome = true
			}
		}
	}
	if !sawToolUse || !sawToolResult || !sawOutcome {
		t.Fatalf("blocks missing expected kinds: %#v", blocks)
	}
}

func TestBuildTurnBlocks_IncludesPublishDiagnostics(t *testing.T) {
	rec := AgentTurnRecord{
		TriggerEventType: "scan.requested",
		EntityID:         "ent-1",
		PublishDiagnostics: []runtimebus.PublishDiagnostic{
			{
				EventID:   "evt-1",
				EventType: "scoring/vertical.shortlisted",
				EntityID:  "ent-1",
				RoutedRecipients: []runtimebus.PublishDiagnosticRecipient{
					{
						ID:             "validation-orchestrator",
						Type:           "node",
						Path:           "validation",
						MatchedPattern: "scoring/vertical.shortlisted",
						RouteSource:    "subscription",
						LocalizedEvent: "vertical.shortlisted",
					},
				},
			},
		},
	}

	blocks := BuildTurnBlocks(rec)
	if len(blocks) < 2 {
		t.Fatalf("expected dispatch and publish blocks, got %#v", blocks)
	}
	if blocks[1].Kind != "publish" {
		t.Fatalf("blocks[1].kind = %q, want publish", blocks[1].Kind)
	}
	if blocks[1].Title != "scoring/vertical.shortlisted" {
		t.Fatalf("blocks[1].title = %q", blocks[1].Title)
	}
	routed, ok := blocks[1].Data["routed_recipients"].([]runtimebus.PublishDiagnosticRecipient)
	if !ok || len(routed) != 1 || routed[0].LocalizedEvent != "vertical.shortlisted" {
		t.Fatalf("publish block routed_recipients = %#v", blocks[1].Data["routed_recipients"])
	}
}

func TestBuildTurnBlocks_IncludesSpecShapedRuntimeLogFlightRecorderEntries(t *testing.T) {
	rec := AgentTurnRecord{
		FlightRecorder: []runtimebus.FlightRecorderEntry{
			{
				Kind:     "runtime_log",
				LogLevel: "warn",
				Message:  "Tool execution was denied for save_entity_field",
				Details: map[string]any{
					"component":     "tool-executor",
					"action":        "tool_execution_denied",
					"tool_name":     "save_entity_field",
					"denial_layer":  "executor",
					"denial_reason": "cross_flow_write_forbidden",
				},
			},
		},
	}

	blocks := BuildTurnBlocks(rec)
	if len(blocks) != 1 {
		t.Fatalf("blocks = %#v", blocks)
	}
	if blocks[0].Kind != "runtime_log" {
		t.Fatalf("kind = %q, want runtime_log", blocks[0].Kind)
	}
	if blocks[0].Title != "Tool execution was denied for save_entity_field" {
		t.Fatalf("title = %q", blocks[0].Title)
	}
	if got := asString(blocks[0].Data["log_level"]); got != "warn" {
		t.Fatalf("log_level = %q", got)
	}
	details, ok := blocks[0].Data["details"].(map[string]any)
	if !ok {
		t.Fatalf("details = %#v", blocks[0].Data["details"])
	}
	if details["denial_layer"] != "executor" {
		t.Fatalf("details.denial_layer = %#v", details["denial_layer"])
	}
}
