package llm

import (
	"encoding/json"
	"strings"
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

	var sawToolUse, sawToolResult, sawOutcome, sawSummary bool
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
		case turnSummaryBlockKind:
			sawSummary = true
		}
	}
	if !sawToolUse || !sawToolResult || !sawOutcome || !sawSummary {
		t.Fatalf("blocks missing expected kinds: %#v", blocks)
	}
}

func TestBuildTurnBlocks_IncludesPublishEntriesFromFlightRecorder(t *testing.T) {
	rec := AgentTurnRecord{
		TriggerEventType: "scan.requested",
		EntityID:         "ent-1",
		FlightRecorder: []runtimebus.FlightRecorderEntry{
			{
				Kind:      "publish",
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
	data, ok, err := blocks[1].PublishData()
	if err != nil {
		t.Fatalf("PublishData: %v", err)
	}
	if !ok || len(data.RoutedRecipients) != 1 || data.RoutedRecipients[0].LocalizedEvent != "vertical.shortlisted" {
		t.Fatalf("publish block routed_recipients = %#v", data.RoutedRecipients)
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
	data, ok, err := blocks[0].RuntimeLogData()
	if err != nil {
		t.Fatalf("RuntimeLogData: %v", err)
	}
	if !ok {
		t.Fatal("expected runtime_log data")
	}
	if data.LogLevel != "warn" {
		t.Fatalf("log_level = %q", data.LogLevel)
	}
	var details map[string]any
	if err := json.Unmarshal(data.Details, &details); err != nil {
		t.Fatalf("decode details: %v", err)
	}
	if details["denial_layer"] != "executor" {
		t.Fatalf("details.denial_layer = %#v", details["denial_layer"])
	}
}

func TestBuildTurnBlocks_AppendsCanonicalSummaryForExplicitBlocks(t *testing.T) {
	blocks := BuildTurnBlocks(AgentTurnRecord{
		TurnBlocks: []TurnBlock{
			{Kind: "progress", Text: "Scheduling the follow-up review."},
			newToolResultTurnBlock("schedule", map[string]any{"status": "scheduled"}, "toolu_1"),
			{Kind: "assistant_text", Text: "Parking for manual review."},
			{Kind: "outcome", Text: "14-day review scheduled."},
		},
	})

	var summary TurnSummaryTurnBlockData
	var ok bool
	for _, block := range blocks {
		if block.Kind == turnSummaryBlockKind {
			var err error
			summary, ok, err = block.TurnSummaryData()
			if err != nil {
				t.Fatalf("TurnSummaryData: %v", err)
			}
			break
		}
	}
	if !ok {
		t.Fatalf("expected turn summary block, got %#v", blocks)
	}
	if got := summary.AssistantVisibleOutput; got != "Parking for manual review." {
		t.Fatalf("assistant_visible_output = %q", got)
	}
	if got := summary.Outcome; got != "14-day review scheduled." {
		t.Fatalf("outcome = %q", got)
	}
	if len(summary.ProgressUpdates) != 1 || summary.ProgressUpdates[0] != "Scheduling the follow-up review." {
		t.Fatalf("progress_updates = %#v", summary.ProgressUpdates)
	}
	if len(summary.ToolResults) != 1 {
		t.Fatalf("tool_results = %#v", summary.ToolResults)
	}
	if summary.ToolResults[0].ToolName != "schedule" {
		t.Fatalf("tool_result = %#v", summary.ToolResults[0])
	}
}

func TestDecodeCanonicalTurnSummaryBlocks_FailsWhenSummaryBearingBlocksLackCanonicalSummary(t *testing.T) {
	_, ok, err := DecodeCanonicalTurnSummaryBlocks([]TurnBlock{
		{Kind: "assistant_text", Text: "Parking for manual review."},
		{Kind: "outcome", Text: "14-day review scheduled."},
	})
	if err == nil || !strings.Contains(err.Error(), "missing canonical turn_summary for summary-bearing turn blocks") {
		t.Fatalf("DecodeCanonicalTurnSummaryBlocks error = %v", err)
	}
	if ok {
		t.Fatal("expected missing canonical summary not to decode successfully")
	}
}

func TestDecodeCanonicalTurnSummaryBlocks_FailsOnDuplicateCanonicalSummary(t *testing.T) {
	_, ok, err := DecodeCanonicalTurnSummaryBlocks([]TurnBlock{
		newTurnSummaryTurnBlock(TurnSummaryTurnBlockData{AssistantVisibleOutput: "one", Outcome: "one"}),
		newTurnSummaryTurnBlock(TurnSummaryTurnBlockData{AssistantVisibleOutput: "two", Outcome: "two"}),
	})
	if err == nil || !strings.Contains(err.Error(), "multiple canonical turn_summary blocks") {
		t.Fatalf("DecodeCanonicalTurnSummaryBlocks error = %v", err)
	}
	if ok {
		t.Fatal("expected duplicate canonical summary not to decode successfully")
	}
}

func TestDecodeCanonicalRuntimeLogTurnBlocks_FailsOnMissingCanonicalDetailsComponent(t *testing.T) {
	_, err := DecodeCanonicalRuntimeLogTurnBlocks([]TurnBlock{
		newRuntimeLogTurnBlock("runtime log", TurnBlockRuntimeLogData{
			LogLevel: "warn",
			Message:  "runtime log",
			Details:  json.RawMessage(`{"action":"tool_execution_denied"}`),
		}),
	})
	if err == nil || !strings.Contains(err.Error(), "canonical runtime_log block details.component is required") {
		t.Fatalf("DecodeCanonicalRuntimeLogTurnBlocks error = %v", err)
	}
}

func TestDecodeCanonicalRuntimeLogTurnBlocks_FailsOnNonStringCanonicalDetailsComponent(t *testing.T) {
	_, err := DecodeCanonicalRuntimeLogTurnBlocks([]TurnBlock{
		newRuntimeLogTurnBlock("runtime log", TurnBlockRuntimeLogData{
			LogLevel: "warn",
			Message:  "runtime log",
			Details:  json.RawMessage(`{"component":123,"action":"tool_execution_denied"}`),
		}),
	})
	if err == nil || !strings.Contains(err.Error(), "canonical runtime_log block details.component must be a string") {
		t.Fatalf("DecodeCanonicalRuntimeLogTurnBlocks error = %v", err)
	}
}
