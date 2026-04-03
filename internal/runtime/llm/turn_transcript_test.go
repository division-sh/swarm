package llm

import "testing"

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
