package llm

import "testing"

func TestCLIStreamAccumulator_Response(t *testing.T) {
	acc := newCLIStreamAccumulator()
	acc.AddLine([]byte(`{"type":"system","subtype":"init","session_id":"sess-1"}`))
	acc.AddLine([]byte(`{"type":"assistant","message":{"content":[{"type":"text","text":"hello"},{"type":"tool_use","name":"sql_execute","input":{"query":"select 1"}}]}}`))
	acc.AddLine([]byte(`{"type":"result","session_id":"sess-1","result":"completed"}`))

	resp := acc.Response()
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.SessionID != "sess-1" {
		t.Fatalf("unexpected session id: %q", resp.SessionID)
	}
	if resp.Message.Content != "hello" {
		t.Fatalf("unexpected message content: %q", resp.Message.Content)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "sql_execute" {
		t.Fatalf("unexpected tool calls: %+v", resp.ToolCalls)
	}
	if len(resp.Raw) == 0 {
		t.Fatal("expected raw stream payload")
	}
}

func TestCLIStreamAccumulator_ReconstructsStreamedToolInput(t *testing.T) {
	acc := newCLIStreamAccumulator()
	acc.AddLine([]byte(`{"type":"system","subtype":"init","session_id":"sess-2","mcp_servers":[{"name":"runtime-tools","status":"connected"}],"tools":["Read","mcp__runtime-tools__query_metrics"]}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","name":"emit_category_assessed","input":{}}},"session_id":"sess-2"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"opportunity_name\":\"Lease\""}},"session_id":"sess-2"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":",\"score\":65}"}},"session_id":"sess-2"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_stop","index":2},"session_id":"sess-2"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_start","index":3,"content_block":{"type":"tool_use","name":"mcp__runtime-tools__emit_category_assessed","input":{}}},"session_id":"sess-2"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_stop","index":3},"session_id":"sess-2"}`))

	resp := acc.Response()
	if resp == nil {
		t.Fatal("expected response")
	}
	if len(resp.ToolCalls) != 2 {
		t.Fatalf("unexpected tool calls: %+v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].Name != "emit_category_assessed" {
		t.Fatalf("unexpected tool name: %+v", resp.ToolCalls[0])
	}
	args, ok := resp.ToolCalls[0].Arguments.(map[string]any)
	if !ok {
		t.Fatalf("expected object arguments, got %#v", resp.ToolCalls[0].Arguments)
	}
	if got := args["opportunity_name"]; got != "Lease" {
		t.Fatalf("unexpected opportunity_name: %#v", got)
	}
	if got := args["score"]; got != float64(65) {
		t.Fatalf("unexpected score: %#v", got)
	}
	if resp.ToolCalls[1].Name != "mcp__runtime-tools__emit_category_assessed" {
		t.Fatalf("unexpected second tool name: %+v", resp.ToolCalls[1])
	}
	if got := resp.MCPServers["runtime-tools"]; got != "connected" {
		t.Fatalf("mcp servers = %#v", resp.MCPServers)
	}
	if len(resp.MCPVisibleTools) != 2 || resp.MCPVisibleTools[0] != "mcp__runtime-tools__emit_category_assessed" || resp.MCPVisibleTools[1] != "mcp__runtime-tools__query_metrics" {
		t.Fatalf("mcp visible tools = %#v", resp.MCPVisibleTools)
	}
}

func TestSummarizeMonitorEventLine_Assistant(t *testing.T) {
	got := summarizeMonitorEventLine([]byte(`{"type":"assistant","message":{"content":[{"type":"text","text":"working"},{"type":"tool_use","name":"emit_event","input":{"ok":true}}]}}`))
	want := "assistant: working tools=emit_event"
	if got != want {
		t.Fatalf("unexpected monitor summary: got=%q want=%q", got, want)
	}
}
