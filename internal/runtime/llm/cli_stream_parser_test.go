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
	if len(resp.ObservedToolCalls) != 1 || resp.ObservedToolCalls[0].Name != "sql_execute" {
		t.Fatalf("unexpected observed tool calls: %+v", resp.ObservedToolCalls)
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
	if resp.ToolCalls[1].Name != "emit_category_assessed" {
		t.Fatalf("unexpected second tool name: %+v", resp.ToolCalls[1])
	}
	if got := resp.MCPServers["runtime-tools"]; got != "connected" {
		t.Fatalf("mcp servers = %#v", resp.MCPServers)
	}
	if len(resp.VisibleTools) != 2 || resp.VisibleTools[0] != "query_metrics" || resp.VisibleTools[1] != "read_file" {
		t.Fatalf("visible tools = %#v", resp.VisibleTools)
	}
	if len(resp.MCPVisibleTools) != 1 || resp.MCPVisibleTools[0] != "mcp__runtime-tools__query_metrics" {
		t.Fatalf("mcp visible tools = %#v", resp.MCPVisibleTools)
	}
}

func TestCLIStreamAccumulator_DoesNotPromoteAttemptedStreamedToolUseIntoVisibleSurface(t *testing.T) {
	acc := newCLIStreamAccumulator()
	acc.AddLine([]byte(`{"type":"system","subtype":"init","session_id":"sess-attempted","mcp_servers":[{"name":"runtime-tools","status":"failed"}],"tools":["ExitPlanMode"]}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","name":"mcp__runtime-tools__read_file","input":{}}},"session_id":"sess-attempted"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"/workspace/corpus.json\"}"}},"session_id":"sess-attempted"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_stop","index":2},"session_id":"sess-attempted"}`))

	resp := acc.Response()
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "read_file" {
		t.Fatalf("tool calls = %#v", resp.ToolCalls)
	}
	if len(resp.VisibleTools) != 0 {
		t.Fatalf("visible tools = %#v", resp.VisibleTools)
	}
	if len(resp.MCPVisibleTools) != 0 {
		t.Fatalf("mcp visible tools = %#v", resp.MCPVisibleTools)
	}
}

func TestCLIStreamAccumulator_IgnoresAssistantToolCallsWhenMCPConnected(t *testing.T) {
	acc := newCLIStreamAccumulator()
	acc.AddLine([]byte(`{"type":"system","subtype":"init","session_id":"sess-3","mcp_servers":[{"name":"runtime-tools","status":"connected"}],"tools":["mcp__runtime-tools__query_metrics"]}`))
	acc.AddLine([]byte(`{"type":"assistant","message":{"content":[{"type":"text","text":"I attempted emit_scan_requested"},{"type":"tool_use","name":"emit_scan_requested","input":{"mode":"corpus"}}]}}`))

	resp := acc.Response()
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("expected assistant-object tool calls to be ignored when MCP is connected, got %+v", resp.ToolCalls)
	}
	if len(resp.ObservedToolCalls) != 1 || resp.ObservedToolCalls[0].Name != "emit_scan_requested" {
		t.Fatalf("expected observed assistant tool call to remain available for persistence, got %+v", resp.ObservedToolCalls)
	}
}

func TestCLIStreamAccumulator_SuppressesCompletedMCPToolCalls(t *testing.T) {
	acc := newCLIStreamAccumulator()
	acc.AddLine([]byte(`{"type":"system","subtype":"init","session_id":"sess-4","mcp_servers":[{"name":"runtime-tools","status":"connected"}],"tools":["mcp__runtime-tools__emit_scan_requested"]}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_123","name":"mcp__runtime-tools__emit_scan_requested","input":{}}},"session_id":"sess-4"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"mode\":\"corpus\"}"}},"session_id":"sess-4"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_stop","index":2},"session_id":"sess-4"}`))
	acc.AddLine([]byte(`{"type":"user","content":[{"type":"tool_result","tool_use_id":"toolu_123","content":[{"type":"text","text":"{\"event_id\":\"evt-1\",\"status\":\"published\"}"}]}]}`))

	resp := acc.Response()
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("expected completed MCP tool call to be suppressed, got %+v", resp.ToolCalls)
	}
	if len(resp.ObservedToolCalls) != 1 || resp.ObservedToolCalls[0].Name != "emit_scan_requested" {
		t.Fatalf("expected completed MCP tool call to remain observed for persistence, got %+v", resp.ObservedToolCalls)
	}
}

func TestCLIStreamAccumulator_SuppressesCompletedMCPToolCalls_FromMessageWrappedUserEvent(t *testing.T) {
	acc := newCLIStreamAccumulator()
	acc.AddLine([]byte(`{"type":"system","subtype":"init","session_id":"sess-5","mcp_servers":[{"name":"runtime-tools","status":"connected"}],"tools":["mcp__runtime-tools__emit_scan_requested"]}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_123","name":"mcp__runtime-tools__emit_scan_requested","input":{}}},"session_id":"sess-5"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"mode\":\"corpus\"}"}},"session_id":"sess-5"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_stop","index":2},"session_id":"sess-5"}`))
	acc.AddLine([]byte(`{"type":"user","message":{"role":"user","content":[{"tool_use_id":"toolu_123","type":"tool_result","content":[{"type":"text","text":"{\"event_id\":\"evt-1\",\"status\":\"published\"}"}]}]}}`))

	resp := acc.Response()
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("expected wrapped completed MCP tool call to be suppressed, got %+v", resp.ToolCalls)
	}
	if len(resp.ObservedToolCalls) != 1 || resp.ObservedToolCalls[0].Name != "emit_scan_requested" {
		t.Fatalf("expected wrapped completed MCP tool call to remain observed for persistence, got %+v", resp.ObservedToolCalls)
	}
}

func TestSummarizeMonitorEventLine_Assistant(t *testing.T) {
	got := summarizeMonitorEventLine([]byte(`{"type":"assistant","message":{"content":[{"type":"text","text":"working"},{"type":"tool_use","name":"emit_event","input":{"ok":true}}]}}`))
	want := "assistant: working tools=emit_event"
	if got != want {
		t.Fatalf("unexpected monitor summary: got=%q want=%q", got, want)
	}
}

func TestParseCLIResponse_NormalizesBuiltinToolCallNames(t *testing.T) {
	resp := parseCLIResponse([]byte(`{"content":[{"type":"tool_use","name":"Read","input":{"path":"/tmp/a.txt"}},{"type":"tool_use","name":"Edit","input":{"path":"/tmp/a.txt","content":"x"}}]}`))
	if resp == nil {
		t.Fatal("expected response")
	}
	if len(resp.ToolCalls) != 2 {
		t.Fatalf("tool calls = %#v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].Name != "read_file" {
		t.Fatalf("first tool call = %#v", resp.ToolCalls[0])
	}
	if resp.ToolCalls[1].Name != "write_file" {
		t.Fatalf("second tool call = %#v", resp.ToolCalls[1])
	}
}

func TestParseCLIResponse_PreservesDuplicateToolCalls(t *testing.T) {
	resp := parseCLIResponse([]byte(`{"content":[{"type":"tool_use","name":"Read","input":{"path":"/tmp/a.txt"}},{"type":"tool_use","name":"Read","input":{"path":"/tmp/a.txt"}}]}`))
	if resp == nil {
		t.Fatal("expected response")
	}
	if len(resp.ToolCalls) != 2 {
		t.Fatalf("tool calls = %#v", resp.ToolCalls)
	}
	if len(resp.ObservedToolCalls) != 2 {
		t.Fatalf("observed tool calls = %#v", resp.ObservedToolCalls)
	}
}

func TestCLIStreamAccumulator_PreservesDuplicateObservedToolCallsForPersistence(t *testing.T) {
	acc := newCLIStreamAccumulator()
	acc.AddLine([]byte(`{"type":"system","subtype":"init","session_id":"sess-dup","mcp_servers":[{"name":"runtime-tools","status":"connected"}],"tools":["mcp__runtime-tools__emit_scan_requested"]}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_1","name":"mcp__runtime-tools__emit_scan_requested","input":{"mode":"corpus"}}},"session_id":"sess-dup"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_stop","index":2},"session_id":"sess-dup"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_start","index":3,"content_block":{"type":"tool_use","id":"toolu_2","name":"mcp__runtime-tools__emit_scan_requested","input":{"mode":"corpus"}}},"session_id":"sess-dup"}`))
	acc.AddLine([]byte(`{"type":"stream_event","event":{"type":"content_block_stop","index":3},"session_id":"sess-dup"}`))
	acc.AddLine([]byte(`{"type":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":[{"type":"text","text":"{\"event_id\":\"evt-1\"}"}]},{"type":"tool_result","tool_use_id":"toolu_2","content":[{"type":"text","text":"{\"event_id\":\"evt-2\"}"}]}]}`))

	resp := acc.Response()
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("expected completed MCP tool calls to stay suppressed for execution, got %+v", resp.ToolCalls)
	}
	if len(resp.ObservedToolCalls) != 2 {
		t.Fatalf("observed tool calls = %#v", resp.ObservedToolCalls)
	}
	if resp.ObservedToolCalls[0].Name != "emit_scan_requested" || resp.ObservedToolCalls[1].Name != "emit_scan_requested" {
		t.Fatalf("observed tool calls = %#v", resp.ObservedToolCalls)
	}
}

func TestCLIStreamAccumulator_IgnoresNestedToolArgumentListsForVisibility(t *testing.T) {
	acc := newCLIStreamAccumulator()
	acc.AddLine([]byte(`{"type":"system","subtype":"init","session_id":"sess-nested","tools":["Read"]}`))
	acc.AddLine([]byte(`{"type":"assistant","message":{"content":[{"type":"tool_use","name":"agent_hire","input":{"tools":["bash","web_search"]}}]}}`))

	resp := acc.Response()
	if len(resp.VisibleTools) != 1 || resp.VisibleTools[0] != "read_file" {
		t.Fatalf("visible tools = %#v", resp.VisibleTools)
	}
}

func TestCLIStreamAccumulator_IgnoresCLIControlToolsForVisibility(t *testing.T) {
	acc := newCLIStreamAccumulator()
	acc.AddLine([]byte(`{"type":"system","subtype":"init","session_id":"sess-control","tools":["Read","ExitPlanMode","mcp__runtime-tools__emit_category_assessed"]}`))

	resp := acc.Response()
	if len(resp.VisibleTools) != 2 || resp.VisibleTools[0] != "emit_category_assessed" || resp.VisibleTools[1] != "read_file" {
		t.Fatalf("visible tools = %#v", resp.VisibleTools)
	}
}
