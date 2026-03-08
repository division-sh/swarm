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

func TestSummarizeMonitorEventLine_Assistant(t *testing.T) {
	got := summarizeMonitorEventLine([]byte(`{"type":"assistant","message":{"content":[{"type":"text","text":"working"},{"type":"tool_use","name":"emit_event","input":{"ok":true}}]}}`))
	want := "assistant: working tools=emit_event"
	if got != want {
		t.Fatalf("unexpected monitor summary: got=%q want=%q", got, want)
	}
}
