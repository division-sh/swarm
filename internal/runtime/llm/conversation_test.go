package llm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type fakeRuntime struct {
	calls    int
	seenMsgs []Message
}

func (f *fakeRuntime) StartSession(_ context.Context, agentID, _ string, _ []ToolDefinition) (*Session, error) {
	return &Session{ID: "sess-1", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (f *fakeRuntime) ContinueSession(_ context.Context, s *Session, message Message) (*Response, error) {
	f.calls++
	f.seenMsgs = append(f.seenMsgs, message)
	switch f.calls {
	case 1:
		return &Response{
			Message: Message{Role: "assistant", Content: "running tool"},
			ToolCalls: []ToolCall{
				{Name: "echo", Arguments: map[string]any{"text": "hello"}},
			},
		}, nil
	case 2:
		s.TurnCount++
		return &Response{Message: Message{Role: "assistant", Content: "done"}}, nil
	default:
		return &Response{Message: Message{Role: "assistant", Content: "extra"}}, nil
	}
}

type fakeToolExec struct {
	calls int
}

func (f *fakeToolExec) Execute(_ context.Context, name string, input any) (any, error) {
	f.calls++
	return map[string]any{"name": name, "input": input}, nil
}

type panicToolExec struct{}

func (panicToolExec) Execute(context.Context, string, any) (any, error) {
	panic("boom")
}

type largeToolExec struct {
	payload any
}

func (l largeToolExec) Execute(context.Context, string, any) (any, error) {
	return l.payload, nil
}

func testAsString(v any) string {
	s, _ := v.(string)
	return s
}

func TestConversationStep_ResolvesToolCalls(t *testing.T) {
	rt := &fakeRuntime{}
	te := &fakeToolExec{}
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 10, rt)
	c.SetToolExecutor(te)

	resp, err := c.Step(context.Background(), "start")
	if err != nil {
		t.Fatalf("step error: %v", err)
	}
	if resp.Message.Content != "done" {
		t.Fatalf("expected final assistant reply, got: %q", resp.Message.Content)
	}
	if te.calls != 1 {
		t.Fatalf("expected 1 tool execution, got %d", te.calls)
	}
	if rt.calls != 2 {
		t.Fatalf("expected 2 runtime calls, got %d", rt.calls)
	}
	if len(rt.seenMsgs) != 2 {
		t.Fatalf("expected 2 seen messages, got %d", len(rt.seenMsgs))
	}
	if rt.seenMsgs[1].Role != "tool" {
		t.Fatalf("expected second message role tool, got %s", rt.seenMsgs[1].Role)
	}
	var payload []map[string]any
	if err := json.Unmarshal([]byte(rt.seenMsgs[1].Content), &payload); err != nil {
		t.Fatalf("tool payload not json: %v", err)
	}
	if len(payload) != 1 {
		t.Fatalf("expected one tool payload entry, got %d", len(payload))
	}
}

func TestConversationStep_NoExecutorReturnsInitialToolCall(t *testing.T) {
	rt := &fakeRuntime{}
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 10, rt)

	resp, err := c.Step(context.Background(), "start")
	if err != nil {
		t.Fatalf("step error: %v", err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("expected tool call passthrough, got %d", len(resp.ToolCalls))
	}
	if rt.calls != 1 {
		t.Fatalf("expected one runtime call, got %d", rt.calls)
	}
}

func TestConversation_MiscHelpers(t *testing.T) {
	rt := &fakeRuntime{}
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 10, rt)

	c.SetMaxToolRounds(-1)
	if c.maxToolRounds <= 0 {
		t.Fatalf("expected positive maxToolRounds, got %d", c.maxToolRounds)
	}

	c.AppendFeedback("  hello  ")
	if len(c.Messages) == 0 || c.Messages[len(c.Messages)-1].Role != "system" {
		t.Fatalf("expected system feedback message, got %+v", c.Messages)
	}

	c.AppendResult(ToolResult{Name: "t", Payload: "{\"ok\":true}"})
	if len(c.Messages) == 0 || c.Messages[len(c.Messages)-1].Role != "tool" {
		t.Fatalf("expected tool message after AppendResult, got %+v", c.Messages)
	}

	c.Reset()
	if c.Session != nil || c.TurnCount != 0 || len(c.Messages) != 0 {
		t.Fatalf("expected reset to clear session/messages/turns, got session=%v turns=%d msgs=%d", c.Session != nil, c.TurnCount, len(c.Messages))
	}

	if err := c.InjectAsyncToolResult(context.Background(), "x", true, map[string]any{"ok": true}, ""); err != nil {
		t.Fatalf("InjectAsyncToolResult: %v", err)
	}
	if len(c.Messages) != 1 || c.Messages[0].Role != "tool" {
		t.Fatalf("expected tool message, got %+v", c.Messages)
	}
	var arr []map[string]any
	if err := json.Unmarshal([]byte(c.Messages[0].Content), &arr); err != nil || len(arr) != 1 {
		t.Fatalf("expected json array tool result, err=%v content=%q", err, c.Messages[0].Content)
	}
}

func TestConversation_ExecuteToolCalls_RecoversPanic(t *testing.T) {
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 4, &fakeRuntime{})
	c.SetToolExecutor(panicToolExec{})

	raw := c.executeToolCalls(context.Background(), []ToolCall{{Name: "sql_execute", Arguments: map[string]any{"query": "select 1"}}})
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		t.Fatalf("unmarshal tool payload: %v", err)
	}
	if len(arr) != 1 || arr[0]["ok"] != false {
		t.Fatalf("expected panic to be captured as error payload, got %#v", arr)
	}
	if !strings.Contains(strings.ToLower(testAsString(arr[0]["error"])), "tool panic") {
		t.Fatalf("expected tool panic text, got %#v", arr[0]["error"])
	}
}

func TestConversation_ExecuteToolCalls_TruncatesLargeResult(t *testing.T) {
	huge := strings.Repeat("x", maxToolResultBytes+1024)
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 4, &fakeRuntime{})
	c.SetToolExecutor(largeToolExec{payload: map[string]any{"blob": huge}})

	raw := c.executeToolCalls(context.Background(), []ToolCall{{Name: "sql_execute", Arguments: map[string]any{"query": "select 1"}}})
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		t.Fatalf("unmarshal tool payload: %v", err)
	}
	if len(arr) != 1 || arr[0]["ok"] != true {
		t.Fatalf("expected successful tool payload, got %#v", arr)
	}
	resultMap, _ := arr[0]["result"].(map[string]any)
	if resultMap == nil || resultMap["truncated"] != true {
		t.Fatalf("expected truncated result metadata, got %#v", arr[0]["result"])
	}
}
