package runtime

import (
	"context"
	"encoding/json"
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
