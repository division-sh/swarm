package runtime

import (
	"context"
	"encoding/json"
	"testing"
)

func TestConversation_MiscHelpers(t *testing.T) {
	rt := &fakeRuntime{}
	c := NewConversation("a1", "t1", "sys", nil, SessionScoped, 10, rt)

	// SetMaxToolRounds (covers defaulting + setter).
	c.SetMaxToolRounds(-1)
	if c.maxToolRounds <= 0 {
		t.Fatalf("expected positive maxToolRounds, got %d", c.maxToolRounds)
	}

	// AppendFeedback should append system message.
	c.AppendFeedback("  hello  ")
	if len(c.Messages) == 0 || c.Messages[len(c.Messages)-1].Role != "system" {
		t.Fatalf("expected system feedback message, got %+v", c.Messages)
	}

	// AppendResult should append tool message.
	c.AppendResult(ToolResult{Name: "t", Payload: "{\"ok\":true}"})
	if len(c.Messages) == 0 || c.Messages[len(c.Messages)-1].Role != "tool" {
		t.Fatalf("expected tool message after AppendResult, got %+v", c.Messages)
	}

	// Reset clears state.
	c.Reset()
	if c.Session != nil || c.TurnCount != 0 || len(c.Messages) != 0 {
		t.Fatalf("expected reset to clear session/messages/turns, got session=%v turns=%d msgs=%d", c.Session != nil, c.TurnCount, len(c.Messages))
	}

	// InjectAsyncToolResult should create a tool message JSON payload.
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
