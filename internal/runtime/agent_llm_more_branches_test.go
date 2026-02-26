package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
)

func TestAllowedTools_ExtractionAndFiltering(t *testing.T) {
	cfg := models.AgentConfig{
		ID: "a1",
		Config: []byte(`{
			"system_prompt": "x",
			"tools": ["t1", "t2", ""],
			"allowed_tools": ["t3", "t2", " "]
		}`),
	}
	allowed, constrained := extractAllowedToolSet(cfg)
	if !constrained {
		t.Fatal("expected constrained=true when tools list present")
	}
	for _, k := range []string{"t1", "t2", "t3"} {
		if _, ok := allowed[k]; !ok {
			t.Fatalf("expected allowed to include %q", k)
		}
	}

	in := []ToolDefinition{{Name: "t1"}, {Name: "t2"}, {Name: "t9"}}
	out := filterTools(in, allowed, true)
	if len(out) != 2 {
		t.Fatalf("expected 2 tools after filtering, got %d", len(out))
	}
	if out[0].Name != "t1" || out[1].Name != "t2" {
		t.Fatalf("unexpected filtered tools: %+v", out)
	}

	// Unconstrained mode passes through unchanged.
	out2 := filterTools(in, map[string]struct{}{}, false)
	if len(out2) != len(in) || out2[0].Name != in[0].Name || out2[2].Name != in[2].Name {
		t.Fatalf("expected pass-through when unconstrained, got %+v", out2)
	}
}

func TestAllowedTools_InvalidConfig_DoesNotConstrain(t *testing.T) {
	cfg := models.AgentConfig{Config: []byte("{")}
	allowed, constrained := extractAllowedToolSet(cfg)
	if constrained {
		t.Fatal("expected constrained=false for invalid json")
	}
	if len(allowed) != 0 {
		t.Fatalf("expected empty allowed set, got %+v", allowed)
	}

	cfg2 := models.AgentConfig{Config: []byte(`{"tools":"not-an-array"}`)}
	_, constrained = extractAllowedToolSet(cfg2)
	if constrained {
		t.Fatal("expected constrained=false when tools is not an array")
	}
}

func TestInjectHumanTaskToolResult_Branches(t *testing.T) {
	conv := &Conversation{
		AgentID: "a1",
		Session: &Session{ID: "s1", AgentID: "a1"},
	}
	agent := &LLMAgent{cfg: models.AgentConfig{ID: "a1"}, conversation: conv}

	// Empty payload -> no-op.
	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.completed"})
	if len(conv.Messages) != 0 {
		t.Fatalf("expected no messages, got %d", len(conv.Messages))
	}

	// Invalid JSON -> no-op.
	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.completed", Payload: []byte("{")})
	if len(conv.Messages) != 0 {
		t.Fatalf("expected no messages after invalid json, got %d", len(conv.Messages))
	}

	// Wrong requesting agent -> no-op.
	pWrong, _ := json.Marshal(map[string]any{"requesting_agent": "other", "task_id": "t1"})
	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.completed", Payload: pWrong})
	if len(conv.Messages) != 0 {
		t.Fatalf("expected no messages after wrong agent, got %d", len(conv.Messages))
	}

	// Rejected with explicit reason.
	pReject, _ := json.Marshal(map[string]any{"requesting_agent": "a1", "task_id": "t2", "rejection_reason": "nope"})
	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.rejected", Payload: pReject})
	if len(conv.Messages) != 1 {
		t.Fatalf("expected 1 injected message, got %d", len(conv.Messages))
	}
	if conv.Messages[0].Role != "tool" || !strings.Contains(conv.Messages[0].Content, `"name":"human_task_request"`) {
		t.Fatalf("unexpected injected message: %+v", conv.Messages[0])
	}
	if !strings.Contains(conv.Messages[0].Content, `"ok":false`) || !strings.Contains(conv.Messages[0].Content, "nope") {
		t.Fatalf("expected rejection details in payload, got %q", conv.Messages[0].Content)
	}

	// Expired without explicit reason -> default error text.
	pExpire, _ := json.Marshal(map[string]any{"requesting_agent": "a1", "task_id": "t3"})
	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.expired", Payload: pExpire})
	if len(conv.Messages) != 2 {
		t.Fatalf("expected 2 injected messages, got %d", len(conv.Messages))
	}
	if !strings.Contains(conv.Messages[1].Content, "human task expired") {
		t.Fatalf("expected default expiry message, got %q", conv.Messages[1].Content)
	}

	// Completed -> ok=true and no error field.
	pDone, _ := json.Marshal(map[string]any{"requesting_agent": "a1", "task_id": "t4"})
	_ = agent.injectHumanTaskToolResult(context.Background(), events.Event{Type: "human_task.completed", Payload: pDone})
	if len(conv.Messages) != 3 {
		t.Fatalf("expected 3 injected messages, got %d", len(conv.Messages))
	}
	if !strings.Contains(conv.Messages[2].Content, `"ok":true`) {
		t.Fatalf("expected ok=true, got %q", conv.Messages[2].Content)
	}
}

