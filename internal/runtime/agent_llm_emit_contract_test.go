package runtime

import (
	"context"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
)

type scriptedToolCallRuntime struct {
	responses []Response
	idx       int
}

func (r *scriptedToolCallRuntime) StartSession(_ context.Context, agentID, _ string, _ []ToolDefinition) (*Session, error) {
	return &Session{ID: "sess-scripted", AgentID: agentID, RuntimeMode: "api"}, nil
}

func (r *scriptedToolCallRuntime) ContinueSession(_ context.Context, _ *Session, _ Message) (*Response, error) {
	if len(r.responses) == 0 {
		return &Response{Message: Message{Role: "assistant", Content: "ok"}}, nil
	}
	if r.idx >= len(r.responses) {
		last := r.responses[len(r.responses)-1]
		if strings.TrimSpace(last.Message.Role) == "" {
			last.Message.Role = "assistant"
		}
		return &last, nil
	}
	resp := r.responses[r.idx]
	r.idx++
	if strings.TrimSpace(resp.Message.Role) == "" {
		resp.Message.Role = "assistant"
	}
	return &resp, nil
}

func TestLLMAgentOnEvent_EmitToolRecoveryAfterInitialSchemaError(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	cfg := models.AgentConfig{
		ID:            "empire-coordinator",
		Type:          "worker",
		Role:          "empire-coordinator",
		Mode:          "holding",
		Subscriptions: []string{"system.directive"},
		Config:        mustJSON(map[string]any{"system_prompt": "coordinator"}),
	}

	rt := &scriptedToolCallRuntime{
		responses: []Response{
			{
				Message: Message{Role: "assistant", Content: "attempt 1"},
				ToolCalls: []ToolCall{
					{
						Name:      "emit_scan_requested",
						Arguments: map[string]any{"priority": "normal"}, // missing required mode
					},
				},
			},
			{
				Message: Message{Role: "assistant", Content: "attempt 2"},
				ToolCalls: []ToolCall{
					{
						Name: "emit_scan_requested",
						Arguments: map[string]any{
							"mode":      "saas_gap",
							"geography": "paraguay",
						},
					},
				},
			},
			{
				Message: Message{Role: "assistant", Content: "done"},
			},
		},
	}

	agent := NewLLMAgent(cfg, rt, exec, exec.ToolDefinitions())
	_, err := agent.OnEvent(context.Background(), events.Event{
		ID:          "evt-recovery-1",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})
	if err != nil {
		t.Fatalf("OnEvent should succeed after recovery, got %v", err)
	}

	found := 0
	for _, evt := range store.events {
		if string(evt.Type) == "scan.requested" {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("expected exactly one scan.requested emission after recovery, got %d", found)
	}
}

func TestLLMAgentOnEvent_EmitToolFailureWithoutRecoveryFailsContract(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	cfg := models.AgentConfig{
		ID:            "empire-coordinator",
		Type:          "worker",
		Role:          "empire-coordinator",
		Mode:          "holding",
		Subscriptions: []string{"system.directive"},
		Config:        mustJSON(map[string]any{"system_prompt": "coordinator"}),
	}

	rt := &scriptedToolCallRuntime{
		responses: []Response{
			{
				Message: Message{Role: "assistant", Content: "attempt only"},
				ToolCalls: []ToolCall{
					{
						Name:      "emit_scan_requested",
						Arguments: map[string]any{"priority": "normal"}, // still missing required mode
					},
				},
			},
			{
				Message: Message{Role: "assistant", Content: "done"},
			},
		},
	}

	agent := NewLLMAgent(cfg, rt, exec, exec.ToolDefinitions())
	_, err := agent.OnEvent(context.Background(), events.Event{
		ID:          "evt-recovery-2",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})
	if err == nil || !strings.Contains(err.Error(), "must emit scan.requested") {
		t.Fatalf("expected post-turn contract failure, got %v", err)
	}

	for _, evt := range store.events {
		if string(evt.Type) == "scan.requested" {
			t.Fatalf("did not expect scan.requested emission on unrecovered failure")
		}
	}
}

func TestLLMAgentOnEvent_RemediatesWhenInitialTurnOmitsRequiredEmitTool(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	cfg := models.AgentConfig{
		ID:            "empire-coordinator",
		Type:          "worker",
		Role:          "empire-coordinator",
		Mode:          "holding",
		Subscriptions: []string{"system.directive"},
		Config:        mustJSON(map[string]any{"system_prompt": "coordinator"}),
	}

	// First response omits tool usage. Remediation turn should force tool use.
	rt := &scriptedToolCallRuntime{
		responses: []Response{
			{Message: Message{Role: "assistant", Content: "Acknowledged; I will proceed."}},
			{
				Message: Message{Role: "assistant", Content: "tool remediation"},
				ToolCalls: []ToolCall{
					{
						Name: "emit_scan_requested",
						Arguments: map[string]any{
							"mode":      "saas_gap",
							"priority":  "normal",
							"geography": "paraguay",
						},
					},
				},
			},
			{Message: Message{Role: "assistant", Content: "done"}},
		},
	}

	agent := NewLLMAgent(cfg, rt, exec, exec.ToolDefinitions())
	_, err := agent.OnEvent(context.Background(), events.Event{
		ID:          "evt-remediate-plain",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})
	if err != nil {
		t.Fatalf("OnEvent should succeed via remediation, got %v", err)
	}

	found := 0
	for _, evt := range store.events {
		if string(evt.Type) == "scan.requested" {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("expected one scan.requested after remediation, got %d", found)
	}
}
