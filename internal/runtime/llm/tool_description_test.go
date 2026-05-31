package llm

import (
	"context"
	"strings"
	"testing"

	"swarm/internal/config"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/sessions"
)

func TestDeliveredToolDescription_AppendsUsageWithoutProtocolExtension(t *testing.T) {
	def := ToolDefinition{
		Name:        "query_entities",
		Description: "Query entity_state rows.",
		Usage:       "Use CEL equality with ==.",
	}

	got := DeliveredToolDescription(def)
	if !strings.Contains(got, "Query entity_state rows.\n\nUsage:\nUse CEL equality with ==.") {
		t.Fatalf("delivered description = %q", got)
	}
}

func TestDeliveredToolDescription_DoesNotDuplicateUsageBlock(t *testing.T) {
	def := ToolDefinition{
		Name:        "query_entities",
		Description: "Query entity_state rows.\n\nUsage:\nUse CEL equality with ==.",
		Usage:       "Use CEL equality with ==.",
	}

	got := DeliveredToolDescription(def)
	if strings.Count(got, "Usage:") != 1 {
		t.Fatalf("delivered description duplicated usage block: %q", got)
	}
}

func TestAnthropicAPIRuntimeBuildRequest_DeliversUsageInToolDescription(t *testing.T) {
	runtime := NewAnthropicAPIRuntime(&config.Config{
		LLM: config.LLMConfig{},
	}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil)
	session := &Session{
		ID: "session-1",
		Messages: []Message{{
			Role:    "user",
			Content: "work",
		}},
		Tools: []ToolDefinition{{
			Name:        "query_entities",
			Description: "Query entity_state rows.",
			Usage:       "Use CEL equality with ==.",
		}},
	}

	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{ID: "agent-1", Model: "regular"})
	req, err := runtime.buildRequest(ctx, session, Message{Role: "user", Content: "continue"})
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	if len(req.Tools) != 1 {
		t.Fatalf("tool count = %d, want 1", len(req.Tools))
	}
	if !strings.Contains(req.Tools[0].Description, "\n\nUsage:\nUse CEL equality with ==.") {
		t.Fatalf("tool description = %q, want usage block", req.Tools[0].Description)
	}
}

func TestBuildInitialPrompt_DeliversUsageInPromptTransportFallback(t *testing.T) {
	prompt := buildInitialPrompt(&Session{
		SystemPrompt: "system",
		Tools: []ToolDefinition{{
			Name:        "query_entities",
			Description: "Query entity_state rows.",
			Usage:       "Use CEL equality with ==.",
		}},
	}, "continue")

	if !strings.Contains(prompt, "- query_entities: Query entity_state rows.\n\nUsage:\nUse CEL equality with ==.") {
		t.Fatalf("prompt = %q, want delivered usage in tool description", prompt)
	}
}
