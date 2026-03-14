package agents

import (
	"encoding/json"
	"strings"
	"testing"

	"empireai/internal/events"
	models "empireai/internal/runtime/core/actors"
	llm "empireai/internal/runtime/llm"
	runtimetools "empireai/internal/runtime/tools"
)

func TestFormatEventForAgent_IncludesEntityToolsLine(t *testing.T) {
	cfg := models.AgentConfig{
		ID:   "agent-1",
		Role: "operator",
		Mode: "task",
	}
	evt := (events.Event{
		ID:          "evt-1",
		Type:        "item.created",
		SourceAgent: "runtime",
		TaskID:      "task-1",
		Payload:     []byte(`{"item_id":"x"}`),
	}).WithEntityID("entity-1")

	formatted := formatEventForAgent(cfg, evt)
	if !strings.Contains(formatted, "Available entity persistence tools: get_entity, save_entity_field, create_entity, search_entities, query_metrics") {
		t.Fatalf("expected entity tools line, got %q", formatted)
	}
}

func TestFilterTools_RetainsUniversalEntityToolsWhenConstrained(t *testing.T) {
	allowed, constrained := extractAllowedToolSet(models.AgentConfig{
		Config: mustAgentConfigJSON(t, map[string]any{
			"allowed_tools": []string{"emit_example"},
		}),
	})
	if !constrained {
		t.Fatal("expected constrained tool set")
	}
	tools := []llm.ToolDefinition{
		{Name: "get_entity"},
		{Name: "search_entities"},
		{Name: "agent_message"},
		{Name: "non_universal"},
	}
	filtered := filterTools(tools, allowed, constrained)
	names := make([]string, 0, len(filtered))
	for _, tool := range filtered {
		names = append(names, tool.Name)
	}
	if !containsString(names, "get_entity") || !containsString(names, "search_entities") || !containsString(names, "agent_message") {
		t.Fatalf("expected universal tools preserved, got %v", names)
	}
	if containsString(names, "non_universal") {
		t.Fatalf("expected non-universal tool filtered out, got %v", names)
	}
	if !runtimetools.IsUniversal("get_entity") {
		t.Fatal("expected get_entity to be universal")
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == strings.TrimSpace(target) {
			return true
		}
	}
	return false
}

func mustAgentConfigJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return raw
}
