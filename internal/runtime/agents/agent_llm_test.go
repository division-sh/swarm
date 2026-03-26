package agents

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	models "empireai/internal/runtime/core/actors"
	llm "empireai/internal/runtime/llm"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
	runtimetools "empireai/internal/runtime/tools"
)

func TestFormatEventForAgent_UsesConfiguredToolSummary(t *testing.T) {
	cfg := models.AgentConfig{
		ID:   "agent-1",
		Role: "operator",
		Mode: "task",
		Config: mustAgentConfigJSON(t, map[string]any{
			"allowed_tools": []string{"schedule", "get_entity", "emit_example"},
		}),
	}
	evt := (events.Event{
		ID:          "evt-1",
		Type:        "item.created",
		SourceAgent: "runtime",
		TaskID:      "task-1",
		Payload:     []byte(`{"item_id":"x"}`),
	}).WithEntityID("entity-1")

	formatted := formatEventForAgent(cfg, evt)
	if !strings.Contains(formatted, "Available non-emit tools from your contract: get_entity, schedule") {
		t.Fatalf("expected configured tool summary, got %q", formatted)
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

func TestResolvePromptForMode_ExpandsConfigVariables(t *testing.T) {
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts"),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	runtimecontracts.SetActivePromptBundle(bundle)

	agent := &LLMAgent{
		cfg: models.AgentConfig{
			ID:   "cos-entity-1",
			Role: "chief_of_staff",
			Config: mustAgentConfigJSON(t, map[string]any{
				"vertical_name": "Acme Ops",
			}),
		},
		conversation: llm.NewConversation("cos-entity-1", "", "", nil, llm.SessionScoped, 10, nil),
		promptCache:  map[string]string{},
	}

	got := agent.resolvePromptForMode("")
	if !strings.Contains(got, "Acme Ops") {
		t.Fatalf("expected resolved prompt to include config-expanded vertical name, got %q", got)
	}
	if strings.Contains(got, "{{vertical_name}}") {
		t.Fatalf("expected resolved prompt to expand vertical_name token, got %q", got)
	}
}

func TestParseConversationMode_AcceptsStatelessAlias(t *testing.T) {
	mode, ok := parseConversationMode("stateless")
	if !ok {
		t.Fatal("expected stateless alias to be accepted")
	}
	if mode != llm.TaskScoped {
		t.Fatalf("parseConversationMode(stateless) = %v, want %v", mode, llm.TaskScoped)
	}
}

func TestNewLLMAgent_UsesConfiguredEmitEventsAndAllowedTools(t *testing.T) {
	runtimetools.InitEventSchemaRegistry(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"coord.done": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
						"task_id":   {Type: "string"},
					},
					Required: []string{"entity_id"},
				},
			},
		},
	}))
	agent := NewLLMAgent(
		models.AgentConfig{
			ID:   "coordinator-1",
			Role: "coordinator",
			Config: mustAgentConfigJSON(t, map[string]any{
				"allowed_tools": []string{"schedule"},
				"emit_events":   []string{"coord.done"},
			}),
		},
		nil,
		nil,
		[]llm.ToolDefinition{
			{Name: "schedule"},
			{Name: "systemd_control"},
			{Name: "agent_message"},
		},
	)
	names := make([]string, 0, len(agent.conversation.Tools))
	for _, tool := range agent.conversation.Tools {
		names = append(names, tool.Name)
	}
	if !containsString(names, "schedule") {
		t.Fatalf("expected configured tier2 tool in session, got %v", names)
	}
	if !containsString(names, "agent_message") {
		t.Fatalf("expected universal tool in session, got %v", names)
	}
	if !containsString(names, "emit_coord_done") {
		t.Fatalf("expected explicit emit tool in session, got %v", names)
	}
	if containsString(names, "systemd_control") {
		t.Fatalf("expected unconstrained non-universal tool to be filtered out, got %v", names)
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
