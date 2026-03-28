package agents

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
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
	bundleRoot := writeAgentPromptTestBundle(t, repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		bundleRoot,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	runtimecontracts.SetActivePromptBundle(bundle)

	agent := &LLMAgent{
		cfg: models.AgentConfig{
			ID:   "cos-entity-1",
			Role: "ops_lead",
			Config: mustAgentConfigJSON(t, map[string]any{
				"team_name": "Acme Ops",
			}),
		},
		conversation: llm.NewConversation("cos-entity-1", "", "", nil, llm.SessionScoped, 10, nil),
		promptCache:  map[string]string{},
	}

	got := agent.resolvePromptForMode("")
	if !strings.Contains(got, "Acme Ops") {
		t.Fatalf("expected resolved prompt to include config-expanded team name, got %q", got)
	}
	if strings.Contains(got, "{{team_name}}") {
		t.Fatalf("expected resolved prompt to expand team_name token, got %q", got)
	}
	if !strings.Contains(got, "Working directory: /workspace (read-write)") {
		t.Fatalf("expected prompt postamble in resolved prompt, got %q", got)
	}
	if !strings.Contains(got, "Reference data: /data (read-only)") {
		t.Fatalf("expected prompt postamble in resolved prompt, got %q", got)
	}
	if !strings.Contains(got, "Contracts: /opt/swarm/contracts (read-only)") {
		t.Fatalf("expected prompt postamble in resolved prompt, got %q", got)
	}
}

func writeAgentPromptTestBundle(t *testing.T, repoRoot string) string {
	t.Helper()
	srcRoot := filepath.Join(repoRoot, "internal", "runtime", "testdata", "generic-swarm-bundle")
	dstRoot := filepath.Join(t.TempDir(), "agent-prompt-test-bundle")
	copyBundleTree(t, srcRoot, dstRoot)

	agentsPath := filepath.Join(dstRoot, "agents.yaml")
	agentsRaw, err := os.ReadFile(agentsPath)
	if err != nil {
		t.Fatalf("read %s: %v", agentsPath, err)
	}
	agentsRaw = append(agentsRaw, []byte(strings.TrimLeft(`
ops-lead:
  id: ops-lead
  role: ops_lead
  manager_fallback: control-plane
  emit_events:
    - item.created
`, "\n"))...)
	if err := os.WriteFile(agentsPath, agentsRaw, 0o644); err != nil {
		t.Fatalf("write %s: %v", agentsPath, err)
	}

	promptsDir := filepath.Join(dstRoot, "prompts")
	if err := os.MkdirAll(promptsDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", promptsDir, err)
	}
	prompt := strings.TrimSpace(`
You are the operations lead for {{team_name}}.
`)
	if err := os.WriteFile(filepath.Join(promptsDir, "ops-lead.md"), []byte(prompt+"\n"), 0o644); err != nil {
		t.Fatalf("write prompt fixture: %v", err)
	}
	return dstRoot
}

func copyBundleTree(t *testing.T, srcRoot, dstRoot string) {
	t.Helper()
	if err := filepath.Walk(srcRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcRoot, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dstRoot, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	}); err != nil {
		t.Fatalf("copy %s -> %s: %v", srcRoot, dstRoot, err)
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
			{Name: "check_status"},
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
	if containsString(names, "check_status") {
		t.Fatalf("expected unconstrained non-universal tool to be filtered out, got %v", names)
	}
}

func TestAppendPromptPostamble_IsIdempotent(t *testing.T) {
	prompt := "You are helpful."
	once := appendPromptPostamble(prompt)
	twice := appendPromptPostamble(once)
	if once != twice {
		t.Fatalf("expected postamble append to be idempotent\nonce=%q\ntwice=%q", once, twice)
	}
}

type nativeCapabilityRuntimeStub struct {
	llm.NoopRuntime
	caps llm.NativeToolCapabilities
}

func (s nativeCapabilityRuntimeStub) NativeToolCapabilities() llm.NativeToolCapabilities {
	return s.caps
}

func TestNewLLMAgent_InjectsNativeFallbackToolsWhenProviderLacksSupport(t *testing.T) {
	agent := NewLLMAgent(
		models.AgentConfig{
			ID:   "researcher-1",
			Role: "researcher",
			Config: mustAgentConfigJSON(t, map[string]any{
				"native_tools": map[string]any{
					"bash":       true,
					"web_search": true,
					"file_io":    true,
				},
			}),
		},
		nativeCapabilityRuntimeStub{},
		nil,
		nil,
	)
	names := make([]string, 0, len(agent.conversation.Tools))
	for _, tool := range agent.conversation.Tools {
		names = append(names, tool.Name)
	}
	for _, want := range []string{"bash", "web_search", "read_file", "write_file"} {
		if !containsString(names, want) {
			t.Fatalf("expected native fallback tool %s in %v", want, names)
		}
	}
}

func TestNewLLMAgent_DoesNotInjectNativeFallbackToolsWhenProviderSupportsCapability(t *testing.T) {
	agent := NewLLMAgent(
		models.AgentConfig{
			ID:   "ops-1",
			Role: "ops",
			Config: mustAgentConfigJSON(t, map[string]any{
				"native_tools": map[string]any{
					"bash":       true,
					"web_search": true,
					"file_io":    true,
				},
			}),
		},
		nativeCapabilityRuntimeStub{caps: llm.NativeToolCapabilities{
			Bash:      true,
			WebSearch: true,
			FileIO:    true,
		}},
		nil,
		nil,
	)
	names := make([]string, 0, len(agent.conversation.Tools))
	for _, tool := range agent.conversation.Tools {
		names = append(names, tool.Name)
	}
	for _, forbidden := range []string{"bash", "web_search", "read_file", "write_file"} {
		if containsString(names, forbidden) {
			t.Fatalf("did not expect fallback tool %s in %v", forbidden, names)
		}
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
