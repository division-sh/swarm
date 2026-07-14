package agents

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"time"
)

func testBoardDirective(text string) runtimeagentcontrol.BoardDirective {
	return runtimeagentcontrol.BoardDirective{
		Directive: text,
		Event: eventtest.RootIngress("00000000-0000-0000-0000-000000000101",
			events.EventType(runtimeagentcontrol.DirectiveEventType),
			"runtime", "", []byte(`{"directive_text":"`+text+`","mode":"directive","run_id":"00000000-0000-0000-0000-000000000201","run_id_resolution":"new_run_allocated","source":"test"}`), 0, "00000000-0000-0000-0000-000000000201", "", events.EventEnvelope{}, time.Time{}),

		RunIDResolution: runtimeagentcontrol.RunResolutionNewRunAllocated,
		Source:          "test",
	}
}

func TestFormatEventForAgent_UsesPostCompositionToolSurface(t *testing.T) {
	cfg := models.AgentConfig{
		ID:     "agent-1",
		Role:   "operator",
		FlowID: "task",
		Tools:  []string{"schedule", "get_entity", "emit_example"},
	}
	evt := eventtest.RootIngress(
		"evt-1",
		"item.created",
		"runtime",
		"task-1",
		[]byte(`{"item_id":"x"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"),
		time.Time{},
	)

	formatted := formatEventForAgent(cfg, evt, []llm.ToolDefinition{
		{Name: "get_entity"},
		{Name: "emit_example"},
		{Name: "read_file"},
		{
			Name: "save_entity_field",
			Schema: map[string]any{
				"properties": map[string]any{
					"field": map[string]any{
						"enum": []any{"metadata", "metadata.region", "status"},
					},
				},
			},
		},
	})
	if !strings.Contains(formatted, "Available non-emit tools in this turn: get_entity, read_file, save_entity_field") {
		t.Fatalf("expected post-composition non-emit summary, got %q", formatted)
	}
	if !strings.Contains(formatted, "Writable entity paths for save_entity_field in this turn: metadata, metadata.region, status") {
		t.Fatalf("expected writable path summary, got %q", formatted)
	}
	if strings.Contains(formatted, "schedule") {
		t.Fatalf("expected raw contract-only tool to stay out of event summary, got %q", formatted)
	}
	if !strings.Contains(formatted, "Available emit tools in this turn: emit_example") {
		t.Fatalf("expected emit tool summary, got %q", formatted)
	}
}

func TestFormatEventForAgent_UsesCanonicalNativeBuiltinNames(t *testing.T) {
	cfg := models.AgentConfig{
		ID:     "agent-1",
		Role:   "operator",
		FlowID: "task",
		NativeTools: models.NativeToolConfig{
			FileIO: true,
			Bash:   true,
		},
	}
	evt := eventtest.RootIngress(
		"evt-1",
		"item.created",
		"runtime",
		"task-1",
		[]byte(`{"item_id":"x"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"),
		time.Time{},
	)

	formatted := formatEventForAgent(cfg, evt, []llm.ToolDefinition{
		{Name: "query_entities"},
		{Name: "emit_example"},
	})
	if !strings.Contains(formatted, "Available native CLI tools in this turn: Bash, Edit, Read, Write") {
		t.Fatalf("expected canonical native builtin summary, got %q", formatted)
	}
	if strings.Contains(formatted, "file_io") {
		t.Fatalf("expected raw native contract flag to stay out of event summary, got %q", formatted)
	}
}

func TestFormatEventForAgent_DoesNotAdvertiseCLIOnlyControlTools(t *testing.T) {
	cfg := models.AgentConfig{
		ID:     "agent-1",
		Role:   "operator",
		FlowID: "task",
	}
	evt := eventtest.RootIngress(
		"evt-1",
		"item.created",
		"runtime",
		"task-1",
		[]byte(`{"item_id":"x"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"),
		time.Time{},
	)

	formatted := formatEventForAgent(cfg, evt, []llm.ToolDefinition{
		{Name: "query_entities"},
	})
	if strings.Contains(formatted, "ExitPlanMode") {
		t.Fatalf("expected non-CLI event formatting to omit CLI-only control tools, got %q", formatted)
	}
	if strings.Contains(formatted, "Available control tools in this turn") {
		t.Fatalf("expected non-CLI event formatting to omit control tool summary, got %q", formatted)
	}
}

func TestFilterTools_RemovesLegacyEntityToolsWhenConstrained(t *testing.T) {
	allowed, constrained := extractAllowedToolSet(models.AgentConfig{
		Tools: []string{"emit_example", "get_entity"},
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
	filtered := filterTools(tools, allowed, constrained, nil)
	names := make([]string, 0, len(filtered))
	for _, tool := range filtered {
		names = append(names, tool.Name)
	}
	if containsString(names, "get_entity") || containsString(names, "search_entities") {
		t.Fatalf("legacy entity tools should not be preserved by constrained filtering, got %v", names)
	}
	if !containsString(names, "agent_message") {
		t.Fatalf("expected non-entity universal tool preserved, got %v", names)
	}
	if containsString(names, "non_universal") {
		t.Fatalf("expected non-universal tool filtered out, got %v", names)
	}
	if runtimetools.IsUniversal("get_entity") {
		t.Fatal("get_entity must not remain universal")
	}
}

func TestFilterTools_DefaultDeniesLegacyEntityToolsWhenNoToolList(t *testing.T) {
	allowed, constrained := extractAllowedToolSet(models.AgentConfig{})
	if constrained {
		t.Fatal("expected unconstrained tool set when no tools are configured")
	}
	tools := []llm.ToolDefinition{
		{Name: "get_entity"},
		{Name: "agent_message"},
		{Name: "schedule"},
	}
	filtered := filterTools(tools, allowed, constrained, nil)
	names := make([]string, 0, len(filtered))
	for _, tool := range filtered {
		names = append(names, tool.Name)
	}
	if containsString(names, "get_entity") {
		t.Fatalf("legacy entity tool should not be preserved by default filtering, got %v", names)
	}
	if !containsString(names, "agent_message") {
		t.Fatalf("expected non-entity universal tool preserved, got %v", names)
	}
	if containsString(names, "schedule") {
		t.Fatalf("expected non-universal tool filtered out, got %v", names)
	}
}

func TestFilterTools_RetainsRoleScopedEntityToolsOnNonPrecomposedPath(t *testing.T) {
	allowed, constrained := extractAllowedToolSet(models.AgentConfig{})
	if constrained {
		t.Fatal("expected unconstrained tool set when no tools are configured")
	}
	tools := []llm.ToolDefinition{
		{Name: "read_validation_case"},
		{Name: "save_validation_case_business_brief"},
		{Name: "update_validation_case_business_brief_summary"},
		{Name: "read_unrelated_prefix_tool"},
		{Name: "schedule"},
	}
	filtered := filterTools(tools, allowed, constrained, map[string]struct{}{
		"read_validation_case":                          {},
		"save_validation_case_business_brief":           {},
		"update_validation_case_business_brief_summary": {},
	})
	names := make([]string, 0, len(filtered))
	for _, tool := range filtered {
		names = append(names, tool.Name)
	}
	for _, want := range []string{"read_validation_case", "save_validation_case_business_brief", "update_validation_case_business_brief_summary"} {
		if !containsString(names, want) {
			t.Fatalf("expected role-scoped entity tool %s preserved, got %v", want, names)
		}
	}
	if containsString(names, "schedule") {
		t.Fatalf("expected unrelated non-universal tool filtered out, got %v", names)
	}
	if containsString(names, "read_unrelated_prefix_tool") {
		t.Fatalf("expected unproven read_* tool filtered out, got %v", names)
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
	agent := &LLMAgent{
		cfg: models.AgentConfig{
			ID:   "cos-entity-1",
			Role: "ops_lead",
			Config: mustAgentConfigJSON(t, map[string]any{
				"team_name": "Acme Ops",
			}),
		},
		conversation:   llm.NewConversation("cos-entity-1", "", "", nil, agentmemory.Authored(true), 10, nil),
		promptCache:    map[string]string{},
		promptResolver: runtimecontracts.NewBundlePromptResolver(bundle),
	}

	got := agent.resolvePromptForMode("")
	if !strings.Contains(got, "Acme Ops") {
		t.Fatalf("expected resolved prompt to include config-expanded team name, got %q", got)
	}
	if strings.Contains(got, "{{team_name}}") {
		t.Fatalf("expected resolved prompt to expand team_name token, got %q", got)
	}
	if !strings.Contains(got, "Workspace: /workspace (read-write logical path)") {
		t.Fatalf("expected prompt postamble in resolved prompt, got %q", got)
	}
	if !strings.Contains(got, "Reference data: /data (read-only logical path)") {
		t.Fatalf("expected prompt postamble in resolved prompt, got %q", got)
	}
	if !strings.Contains(got, "Contracts: /opt/swarm/contracts (read-only logical path)") {
		t.Fatalf("expected prompt postamble in resolved prompt, got %q", got)
	}
	if strings.Contains(got, "Trusted host bash starts in the workspace backing directory") {
		t.Fatalf("expected legacy prompt postamble guard to be absent from resolved prompt, got %q", got)
	}
	if !strings.Contains(got, "Trusted host bash is full host-user shell execution from the workspace backing directory") {
		t.Fatalf("expected host bash full-power postamble in resolved prompt, got %q", got)
	}
	if !strings.Contains(got, "absolute path availability follows the host deployment namespace and OS permissions") {
		t.Fatalf("expected host path namespace caveat in resolved prompt, got %q", got)
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

func mustNewLLMAgent(t *testing.T, cfg models.AgentConfig, modelRuntime llm.Runtime, toolExecutor actorScopedToolExecutor, tools []llm.ToolDefinition) *LLMAgent {
	t.Helper()
	agent, err := NewLLMAgent(cfg, modelRuntime, toolExecutor, tools)
	if err != nil {
		t.Fatalf("NewLLMAgent: %v", err)
	}
	return agent
}

func TestNewLLMAgent_UsesConfiguredEmitEventsAndAllowedTools(t *testing.T) {
	emitRegistry := runtimetools.NewEmitRegistry(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
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
	}), nil)
	agent, err := NewLLMAgentWithOptions(
		models.AgentConfig{
			ID:         "coordinator-1",
			Role:       "coordinator",
			Tools:      []string{"schedule"},
			EmitEvents: []string{"coord.done"},
		},
		nil,
		nil,
		[]llm.ToolDefinition{
			{Name: "schedule"},
			{Name: "check_status"},
			{Name: "agent_message"},
		},
		LLMAgentOptions{EmitRegistry: emitRegistry},
	)
	if err != nil {
		t.Fatalf("NewLLMAgentWithOptions: %v", err)
	}
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

type boardTestRuntime struct {
	steps         []*llm.Response
	errs          []error
	call          int
	startTools    []string
	continueTools []string
	inputs        []string
}

func (r *boardTestRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	r.startTools = toolNamesForAgentTest(tools)
	return &llm.Session{
		ID:                "sess-1",
		AgentID:           agentID,
		SystemPrompt:      systemPrompt,
		Tools:             tools,
		Messages:          nil,
		ProviderSessionID: "",
	}, nil
}

func (r *boardTestRuntime) ContinueSession(_ context.Context, s *llm.Session, message llm.Message) (*llm.Response, error) {
	if s != nil {
		r.continueTools = toolNamesForAgentTest(s.Tools)
	}
	r.inputs = append(r.inputs, strings.TrimSpace(message.Content))
	if r.call < len(r.errs) && r.errs[r.call] != nil {
		err := r.errs[r.call]
		r.call++
		return nil, err
	}
	if r.call >= len(r.steps) {
		return nil, errors.New("unexpected runtime call")
	}
	resp := r.steps[r.call]
	r.call++
	return resp, nil
}

func toolNamesForAgentTest(tools []llm.ToolDefinition) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, strings.TrimSpace(tool.Name))
	}
	return names
}

func TestLLMAgent_OnEvent_UsesSinglePostStepExecutionPath(t *testing.T) {
	rt := &boardTestRuntime{
		steps: []*llm.Response{
			{Message: llm.Message{Role: "assistant", Content: "Handled."}},
		},
	}
	agent := mustNewLLMAgent(t,
		models.AgentConfig{ID: "analysis-1", Role: "analysis"},
		rt,
		nil,
		nil,
	)

	evt := eventtest.RootIngress(
		"evt-1",
		"analysis/requested",
		"runtime",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	if _, err := agent.OnEvent(context.Background(), evt); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}
	if rt.call != 1 {
		t.Fatalf("runtime call count = %d, want 1", rt.call)
	}
}

type boardEmitExecutor struct{}

func (boardEmitExecutor) Execute(ctx context.Context, name string, input any) (any, error) {
	if rec, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && rec != nil {
		rec.Append(eventtest.RootIngress("", events.EventType(strings.TrimPrefix(name, "emit_")), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	}
	return map[string]any{"ok": true, "name": name, "input": input}, nil
}

func (boardEmitExecutor) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return []llm.ToolDefinition{{Name: "emit_scan_requested"}}
}

func (boardEmitExecutor) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		kind := toolcapabilities.KindStandard
		if strings.HasPrefix(strings.TrimSpace(name), "emit_") {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{Name: name, Kind: kind, Visible: true, Callable: true})
	}
	return toolcapabilities.NewSet(caps)
}

type actorScopedFactoryToolExec struct{}

func (actorScopedFactoryToolExec) Execute(context.Context, string, any) (any, error) {
	return map[string]any{"ok": true}, nil
}

func (actorScopedFactoryToolExec) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		kind := toolcapabilities.KindStandard
		if strings.HasPrefix(strings.TrimSpace(name), "emit_") {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{Name: name, Kind: kind, Visible: true, Callable: true})
	}
	return toolcapabilities.NewSet(caps)
}

func (actorScopedFactoryToolExec) ToolDefinitionsForActor(cfg models.AgentConfig) []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{Name: "query_entities"},
		{Name: "emit_scan_requested"},
		{Name: "scoped_" + strings.TrimSpace(cfg.ID)},
	}
}

type contextAwareFactoryToolExec struct{}

func (contextAwareFactoryToolExec) Execute(context.Context, string, any) (any, error) {
	return map[string]any{"ok": true}, nil
}

func (contextAwareFactoryToolExec) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{Name: "read_scan_campaign"},
		{Name: "save_scan_campaign_mode"},
		{Name: "emit_market_research_scan_complete"},
	}
}

func (contextAwareFactoryToolExec) ToolDefinitionsForActorInContext(ctx context.Context, cfg models.AgentConfig) []llm.ToolDefinition {
	inbound, ok := runtimebus.InboundEventFromContext(ctx)
	if ok && strings.HasPrefix(inbound.EntityID(), "valid-") {
		return contextAwareFactoryToolExec{}.ToolDefinitionsForActor(cfg)
	}
	return []llm.ToolDefinition{{Name: "emit_market_research_scan_complete"}}
}

func (contextAwareFactoryToolExec) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	return roleScopedCapabilitiesForAgentTest(names, true)
}

func (contextAwareFactoryToolExec) ToolCapabilitiesForActorInContext(ctx context.Context, _ models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	inbound, ok := runtimebus.InboundEventFromContext(ctx)
	return roleScopedCapabilitiesForAgentTest(names, ok && strings.HasPrefix(inbound.EntityID(), "valid-"))
}

func roleScopedCapabilitiesForAgentTest(names []string, currentEntityEligible bool) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		visible := true
		callable := true
		if strings.HasPrefix(strings.TrimSpace(name), "read_scan_campaign") || strings.HasPrefix(strings.TrimSpace(name), "save_scan_campaign") {
			if !currentEntityEligible {
				visible = false
				callable = false
			}
		}
		caps = append(caps, toolcapabilities.Capability{Name: name, Visible: visible, Callable: callable})
	}
	return toolcapabilities.NewSet(caps)
}

func TestBoardStep_ReturnsErrorWhenDirectiveDoesNotAct(t *testing.T) {
	agent := mustNewLLMAgent(t,
		models.AgentConfig{ID: "coordinator-1", Role: "coordinator"},
		&boardTestRuntime{
			steps: []*llm.Response{
				{Message: llm.Message{Role: "assistant", Content: "I will emit scan_requested now."}},
				{Message: llm.Message{Role: "assistant", Content: "Still only explaining."}},
			},
		},
		boardEmitExecutor{},
		nil,
	)

	_, err := agent.BoardStep(context.Background(), testBoardDirective("start a corpus run"))
	if err == nil {
		t.Fatal("expected directive without action to fail")
	}
	if !strings.Contains(err.Error(), "without taking action") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBoardStep_RemediatesAndSucceedsWhenDirectiveEmits(t *testing.T) {
	agent := mustNewLLMAgent(t,
		models.AgentConfig{ID: "coordinator-1", Role: "coordinator"},
		&boardTestRuntime{
			steps: []*llm.Response{
				{Message: llm.Message{Role: "assistant", Content: "I will emit scan_requested now."}},
				{
					Message: llm.Message{Role: "assistant", Content: "Dispatching workflow now."},
					ToolCalls: []llm.ToolCall{
						{Name: "emit_scan_requested", Arguments: map[string]any{"entity_id": "corpus-1"}},
					},
				},
				{Message: llm.Message{Role: "assistant", Content: "scan_requested emitted"}},
			},
		},
		boardEmitExecutor{},
		[]llm.ToolDefinition{{Name: "emit_scan_requested"}},
	)

	got, err := agent.BoardStep(context.Background(), testBoardDirective("start a corpus run"))
	if err != nil {
		t.Fatalf("BoardStep: %v", err)
	}
	if got != "scan_requested emitted" && got != "Dispatching workflow now." {
		t.Fatalf("unexpected response: %q", got)
	}
}

func TestNewLLMAgentDefaultsToMemoryDisabled(t *testing.T) {
	agent := mustNewLLMAgent(t,
		models.AgentConfig{
			ID:       "entity-agent-1",
			Role:     "operator",
			EntityID: "ent-1",
		},
		nil,
		nil,
		nil,
	)
	if agent.conversation.Memory.Enabled {
		t.Fatal("conversation memory enabled, want disabled")
	}
}

func TestNewLLMAgentFactory_PrefersActorScopedToolDefinitions(t *testing.T) {
	factory := NewLLMAgentFactory(nil, actorScopedFactoryToolExec{}, []llm.ToolDefinition{
		{Name: "global_only"},
	}, LLMAgentOptions{})
	agent, err := factory(models.AgentConfig{
		ID:    "analysis-agent",
		Tools: []string{"query_entities"},
		Config: mustAgentConfigJSON(t, map[string]any{
			"system_prompt": "You are here.",
		}),
	})
	if err != nil {
		t.Fatalf("factory error: %v", err)
	}
	llmAgent, ok := agent.(*LLMAgent)
	if !ok {
		t.Fatalf("agent type = %T, want *LLMAgent", agent)
	}
	names := make([]string, 0, len(llmAgent.conversation.Tools))
	for _, tool := range llmAgent.conversation.Tools {
		names = append(names, tool.Name)
	}
	if !containsString(names, "query_entities") {
		t.Fatalf("expected actor-scoped tool in conversation, got %v", names)
	}
	if containsString(names, "global_only") {
		t.Fatalf("expected global fallback tool to be absent when actor-scoped definitions exist, got %v", names)
	}
	if !containsString(names, "scoped_analysis-agent") {
		t.Fatalf("expected precomposed actor-scoped tool to survive local filtering, got %v", names)
	}
}

func TestLLMAgentOnEvent_FiltersRoleScopedToolsByTurnEntityEligibility(t *testing.T) {
	rt := &boardTestRuntime{
		steps: []*llm.Response{
			{Message: llm.Message{Role: "assistant", Content: "handled"}},
		},
	}
	factory := NewLLMAgentFactory(rt, contextAwareFactoryToolExec{}, nil, LLMAgentOptions{})
	agent, err := factory(models.AgentConfig{
		ID:     "market-research-agent",
		Role:   "market_research",
		Memory: agentmemory.Authored(false),
		Config: mustAgentConfigJSON(t, map[string]any{
			"system_prompt": "You are here.",
		}),
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	llmAgent := agent.(*LLMAgent)
	evt := eventtest.RootIngress(
		"evt-root",
		events.EventType("discovery/market_research.corpus_file_assigned"),
		"",
		"",
		[]byte(`{"assignment":{"scan_id":"root-run-id","geography":"US"}}`),
		0,
		"run-1",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "root-run-id"),
		time.Time{},
	)

	if _, err := llmAgent.OnEvent(context.Background(), evt); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}
	if containsString(rt.continueTools, "read_scan_campaign") || containsString(rt.continueTools, "save_scan_campaign_mode") {
		t.Fatalf("invalid current entity left role-scoped tools in provider turn: %#v", rt.continueTools)
	}
	if !containsString(rt.continueTools, "emit_market_research_scan_complete") {
		t.Fatalf("invalid current entity removed non-entity emit tool: %#v", rt.continueTools)
	}
	if len(rt.inputs) == 0 || strings.Contains(rt.inputs[0], "read_scan_campaign") || strings.Contains(rt.inputs[0], "save_scan_campaign_mode") {
		t.Fatalf("event prompt advertised ineligible role-scoped tools: %#v", rt.inputs)
	}
}

type directiveFactoryRuntime struct {
	steps      []*llm.Response
	call       int
	startTools []string
	inputs     []string
}

func (r *directiveFactoryRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	r.startTools = r.startTools[:0]
	for _, tool := range tools {
		r.startTools = append(r.startTools, strings.TrimSpace(tool.Name))
	}
	return &llm.Session{
		ID:           "sess-" + strings.TrimSpace(agentID),
		AgentID:      agentID,
		SystemPrompt: systemPrompt,
		Tools:        tools,
	}, nil
}

func (r *directiveFactoryRuntime) ContinueSession(_ context.Context, _ *llm.Session, message llm.Message) (*llm.Response, error) {
	r.inputs = append(r.inputs, strings.TrimSpace(message.Content))
	if r.call >= len(r.steps) {
		return nil, errors.New("unexpected runtime call")
	}
	resp := r.steps[r.call]
	r.call++
	return resp, nil
}

type directiveFactoryPublishBus struct {
	events []events.Event
}

func (b *directiveFactoryPublishBus) Publish(ctx context.Context, evt events.Event) error {
	_, evt = runtimecorrelation.CorrelateEvent(ctx, evt)
	b.events = append(b.events, evt)
	return nil
}

func (b *directiveFactoryPublishBus) PublishDirect(_ context.Context, evt events.Event, _ []string) error {
	b.events = append(b.events, evt)
	return nil
}

func newFactoryDirectiveAgent(t *testing.T, cfg models.AgentConfig, modelRuntime llm.Runtime, bundle *runtimecontracts.WorkflowContractBundle) (*LLMAgent, *directiveFactoryPublishBus) {
	t.Helper()

	source := semanticview.Wrap(bundle)
	authority := runtimeauthority.NewSourceProvider(source)
	emitRegistry := runtimetools.NewEmitRegistry(source, authority)
	bus := &directiveFactoryPublishBus{}
	exec := runtimetools.NewExecutorWithOptions(bus, nil, runtimetools.ExecutorOptions{
		WorkflowSource:    source,
		AuthorityProvider: authority,
		EmitRegistry:      emitRegistry,
	})

	factory := NewLLMAgentFactory(modelRuntime, exec, exec.ToolDefinitions(), LLMAgentOptions{
		AuthorityProvider: authority,
		EmitRegistry:      emitRegistry,
	})
	agent, err := factory(cfg)
	if err != nil {
		t.Fatalf("factory error: %v", err)
	}
	llmAgent, ok := agent.(*LLMAgent)
	if !ok {
		t.Fatalf("agent type = %T, want *LLMAgent", agent)
	}
	return llmAgent, bus
}

func TestBoardStep_FactoryCreatedDirectiveTurnPreservesRoleScopedEmitToolSurface(t *testing.T) {
	rt := &directiveFactoryRuntime{
		steps: []*llm.Response{
			{
				Message: llm.Message{Role: "assistant", Content: "Dispatching workflow now."},
				ToolCalls: []llm.ToolCall{
					{Name: "emit_scan_requested", Arguments: map[string]any{}},
				},
			},
			{Message: llm.Message{Role: "assistant", Content: "scan_requested emitted"}},
		},
	}
	agent, bus := newFactoryDirectiveAgent(t, models.AgentConfig{
		ID:   "campaign-coordinator",
		Role: "campaign_coordinator",
		Config: mustAgentConfigJSON(t, map[string]any{
			"system_prompt": "You coordinate workflow launch.",
		}),
	}, rt, &runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"campaign-coordinator": {
				ID:         "campaign-coordinator",
				Role:       "campaign_coordinator",
				EmitEvents: []string{"scan.requested"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	})

	got, err := agent.BoardStep(context.Background(), testBoardDirective("start a corpus run"))
	if err != nil {
		t.Fatalf("BoardStep: %v", err)
	}
	if got != "Dispatching workflow now." {
		t.Fatalf("directive response = %q, want terminal emit turn text", got)
	}
	if !containsString(rt.startTools, "emit_scan_requested") {
		t.Fatalf("session tools = %v, want emit_scan_requested", rt.startTools)
	}
	if len(rt.inputs) == 0 || !strings.Contains(rt.inputs[0], "Available emit tools in this turn: emit_scan_requested") {
		t.Fatalf("directive input = %q, want emit tool summary", firstOrEmpty(rt.inputs))
	}
	if len(bus.events) != 1 || string(bus.events[0].Type()) != "scan.requested" {
		t.Fatalf("published events = %#v, want one scan.requested event", bus.events)
	}
	if bus.events[0].RunID() != "00000000-0000-0000-0000-000000000201" || bus.events[0].ParentEventID() != "00000000-0000-0000-0000-000000000101" {
		t.Fatalf("published event lineage = run:%q parent:%q", bus.events[0].RunID(), bus.events[0].ParentEventID())
	}
}

func TestBoardStep_FactoryCreatedDirectiveRemediationPreservesFlowScopedEmitToolSurface(t *testing.T) {
	rt := &directiveFactoryRuntime{
		steps: []*llm.Response{
			{Message: llm.Message{Role: "assistant", Content: "I will trigger the workflow now."}},
			{
				Message: llm.Message{Role: "assistant", Content: "Dispatching workflow now."},
				ToolCalls: []llm.ToolCall{
					{Name: "emit_scan_requested", Arguments: map[string]any{}},
				},
			},
			{Message: llm.Message{Role: "assistant", Content: "scan_requested emitted"}},
		},
	}
	agent, bus := newFactoryDirectiveAgent(t, models.AgentConfig{
		ID:         "campaign-coordinator",
		Role:       "campaign_coordinator",
		FlowID:     "campaign-flow",
		FlowPath:   "campaign-flow/inst-1",
		EmitEvents: []string{"campaign-flow/inst-1/scan.requested"},
		Config: mustAgentConfigJSON(t, map[string]any{
			"system_prompt": "You coordinate workflow launch.",
		}),
	}, rt, &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			ByID: map[string]*runtimecontracts.FlowContractView{
				"campaign-flow": {
					Paths: runtimecontracts.FlowContractPaths{
						ID:   "campaign-flow",
						Flow: "campaign-flow",
					},
					Events: map[string]runtimecontracts.EventCatalogEntry{
						"scan.requested": {},
					},
					Path: "campaign-flow",
				},
			},
		},
	})

	got, err := agent.BoardStep(context.Background(), testBoardDirective("start a corpus run"))
	if err != nil {
		t.Fatalf("BoardStep: %v", err)
	}
	if got != "Dispatching workflow now." {
		t.Fatalf("directive response = %q, want terminal emit turn text", got)
	}
	if !containsString(rt.startTools, "emit_scan_requested") {
		t.Fatalf("session tools = %v, want emit_scan_requested", rt.startTools)
	}
	if len(rt.inputs) == 0 || !strings.Contains(rt.inputs[0], "Available emit tools in this turn: emit_scan_requested") {
		t.Fatalf("directive input = %q, want emit tool summary", firstOrEmpty(rt.inputs))
	}
	if len(rt.inputs) < 2 || !strings.Contains(rt.inputs[1], "call the appropriate emit_* tool in this turn") {
		t.Fatalf("remediation input = %q, want remediation prompt", firstOrEmpty(rt.inputs[1:]))
	}
	if len(bus.events) != 1 || string(bus.events[0].Type()) != "campaign-flow/inst-1/scan.requested" {
		t.Fatalf("published events = %#v, want one externalized scan.requested event", bus.events)
	}
}

type taskRetryRuntime struct {
	startCalls    int
	continueCalls int
}

func (r *taskRetryRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	r.startCalls++
	return &llm.Session{
		ID:           "sess-" + strings.TrimSpace(agentID) + "-" + string(rune('0'+r.startCalls)),
		AgentID:      agentID,
		SystemPrompt: systemPrompt,
		Tools:        tools,
	}, nil
}

func (r *taskRetryRuntime) ContinueSession(_ context.Context, _ *llm.Session, _ llm.Message) (*llm.Response, error) {
	r.continueCalls++
	if r.continueCalls == 1 {
		return nil, runtimefailures.New(runtimefailures.ClassBudgetExhausted, "agent_turn_budget_exhausted", "llm-conversation", "continue", map[string]any{
			"budget_kind": "agent_turns",
			"actual":      1,
			"limit":       1,
		})
	}
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "ok"}}, nil
}

func TestLLMAgent_StatelessTurnBudgetFailureResetsConversationAndRetries(t *testing.T) {
	rt := &taskRetryRuntime{}
	agent := mustNewLLMAgent(t,
		models.AgentConfig{
			ID:       "spec-reviewer",
			Role:     "spec_reviewer",
			EntityID: "ent-1",
			Memory:   agentmemory.Authored(false),
		},
		rt,
		nil,
		nil,
	)

	evt := eventtest.RootIngress(
		"evt-1",
		"validation/spec_review.requested",
		"runtime",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	if _, err := agent.OnEvent(context.Background(), evt); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}
	if rt.continueCalls != 2 {
		t.Fatalf("continue calls = %d, want 2", rt.continueCalls)
	}
	if rt.startCalls != 2 {
		t.Fatalf("start calls = %d, want 2 after reset", rt.startCalls)
	}
}

type runIDCaptureRuntime struct {
	startRunIDs    []string
	continueRunIDs []string
}

func (r *runIDCaptureRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	r.startRunIDs = append(r.startRunIDs, runtimecorrelation.RunIDFromContext(ctx))
	return &llm.Session{ID: "sess-" + agentID, AgentID: agentID}, nil
}

func (r *runIDCaptureRuntime) ContinueSession(ctx context.Context, _ *llm.Session, _ llm.Message) (*llm.Response, error) {
	r.continueRunIDs = append(r.continueRunIDs, runtimecorrelation.RunIDFromContext(ctx))
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "ok"}}, nil
}

func TestLLMAgent_OnEvent_SeedsRunIDIntoConversationContext(t *testing.T) {
	rt := &runIDCaptureRuntime{}
	agent := mustNewLLMAgent(t,
		models.AgentConfig{
			ID:       "analysis-agent",
			Role:     "analysis_agent",
			EntityID: "ent-1",
		},
		rt,
		nil,
		nil,
	)

	evt := eventtest.RootIngress(
		"evt-1",
		"scoring/scoring.requested",
		"runtime",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		"run-123",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	if _, err := agent.OnEvent(context.Background(), evt); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}
	if len(rt.startRunIDs) != 1 || rt.startRunIDs[0] != "run-123" {
		t.Fatalf("start session run_ids = %v, want [run-123]", rt.startRunIDs)
	}
	if len(rt.continueRunIDs) != 1 || rt.continueRunIDs[0] != "run-123" {
		t.Fatalf("continue session run_ids = %v, want [run-123]", rt.continueRunIDs)
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

func TestAppendPromptPostamble_AppendsWhenPartialPostambleMissingRequiredMounts(t *testing.T) {
	partial := strings.Join([]string{
		"You are helpful.",
		"Workspace: /workspace (read-write logical path)",
		"Trusted host bash is full host-user shell execution from the workspace backing directory; use relative paths for workspace files, and absolute path availability follows the host deployment namespace and OS permissions.",
	}, "\n")

	got := appendPromptPostamble(partial)
	if got == partial {
		t.Fatalf("expected canonical postamble appended to partial environment prompt, got unchanged %q", got)
	}
	for _, want := range []string{
		"Reference data: /data (read-only logical path)",
		"Contracts: /opt/swarm/contracts (read-only logical path)",
		"Docker-backed command execution exposes these as OS paths",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected appended canonical postamble to include %q, got %q", want, got)
		}
	}
}

type nativeCapabilityRuntimeStub struct {
	llm.NoopRuntime
	caps llm.NativeToolCapabilities
}

func (s nativeCapabilityRuntimeStub) ProviderContract() llm.ProviderContract {
	contract := llm.AnthropicAPIProviderContract()
	contract.RuntimeMode = "stub"
	contract.Provider = "stub"
	contract.NativeTools.Capabilities = s.caps
	return contract
}

func TestNewLLMAgent_DoesNotInjectNativeFallbackToolsWithoutExecutorAdmission(t *testing.T) {
	agent := mustNewLLMAgent(t,
		models.AgentConfig{
			ID:   "researcher-1",
			Role: "researcher",
			NativeTools: models.NativeToolConfig{
				Bash:      true,
				WebSearch: true,
				FileIO:    true,
			},
		},
		nativeCapabilityRuntimeStub{},
		nil,
		nil,
	)
	names := make([]string, 0, len(agent.conversation.Tools))
	for _, tool := range agent.conversation.Tools {
		names = append(names, tool.Name)
	}
	for _, forbidden := range []string{"bash", "web_search", "read_file", "write_file"} {
		if containsString(names, forbidden) {
			t.Fatalf("did not expect unproven native fallback tool %s in %v", forbidden, names)
		}
	}
}

func TestNewLLMAgent_DoesNotInjectNativeFallbackToolsWhenProviderSupportsCapability(t *testing.T) {
	agent := mustNewLLMAgent(t,
		models.AgentConfig{
			ID:   "ops-1",
			Role: "ops",
			NativeTools: models.NativeToolConfig{
				Bash:      true,
				WebSearch: true,
				FileIO:    true,
			},
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

func firstOrEmpty(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func mustAgentConfigJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return raw
}
