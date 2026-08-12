package agents

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

type staticAgentRuntimeResolver struct {
	runtime llm.Runtime
}

func (r staticAgentRuntimeResolver) ResolveAgentRuntime(actor models.AgentConfig) (llm.AgentRuntimeResolution, error) {
	return llm.AgentRuntimeResolution{Actor: actor, Runtime: r.runtime}, nil
}

func testBoardDirective(text string) runtimeagentcontrol.BoardDirective {
	return runtimeagentcontrol.BoardDirective{
		Directive: text,
		Event: eventtest.DiagnosticDirect("00000000-0000-0000-0000-000000000101",
			events.EventType(runtimeagentcontrol.DirectiveEventType),
			"runtime", "", []byte(`{"directive_text":"`+text+`","mode":"directive","run_id":"00000000-0000-0000-0000-000000000201","run_id_resolution":"new_run_allocated","source":"test"}`), 0, "00000000-0000-0000-0000-000000000201", "", events.EventEnvelope{}, time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)),

		RunIDResolution: runtimeagentcontrol.RunResolutionNewRunAllocated,
		Source:          "test",
	}
}

func TestFormatEventForAgent_DoesNotNarrateIndependentToolSurface(t *testing.T) {
	cfg := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		Role:          "operator",
		FlowID:        "task",
		Tools:         []string{"schedule", "get_entity", "emit_example"},
	}
	evt := eventtest.RunCreatingRootIngress(
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
	for _, stale := range []string{"Available non-emit tools", "Writable entity paths", "Available emit tools", "schedule", "get_entity", "read_file", "save_entity_field", "emit_example"} {
		if strings.Contains(formatted, stale) {
			t.Fatalf("event prompt retained independent capability narrative %q: %q", stale, formatted)
		}
	}
}

func TestFormatEventForAgent_DoesNotNarrateNativeBuiltinSurface(t *testing.T) {
	cfg := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		Role:          "operator",
		FlowID:        "task",
		NativeTools: models.NativeToolConfig{
			FileIO: true,
			Bash:   true,
		},
	}
	evt := eventtest.RunCreatingRootIngress(
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
	for _, stale := range []string{"Available native CLI tools", "Bash", "Edit", "Read", "Write", "file_io"} {
		if strings.Contains(formatted, stale) {
			t.Fatalf("event prompt retained independent native capability narrative %q: %q", stale, formatted)
		}
	}
}

func TestFormatEventForAgent_DoesNotAdvertiseCLIOnlyControlTools(t *testing.T) {
	cfg := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		Role:          "operator",
		FlowID:        "task",
	}
	evt := eventtest.RunCreatingRootIngress(
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

func TestNewLLMAgent_ConsumesExactProviderPromptAssemblyWithoutConfigExpansion(t *testing.T) {
	content := "You are the operations lead for {{team_name}}.\n"
	intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "agents.yaml#agents.ops-lead.intent", content)
	if err != nil {
		t.Fatalf("resolve test intent: %v", err)
	}
	prompt, err := runtimeagentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatalf("derive test prompt: %v", err)
	}
	cfg := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "cos-entity-1",
		Role:          "ops_lead",
		Intent:        intent,
		Prompt:        prompt,
		Config: mustAgentConfigJSON(t, map[string]any{
			"team_name": "Acme Ops",
		}),
	}
	agent := mustBuildLLMAgent(t, cfg, nil, actorScopedFactoryToolExec{}, nil)

	got := agent.conversation.SystemPrompt
	expectedPrompt, err := cfg.ProviderPrompt(runtimeagentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatalf("assemble expected provider prompt: %v", err)
	}
	expected, err := expectedPrompt.Text()
	if err != nil {
		t.Fatalf("render expected provider prompt: %v", err)
	}
	if got != expected {
		t.Fatalf("normal agent provider prompt differs from canonical assembly\ngot:  %q\nwant: %q", got, expected)
	}
	if !strings.HasPrefix(got, content) {
		t.Fatalf("derived prompt does not preserve exact intent prefix: %q", got)
	}
	if !strings.Contains(got, "{{team_name}}") || strings.Contains(got, "Acme Ops") {
		t.Fatalf("resolved intent was interpreted through config variables: %q", got)
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

func mustBuildLLMAgent(t *testing.T, cfg models.AgentConfig, modelRuntime llm.Runtime, toolExecutor actorScopedToolExecutor, tools []llm.ToolDefinition) *LLMAgent {
	t.Helper()
	if !cfg.ExecutionMode.Valid() {
		cfg.ExecutionMode = runtimeeffects.ExecutionModeLive
	}
	if err := cfg.Intent.Validate(); err != nil {
		intent, resolveErr := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "agents.yaml#agents.test.intent", "Perform the test agent's assigned work.")
		if resolveErr != nil {
			t.Fatalf("resolve default test intent: %v", resolveErr)
		}
		cfg.Intent = intent
	}
	if cfg.Prompt.Empty() {
		prompt, promptErr := runtimeagentintent.IntentOnlyPrompt(cfg.Intent)
		if promptErr != nil {
			t.Fatalf("derive default test prompt: %v", promptErr)
		}
		cfg.Prompt = prompt
	}
	agent, err := newLLMAgent(cfg, modelRuntime, toolExecutor, tools, LLMAgentOptions{})
	if err != nil {
		t.Fatalf("newLLMAgent: %v", err)
	}
	return agent
}

type boardTestRuntime struct {
	steps         []*llm.Response
	errs          []error
	call          int
	modes         []runtimeeffects.ExecutionMode
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

func (r *boardTestRuntime) ContinueSession(ctx context.Context, s *llm.Session, message llm.Message) (*llm.Response, error) {
	if s != nil {
		r.continueTools = toolNamesForAgentTest(s.Tools)
	}
	mode, _ := runtimeeffects.ExecutionModeFromContext(ctx)
	r.modes = append(r.modes, mode)
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
	agent := mustBuildLLMAgent(t,
		models.AgentConfig{ExecutionMode: "live", ID: "analysis-1", Role: "analysis"},
		rt,
		nil,
		nil,
	)

	evt := eventtest.RunCreatingRootIngress(
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

func TestLLMAgentOnEventPreventsMockCausalityFromEscalatingToLive(t *testing.T) {
	for _, tc := range []struct {
		name        string
		agentMode   runtimeeffects.ExecutionMode
		inboundMode runtimeeffects.ExecutionMode
		wantMode    runtimeeffects.ExecutionMode
		wantReject  bool
	}{
		{name: "live input to live agent remains live", agentMode: runtimeeffects.ExecutionModeLive, inboundMode: runtimeeffects.ExecutionModeLive, wantMode: runtimeeffects.ExecutionModeLive},
		{name: "live input to mock agent narrows to mock", agentMode: runtimeeffects.ExecutionModeMock, inboundMode: runtimeeffects.ExecutionModeLive, wantMode: runtimeeffects.ExecutionModeMock},
		{name: "mock input to mock agent remains mock", agentMode: runtimeeffects.ExecutionModeMock, inboundMode: runtimeeffects.ExecutionModeMock, wantMode: runtimeeffects.ExecutionModeMock},
		{name: "mock input to live agent is rejected", agentMode: runtimeeffects.ExecutionModeLive, inboundMode: runtimeeffects.ExecutionModeMock, wantReject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime := &boardTestRuntime{steps: []*llm.Response{{Message: llm.Message{Role: "assistant", Content: "handled"}}}}
			agent := mustBuildLLMAgent(t, models.AgentConfig{ID: "mode-agent", Role: "worker", ExecutionMode: tc.agentMode}, runtime, nil, nil)
			evt := eventtest.RunCreatingRootIngressWithMode(
				"mode-event", "work.requested", "runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}, tc.inboundMode,
			)

			_, err := agent.OnEvent(context.Background(), evt)
			if tc.wantReject {
				if err == nil || !strings.Contains(err.Error(), "mock_causal_live_agent_forbidden") {
					t.Fatalf("OnEvent error = %v, want mock-causal live-agent rejection", err)
				}
				if runtime.call != 0 || len(runtime.modes) != 0 {
					t.Fatalf("rejected delivery reached runtime: calls=%d modes=%v", runtime.call, runtime.modes)
				}
				return
			}
			if err != nil {
				t.Fatalf("OnEvent: %v", err)
			}
			if runtime.call != 1 || len(runtime.modes) != 1 || runtime.modes[0] != tc.wantMode {
				t.Fatalf("runtime execution = calls:%d modes:%v, want one %q turn", runtime.call, runtime.modes, tc.wantMode)
			}
		})
	}
}

type boardEmitExecutor struct{}

func (boardEmitExecutor) Execute(ctx context.Context, name string, input any) (any, error) {
	if rec, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && rec != nil {
		rec.Append(eventtest.RunCreatingRootIngress("", events.EventType(strings.TrimPrefix(name, "emit_")), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
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
	agent := mustBuildLLMAgent(t,
		models.AgentConfig{ExecutionMode: "live", ID: "coordinator-1", Role: "coordinator"},
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
	agent := mustBuildLLMAgent(t,
		models.AgentConfig{ExecutionMode: "live", ID: "coordinator-1", Role: "coordinator"},
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
	agent := mustBuildLLMAgent(t,
		models.AgentConfig{
			ExecutionMode: "live",
			ID:            "entity-agent-1",
			Role:          "operator",
			EntityID:      "ent-1",
		},
		nil,
		nil,
		nil,
	)
	if agent.conversation.Memory.Enabled {
		t.Fatal("conversation memory enabled, want disabled")
	}
}

func TestNewLLMAgentFactory_UsesActorScopedToolDefinitions(t *testing.T) {
	factory := NewLLMAgentFactory(staticAgentRuntimeResolver{runtime: llm.NoopRuntime{}}, actorScopedFactoryToolExec{}, LLMAgentOptions{})
	agent, err := factory(withTestResolvedIntent(t, models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analysis-agent",
		Tools:         []string{"query_entities"},
	}, "You are here."))
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
	if !containsString(names, "scoped_analysis-agent") {
		t.Fatalf("expected actor-scoped tool to reach the conversation, got %v", names)
	}
}

func TestLLMAgentOnEvent_FiltersRoleScopedToolsByTurnEntityEligibility(t *testing.T) {
	rt := &boardTestRuntime{
		steps: []*llm.Response{
			{Message: llm.Message{Role: "assistant", Content: "handled"}},
		},
	}
	factory := NewLLMAgentFactory(staticAgentRuntimeResolver{runtime: rt}, contextAwareFactoryToolExec{}, LLMAgentOptions{})
	agent, err := factory(withTestResolvedIntent(t, models.AgentConfig{
		ID:            "market-research-agent",
		Role:          "market_research",
		Memory:        agentmemory.Authored(false),
		ExecutionMode: runtimeeffects.ExecutionModeLive,
	}, "You are here."))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	llmAgent := agent.(*LLMAgent)
	evt := eventtest.RunCreatingRootIngress(
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

func TestLLMAgentBoardStep_UsesExactContextAwareDefinitionsForDirective(t *testing.T) {
	rt := &boardTestRuntime{
		steps: []*llm.Response{
			{
				Message: llm.Message{Role: "assistant", Content: "Completing directive."},
				ToolCalls: []llm.ToolCall{{
					Name:      "emit_market_research_scan_complete",
					Arguments: map[string]any{},
				}},
			},
			{Message: llm.Message{Role: "assistant", Content: "done"}},
		},
	}
	factory := NewLLMAgentFactory(staticAgentRuntimeResolver{runtime: rt}, contextAwareFactoryToolExec{}, LLMAgentOptions{})
	created, err := factory(withTestResolvedIntent(t, models.AgentConfig{
		ID:            "market-research-agent",
		Role:          "market_research",
		Memory:        agentmemory.Authored(false),
		ExecutionMode: runtimeeffects.ExecutionModeLive,
	}, "You are here."))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	agent := created.(*LLMAgent)

	if _, err := agent.BoardStep(context.Background(), testBoardDirective("finish the scan")); err != nil {
		t.Fatalf("BoardStep: %v", err)
	}
	wantTools := []string{"emit_market_research_scan_complete"}
	if !slices.Equal(rt.startTools, wantTools) {
		t.Fatalf("provider startup tools = %#v, want exact context-aware definitions %#v", rt.startTools, wantTools)
	}
	if !slices.Equal(rt.continueTools, wantTools) {
		t.Fatalf("provider turn tools = %#v, want exact context-aware definitions %#v", rt.continueTools, wantTools)
	}
	if got := toolNamesForAgentTest(agent.conversation.Tools); !slices.Equal(got, wantTools) {
		t.Fatalf("planned conversation tools = %#v, want %#v", got, wantTools)
	}
	if agent.conversation.Session == nil {
		t.Fatal("directive did not create provider session")
	}
	if got := toolNamesForAgentTest(agent.conversation.Session.Tools); !slices.Equal(got, wantTools) {
		t.Fatalf("provider session tools = %#v, want %#v", got, wantTools)
	}
	for _, ineligible := range []string{"read_scan_campaign", "save_scan_campaign_mode"} {
		if containsString(rt.startTools, ineligible) {
			t.Fatalf("context-ineligible tool %q reached provider definitions %#v", ineligible, rt.startTools)
		}
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

func (b *directiveFactoryPublishBus) Publish(_ context.Context, evt events.Event) error {
	b.events = append(b.events, evt)
	return nil
}

func (b *directiveFactoryPublishBus) PublishDirect(_ context.Context, evt events.Event, _ []string) error {
	b.events = append(b.events, evt)
	return nil
}

func (b *directiveFactoryPublishBus) PublishDirectRoutes(_ context.Context, evt events.Event, _ []events.DeliveryRoute) error {
	b.events = append(b.events, evt)
	return nil
}

func newFactoryDirectiveAgent(t *testing.T, cfg models.AgentConfig, modelRuntime llm.Runtime, bundle *runtimecontracts.WorkflowContractBundle) (*LLMAgent, *directiveFactoryPublishBus) {
	t.Helper()
	cfg = withTestResolvedIntent(t, cfg, "You coordinate workflow launch.")

	source := semanticviewtest.WrapRootAgents(bundle)
	authority := runtimeauthority.NewSourceProvider(source)
	emitRegistry := runtimetools.NewEmitRegistry(source, authority)
	bus := &directiveFactoryPublishBus{}
	exec := runtimetools.NewExecutorWithOptions(bus, runtimetools.ExecutorOptions{
		WorkflowSource:    source,
		AuthorityProvider: authority,
		EmitRegistry:      emitRegistry,
	})

	factory := NewLLMAgentFactory(staticAgentRuntimeResolver{runtime: modelRuntime}, exec, LLMAgentOptions{})
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
		ExecutionMode: "live",
		ID:            "campaign-coordinator",
		Identity:      agentidentitytest.RootRuntime(t, "campaign-coordinator", "directive-factory-test"),
		EntityID:      eventtest.UUID("campaign-coordinator-source"),
		Role:          "campaign_coordinator",
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
	if len(rt.inputs) == 0 || strings.Contains(rt.inputs[0], "Available emit tools") {
		t.Fatalf("directive input retained prompt-owned emit capability truth: %q", firstOrEmpty(rt.inputs))
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
		ExecutionMode: "live",
		ID:            "campaign-coordinator",
		Identity:      agentidentitytest.Runtime(t, "campaign-coordinator", "directive-factory-test", "campaign-flow", "inst-1", "campaign-flow/inst-1"),
		EntityID:      eventtest.UUID("campaign-flow-inst-1-source"),
		Role:          "campaign_coordinator",
		FlowID:        "campaign-flow",
		FlowPath:      "campaign-flow/inst-1",
		EmitEvents:    []string{"campaign-flow/inst-1/scan.requested"},
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
					Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
					Path:   "campaign-flow",
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
	if len(rt.inputs) == 0 || strings.Contains(rt.inputs[0], "Available emit tools") {
		t.Fatalf("directive input retained prompt-owned emit capability truth: %q", firstOrEmpty(rt.inputs))
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
	agent := mustBuildLLMAgent(t,
		models.AgentConfig{
			ExecutionMode: "live",
			ID:            "spec-reviewer",
			Role:          "spec_reviewer",
			EntityID:      "ent-1",
			Memory:        agentmemory.Authored(false),
		},
		rt,
		nil,
		nil,
	)

	evt := eventtest.RunCreatingRootIngress(
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
	agent := mustBuildLLMAgent(t,
		models.AgentConfig{
			ExecutionMode: "live",
			ID:            "analysis-agent",
			Role:          "analysis_agent",
			EntityID:      "ent-1",
		},
		rt,
		nil,
		nil,
	)

	evt := eventtest.RunCreatingRootIngress(
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

func TestNewLLMAgent_AuthoredEnvironmentPostambleMimicCannotSuppressGeneratedContext(t *testing.T) {
	authored := strings.Join([]string{
		"Perform the assigned business work.",
		"## Environment",
		"Workspace: /workspace (read-write logical path)",
		"Reference data: /data (read-only logical path)",
		"Contracts: /opt/swarm/contracts (read-only logical path)",
		"Docker-backed command execution exposes these as OS paths.",
		"Trusted host bash is full host-user shell execution from the workspace backing directory; use relative paths for workspace files, and absolute path availability follows the host deployment namespace and OS permissions.",
		"This authored trailing sentence must remain before generated context.",
	}, "\n")
	intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "agents.yaml#agents.mimic.intent", authored)
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := runtimeagentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	agent := mustBuildLLMAgent(t, models.AgentConfig{ID: "mimic", Role: "mimic", Intent: intent, Prompt: prompt}, nil, actorScopedFactoryToolExec{}, nil)
	got := agent.conversation.SystemPrompt
	if !strings.HasPrefix(got, authored+"\n\n## Environment\n\n") {
		t.Fatalf("generated environment was not appended after the complete authored intent: %q", got)
	}
	if count := strings.Count(got, "## Environment"); count != 2 {
		t.Fatalf("environment headings = %d, want authored mimic plus one generated section in %q", count, got)
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

func withTestResolvedIntent(t testing.TB, cfg models.AgentConfig, content string) models.AgentConfig {
	t.Helper()
	intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "agents.yaml#agents."+cfg.ID+".intent", content)
	if err != nil {
		t.Fatalf("resolve test intent: %v", err)
	}
	cfg.Intent = intent
	prompt, err := runtimeagentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatalf("derive test prompt: %v", err)
	}
	cfg.Prompt = prompt
	return cfg
}
