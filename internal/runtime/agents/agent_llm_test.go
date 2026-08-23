package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
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
	if actor.Identity.IsZero() {
		actor.Identity = agentidentity.Identity{
			Name:  agentidentity.Name{AgentID: actor.ID, Owner: "agent-runtime-resolver-test", Source: agentidentity.NameSourceRuntimeCreated},
			Route: agentidentity.RootRoute(),
		}
	}
	return llm.AgentRuntimeResolution{Actor: actor, Runtime: wrapAgentTestRuntime(r.runtime)}, nil
}

const agentTestBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type agentTestBehaviorRuntime interface {
	llm.Runtime
	continueAgentTest(context.Context, *llm.Session, llm.Message) (*llm.Response, error)
}

type agentTestRuntimeAdapter struct {
	llm.Runtime
}

type agentTestFrameObserver interface {
	observeAgentTestFrame(agentframe.Frame)
}

func wrapAgentTestRuntime(runtime llm.Runtime) llm.Runtime {
	if runtime == nil {
		runtime = llm.NewNoopRuntime(llm.MockProviderContract())
	}
	return &agentTestRuntimeAdapter{Runtime: runtime}
}

func (r *agentTestRuntimeAdapter) ProviderContract() llm.ProviderContract {
	return llm.MockProviderContract()
}

func (r *agentTestRuntimeAdapter) StartSession(ctx context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	session, err := r.Runtime.StartSession(ctx, agentID, systemPrompt, tools)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, errors.New("agent test runtime returned nil session")
	}
	if execution, ok := agentmemory.FromContext(ctx); ok {
		session.Memory = execution.Plan
		session.MemoryIdentity = execution.Identity
	}
	session.ID = eventtest.UUID("agent-test-session:" + session.ID)
	session.SystemPrompt = systemPrompt
	session.Tools = append([]llm.ToolDefinition(nil), tools...)
	return session, nil
}

func (r *agentTestRuntimeAdapter) ContinueManagedSession(ctx context.Context, session *llm.Session, call llm.ManagedCall) (*llm.Response, error) {
	if typed, ok := r.Runtime.(llm.ManagedSessionRuntime); ok {
		return typed.ContinueManagedSession(ctx, session, call)
	}
	behavior, ok := r.Runtime.(agentTestBehaviorRuntime)
	if !ok {
		return nil, fmt.Errorf("agent test runtime %T has no scripted managed behavior", r.Runtime)
	}
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	if observer, ok := r.Runtime.(agentTestFrameObserver); ok {
		observer.observeAgentTestFrame(call.Frame())
	}
	response, err := behavior.continueAgentTest(ctx, session, message)
	if err != nil || response == nil || response.CapabilitySurface != nil {
		return response, err
	}
	if surface, ok := managedcapabilities.FromContext(ctx); ok {
		evidence := make([]managedcapabilities.DeliveryEvidence, 0, len(surface.Tools))
		for _, tool := range surface.Tools {
			if !tool.Capability.Visible || !tool.Capability.Callable {
				continue
			}
			for _, binding := range tool.Bindings {
				evidence = append(evidence, managedcapabilities.DeliveryEvidence{
					BindingKind: binding.Kind, ExactName: binding.ExactName,
					Kind: binding.RequiredEvidenceKind, Status: managedcapabilities.EvidenceConfirmed,
				})
			}
		}
		observed, observeErr := surface.Observe(evidence...)
		if observeErr != nil {
			return nil, observeErr
		}
		response.CapabilitySurface = &observed
	}
	return response, nil
}

func (r *agentTestRuntimeAdapter) ContinueForkChatSession(context.Context, *llm.Session, llm.ForkChatCall) (*llm.Response, error) {
	return nil, errors.New("agent test runtime does not serve operator fork chat")
}

func (r *agentTestRuntimeAdapter) PersistConversationSnapshot(context.Context, *llm.Session) error {
	return nil
}

func agentManagedTestContext(t testing.TB, agent *LLMAgent) context.Context {
	t.Helper()
	if agent == nil {
		t.Fatal("agent test context requires agent")
	}
	token := runtimeeffects.LifecycleToken{
		Identity: agent.cfg.Identity, RuntimeEpoch: 1, AgentID: agent.cfg.ID, Generation: 1,
	}
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"agent-test-runtime",
		1,
		"",
		"agent-test-census",
		agentTestBundleHash,
		nil,
	)
	if err != nil {
		t.Fatalf("agent test admission: %v", err)
	}
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact(agentTestBundleHash)
	if err != nil {
		t.Fatalf("agent test bundle source: %v", err)
	}
	ctx := runtimeeffects.WithLifecycleToken(context.Background(), token)
	ctx = managedexecution.WithAdmission(ctx, admission)
	return runtimecorrelation.WithBundleSourceFact(ctx, fact)
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
	if toolExecutor == nil {
		toolExecutor = actorScopedFactoryToolExec{}
	}
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
	if cfg.Identity.IsZero() {
		if flowPath := strings.Trim(strings.TrimSpace(cfg.FlowPath), "/"); flowPath != "" {
			scope, instance, ok := strings.Cut(flowPath, "/")
			if !ok || strings.Contains(instance, "/") {
				t.Fatalf("test flow path %q is not a concrete flow instance", flowPath)
			}
			cfg.Identity = agentidentitytest.Runtime(t, cfg.ID, "agent-llm-test", scope, instance, flowPath)
		} else {
			cfg.Identity = agentidentitytest.RootRuntime(t, cfg.ID, "agent-llm-test")
		}
	}
	agent, err := newLLMAgent(cfg, wrapAgentTestRuntime(modelRuntime), toolExecutor, tools, LLMAgentOptions{})
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
	frames        []agentframe.Frame
}

func (r *boardTestRuntime) observeAgentTestFrame(frame agentframe.Frame) {
	r.frames = append(r.frames, frame)
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

func (r *boardTestRuntime) continueAgentTest(ctx context.Context, s *llm.Session, message llm.Message) (*llm.Response, error) {
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
		eventtest.UUID("analysis-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	if _, err := agent.OnEvent(agentManagedTestContext(t, agent), evt); err != nil {
		if failure, ok := runtimefailures.As(err); ok {
			t.Fatalf("OnEvent: %v attributes=%#v", err, failure.Failure.Detail.Attributes)
		}
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
				"mode-event", "work.requested", "runtime", "", []byte(`{}`), 0, eventtest.UUID("mode-run"), "", events.EventEnvelope{}, time.Time{}, tc.inboundMode,
			)

			_, err := agent.OnEvent(agentManagedTestContext(t, agent), evt)
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

	_, err := agent.BoardStep(agentManagedTestContext(t, agent), testBoardDirective("start a corpus run"))
	if err == nil {
		t.Fatal("expected directive without action to fail")
	}
	if !strings.Contains(err.Error(), "without taking action") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBoardStep_RemediatesAndSucceedsWhenDirectiveEmits(t *testing.T) {
	rt := &boardTestRuntime{
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
	}
	agent := mustBuildLLMAgent(t,
		models.AgentConfig{ExecutionMode: "live", ID: "coordinator-1", Role: "coordinator"},
		rt,
		boardEmitExecutor{},
		[]llm.ToolDefinition{{Name: "emit_scan_requested"}},
	)

	got, err := agent.BoardStep(agentManagedTestContext(t, agent), testBoardDirective("start a corpus run"))
	if err != nil {
		t.Fatalf("BoardStep: %v", err)
	}
	if got != "scan_requested emitted" && got != "Dispatching workflow now." {
		t.Fatalf("unexpected response: %q", got)
	}
	if len(rt.frames) < 2 || rt.frames[0].Turn.Kind != agentframe.TurnBoardDirective || rt.frames[1].Turn.Kind != agentframe.TurnRemediation {
		t.Fatalf("board frame kinds = %#v, want directive then remediation", rt.frames)
	}
	if rt.frames[1].Turn.ParentFrameID != rt.frames[0].FrameID || rt.frames[1].FrameID == rt.frames[0].FrameID {
		t.Fatalf("board remediation chronology = first %q second parent %q second %q", rt.frames[0].FrameID, rt.frames[1].Turn.ParentFrameID, rt.frames[1].FrameID)
	}
}

type drainedDirectiveProviderRuntime struct {
	calls int
}

func (r *drainedDirectiveProviderRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	return &llm.Session{ID: "drained-directive", AgentID: agentID, SystemPrompt: systemPrompt, Tools: tools}, nil
}

func (r *drainedDirectiveProviderRuntime) ContinueManagedSession(ctx context.Context, session *llm.Session, call llm.ManagedCall) (*llm.Response, error) {
	r.calls++
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return nil, errors.New("directive provider capability surface missing")
	}
	usageTarget := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: surface.Authority.ID, RunID: surface.Authority.RunID,
		AgentID: session.AgentID, AgentIdentity: session.MemoryIdentity.Agent, SessionID: session.ID,
		Memory: session.Memory, FlowInstance: session.MemoryIdentity.FlowInstance(),
	}
	if !usageTarget.Valid() {
		return nil, fmt.Errorf("directive provider usage target is invalid: %+v", usageTarget)
	}
	ctx = runtimeeffects.WithUsageTarget(ctx, usageTarget)
	handle, err := runtimeeffects.BeginManagedCompletion(ctx, "anthropic_api", []byte("directive"), call.Frame(), nil)
	if err != nil {
		return nil, err
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		return nil, err
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"directive": true}); err != nil {
		return nil, err
	}
	target := handle.Attempt().Authority.Target
	surfaceJSON, err := json.Marshal(surface)
	if err != nil {
		return nil, err
	}
	input, output := int64(1), int64(1)
	result, err := handle.SettleCompletion(ctx, runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled},
		Usage: runtimeeffects.CompletionUsage{
			ResolvedModel: "effect-test", Exactness: runtimeeffects.CompletionUsageExact,
			InputTokens: &input, OutputTokens: &output,
		},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, RunID: target.RunID, AgentID: target.AgentID, SessionID: target.SessionID,
			Identity: agentmemory.Identity{RunID: target.RunID, Agent: target.AgentIdentity}, Memory: target.Memory,
			FlowInstance: target.FlowInstance, CapabilitySurfaceID: surface.ID, CapabilitySurface: surfaceJSON,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: target.FlowInstance, AgentID: target.AgentID, AgentIdentity: target.AgentIdentity,
			Model: "effect-test", BackendProfile: "effect-test", Provider: "effect-test", Transport: "api",
			ResolvedModel: "effect-test", InvocationType: "directive",
		},
		Now: time.Now().UTC(),
	})
	if err != nil {
		return nil, err
	}
	if !result.Drained() || !result.OriginSettled {
		return nil, fmt.Errorf("directive completion was not drained: %+v", result)
	}
	return nil, nil
}

func TestBoardStep_DrainedDirectiveStopsBeforeRemediation(t *testing.T) {
	harness := effecttest.New()
	harness.SettleOrigin = true
	harness.CompletionDisposition = runtimeeffects.CompletionSettlementDrained
	runtime := &drainedDirectiveProviderRuntime{}
	agent := mustBuildLLMAgent(t, models.AgentConfig{
		ExecutionMode: "live", ID: harness.Token.AgentID, Identity: harness.Token.Identity,
		FlowPath: harness.Token.Identity.FlowInstance(), Role: "coordinator",
	}, runtime, boardEmitExecutor{}, nil)

	ctx := runtimedelivery.WithoutClaim(harness.CompletionContext(t.Name()))
	origin := runtimeagentcontrol.DirectiveExecutionOrigin{
		OperationID: "00000000-0000-0000-0000-000000000901", ExecutionOwnerID: "00000000-0000-0000-0000-000000000902",
	}
	ctx = runtimeeffects.WithDirectiveCompletionOrigin(ctx, origin)
	ctx, observation := runtimeeffects.WithCompletionSettlementObserver(ctx)
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if err != nil {
		t.Fatalf("directive test bundle source: %v", err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok {
		t.Fatal("directive completion authority missing")
	}
	target := authority.Target
	ctx = runtimecorrelation.WithRunID(ctx, target.RunID)
	directive := runtimeagentcontrol.BoardDirective{
		Directive: "continue",
		Event: eventtest.DiagnosticDirect(
			"00000000-0000-0000-0000-000000000903", events.EventType(runtimeagentcontrol.DirectiveEventType),
			"runtime", "", []byte(`{"directive_text":"continue","mode":"directive"}`), 0,
			target.RunID, "", events.EventEnvelope{}, time.Now().UTC(),
		),
		RunIDResolution: runtimeagentcontrol.RunResolutionSpecified,
		Source:          runtimeagentcontrol.DirectiveSourceV1RPC,
	}
	if _, err := agent.BoardStep(ctx, directive); !errors.Is(err, runtimeagentcontrol.ErrDirectiveProviderDrained) {
		if failure, ok := runtimefailures.EnvelopeFromError(err); ok {
			t.Fatalf("drained directive BoardStep error=%v attributes=%v", err, failure.Detail.Attributes)
		}
		t.Fatalf("drained directive BoardStep error=%v", err)
	}
	if runtime.calls != 1 {
		t.Fatalf("drained directive provider calls=%d, want one without remediation", runtime.calls)
	}
	observed := observation()
	if !observed.OriginSettled || observed.Disposition != runtimeeffects.CompletionSettlementDrained || !observed.Origin.Directive.Same(origin) {
		t.Fatalf("drained directive observation=%+v", observed)
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
	factory := NewLLMAgentFactory(staticAgentRuntimeResolver{runtime: llm.NewNoopRuntime(llm.MockProviderContract())}, actorScopedFactoryToolExec{}, LLMAgentOptions{})
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

func TestBoardDirectiveToolSuccessRequiresCanonicalContinuationProjection(t *testing.T) {
	if toolMessageHasSuccessfulResult(`[{"ok":true,"result":"retired raw result"}]`) {
		t.Fatal("retired raw tool-result array satisfied a board directive")
	}
	if !toolMessageHasSuccessfulResult(`{"kind":"tool_continuation","tool_result":[{"ok":true,"result":"handled"}]}`) {
		t.Fatal("canonical tool-continuation projection did not satisfy a board directive")
	}
}

func TestHumanTaskOutcomeInjectsCanonicalAskHumanToolResult(t *testing.T) {
	agent := mustBuildLLMAgent(t, models.AgentConfig{ExecutionMode: "live", ID: "reviewer"}, nil, nil, nil)
	evt := eventtest.RunCreatingRootIngress(
		"00000000-0000-0000-0000-000000000401",
		events.EventType("human_task.approved"),
		"runtime",
		"",
		[]byte(`{"card_id":"00000000-0000-0000-0000-000000000402","decision":"approved"}`),
		0,
		"00000000-0000-0000-0000-000000000403",
		"",
		events.EventEnvelope{},
		time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC),
	)
	if err := agent.injectHumanTaskToolResult(context.Background(), evt); err != nil {
		t.Fatalf("inject human-task result: %v", err)
	}
	if len(agent.conversation.Messages) != 1 {
		t.Fatalf("conversation messages = %#v, want one async tool result", agent.conversation.Messages)
	}
	var result []map[string]any
	if err := json.Unmarshal([]byte(agent.conversation.Messages[0].Content), &result); err != nil {
		t.Fatalf("decode async tool result: %v", err)
	}
	if len(result) != 1 || result[0]["name"] != runtimetools.AskHumanToolName || result[0]["ok"] != true {
		t.Fatalf("async tool result = %#v, want canonical ask_human success", result)
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
		eventtest.UUID("market-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "root-run-id"),
		time.Time{},
	)

	if _, err := llmAgent.OnEvent(agentManagedTestContext(t, llmAgent), evt); err != nil {
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

	if _, err := agent.BoardStep(agentManagedTestContext(t, agent), testBoardDirective("finish the scan")); err != nil {
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

func (r *directiveFactoryRuntime) continueAgentTest(_ context.Context, _ *llm.Session, message llm.Message) (*llm.Response, error) {
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
		Identity:      agentidentitytest.RootDeclared(t, "campaign-coordinator", "swarm-test://root/agents/campaign-coordinator"),
		EntityID:      eventtest.UUID("campaign-coordinator-source"),
		Role:          "campaign_coordinator",
		EmitEvents:    []string{"scan.requested"},
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

	got, err := agent.BoardStep(agentManagedTestContext(t, agent), testBoardDirective("start a corpus run"))
	if err != nil {
		t.Fatalf("BoardStep: %v inputs=%#v events=%#v", err, rt.inputs, bus.events)
	}
	if got != "Dispatching workflow now." {
		t.Fatalf("directive response = %q, want terminal emit turn text", got)
	}
	if !containsString(rt.startTools, "emit_scan_requested") {
		t.Fatalf("session tools = %v, want emit_scan_requested", rt.startTools)
	}
	if !containsString(rt.startTools, runtimetools.NotifyHumanToolName) {
		t.Fatalf("BoardStep session tools = %v, want canonical %s", rt.startTools, runtimetools.NotifyHumanToolName)
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
	const owner = "swarm-test://campaign-flow/agents/campaign-coordinator"
	flow := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "campaign-flow",
			Flow: "campaign-flow",
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"campaign-coordinator": {
				ID: "campaign-coordinator", Role: "campaign_coordinator", EmitEvents: []string{"scan.requested"},
			},
		},
		AgentURIs: map[string]string{"campaign-coordinator": owner},
		Schema:    runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
		Path:      "campaign-flow",
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				"campaign-flow/campaign-coordinator": {Kind: "agent", FlowID: "campaign-flow", LocalID: "campaign-coordinator", Full: owner},
			},
			ByURI: map[string]runtimecontracts.ContractURIRef{
				owner: {Kind: "agent", FlowID: "campaign-flow", LocalID: "campaign-coordinator", Full: owner},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: flow,
			ByID: map[string]*runtimecontracts.FlowContractView{"campaign-flow": flow},
		},
	}
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
		Identity:      agentidentitytest.Declared(t, "campaign-coordinator", owner, "campaign-flow", "inst-1", "campaign-flow/inst-1"),
		EntityID:      eventtest.UUID("campaign-flow-inst-1-source"),
		Role:          "campaign_coordinator",
		FlowID:        "campaign-flow",
		FlowPath:      "campaign-flow/inst-1",
		EmitEvents:    []string{"campaign-flow/inst-1/scan.requested"},
	}, rt, bundle)

	got, err := agent.BoardStep(agentManagedTestContext(t, agent), testBoardDirective("start a corpus run"))
	if err != nil {
		t.Fatalf("BoardStep: %v inputs=%#v events=%#v", err, rt.inputs, bus.events)
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
	if len(rt.inputs) < 2 || !strings.Contains(rt.inputs[1], `"kind":"remediation"`) ||
		!strings.Contains(rt.inputs[1], `"reason":"directive_completed_without_action"`) ||
		strings.Contains(rt.inputs[1], "I will trigger the workflow now") {
		t.Fatalf("remediation input = %q, want typed reason without prior assistant prose", firstOrEmpty(rt.inputs[1:]))
	}
	if len(bus.events) != 1 || string(bus.events[0].Type()) != "campaign-flow/inst-1/scan.requested" {
		t.Fatalf("published events = %#v, want one externalized scan.requested event", bus.events)
	}
}

type taskRetryRuntime struct {
	startCalls    int
	continueCalls int
	frames        []agentframe.Frame
}

func (r *taskRetryRuntime) observeAgentTestFrame(frame agentframe.Frame) {
	r.frames = append(r.frames, frame)
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

func (r *taskRetryRuntime) continueAgentTest(_ context.Context, _ *llm.Session, _ llm.Message) (*llm.Response, error) {
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
		eventtest.UUID("spec-review-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	if _, err := agent.OnEvent(agentManagedTestContext(t, agent), evt); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}
	if rt.continueCalls != 2 {
		t.Fatalf("continue calls = %d, want 2", rt.continueCalls)
	}
	if rt.startCalls != 2 {
		t.Fatalf("start calls = %d, want 2 after reset", rt.startCalls)
	}
	if len(rt.frames) != 2 || rt.frames[0].Turn.Kind != agentframe.TurnInitial || rt.frames[1].Turn.Kind != agentframe.TurnInitial {
		t.Fatalf("reset execution frames = %#v, want two initial occurrences", rt.frames)
	}
	if rt.frames[0].FrameID == rt.frames[1].FrameID || rt.frames[0].Turn.ParentFrameID != "" || rt.frames[1].Turn.ParentFrameID != "" {
		t.Fatalf("reset frame identities = first %q second %q", rt.frames[0].FrameID, rt.frames[1].FrameID)
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

func (r *runIDCaptureRuntime) continueAgentTest(ctx context.Context, _ *llm.Session, _ llm.Message) (*llm.Response, error) {
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

	runID := eventtest.UUID("run-123")
	evt := eventtest.RunCreatingRootIngress(
		"evt-1",
		"scoring/scoring.requested",
		"runtime",
		"",
		[]byte(`{"entity_id":"ent-1"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	if _, err := agent.OnEvent(agentManagedTestContext(t, agent), evt); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}
	if len(rt.startRunIDs) != 1 || rt.startRunIDs[0] != runID {
		t.Fatalf("start session run_ids = %v, want [%s]", rt.startRunIDs, runID)
	}
	if len(rt.continueRunIDs) != 1 || rt.continueRunIDs[0] != runID {
		t.Fatalf("continue session run_ids = %v, want [%s]", rt.continueRunIDs, runID)
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
