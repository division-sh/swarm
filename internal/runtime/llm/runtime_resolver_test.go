package llm

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
)

type observedClaudeRuntime struct {
	NoopRuntime
	contractCalls atomic.Int32
}

func (r *observedClaudeRuntime) ProviderContract() ProviderContract {
	r.contractCalls.Add(1)
	return ClaudeCLIProviderContract()
}

func (*observedClaudeRuntime) ProbeStartupVisibleToolSurface(context.Context, models.AgentConfig, string, []ToolDefinition) (*Response, error) {
	return &Response{}, nil
}

func TestAgentRuntimeSetLazilyBindsExactMockInsteadOfConfiguredLiveAdapter(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatalf("resolve claude profile: %v", err)
	}
	cfg := &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendClaudeCLI}}
	harness := effecttest.New()
	live := &observedClaudeRuntime{}
	runtimes, err := NewAgentRuntimeSet(profile, RuntimeFactory{
		Cfg:                  cfg,
		CompletionController: liveTestCompletionController(harness, harness, harness, harness),
	}, live)
	if err != nil {
		t.Fatalf("NewAgentRuntimeSet: %v", err)
	}

	mockResolution, err := runtimes.ResolveAgentRuntime(resolvedMockAgent("mock-agent"))
	if err != nil {
		t.Fatalf("ResolveAgentRuntime(mock): %v", err)
	}
	if _, ok := mockResolution.Runtime.(*MockRuntime); !ok {
		t.Fatalf("mock runtime = %T, want *MockRuntime", mockResolution.Runtime)
	}
	if mockResolution.Actor.ResolvedLLMBackend != llmselection.BackendMock || mockResolution.Actor.ExecutionMode != runtimeeffects.ExecutionModeMock {
		t.Fatalf("mock actor selection = %q/%q", mockResolution.Actor.ResolvedLLMBackend, mockResolution.Actor.ExecutionMode)
	}
	if live.contractCalls.Load() != 0 {
		t.Fatalf("configured live adapter was touched %d time(s) while resolving an exact mock", live.contractCalls.Load())
	}

	liveResolution, err := runtimes.ResolveAgentRuntime(resolvedClaudeAgent("live-agent"))
	if err != nil {
		t.Fatalf("ResolveAgentRuntime(live): %v", err)
	}
	if liveResolution.Runtime != live {
		t.Fatalf("live runtime = %T, want injected configured adapter", liveResolution.Runtime)
	}
	if live.contractCalls.Load() != 1 {
		t.Fatalf("configured live adapter contract checks = %d, want 1", live.contractCalls.Load())
	}
}

func TestResolveAgentExecutionPreservesAuthoredIntentAndUsesConfiguredLiveModelProfile(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	actor := resolvedMockAgent("custom-alias-mock")
	actor.LLMBackend = ""
	actor.Model = llmselection.ModelAliasRegular
	actor.ResolvedLLMBackend = llmselection.BackendOpenAIResponses
	actor.ResolvedLLMProvider = llmselection.ProviderOpenAI
	actor.ResolvedLLMTransport = llmselection.TransportAPI
	actor.ResolvedModel = "stale-model"
	resolved, err := ResolveAgentExecution(profile, llmselection.ModelAliases{
		llmselection.ModelAliasRegular: {llmselection.BackendClaudeCLI: "configured-live-model"},
	}, actor)
	if err != nil {
		t.Fatalf("ResolveAgentExecution: %v", err)
	}
	if resolved.Actor.LLMBackend != "" {
		t.Fatalf("authored llm_backend = %q, want preserved blank intent", resolved.Actor.LLMBackend)
	}
	if resolved.Actor.Model != llmselection.ModelAliasRegular {
		t.Fatalf("authored model alias = %q, want %q", resolved.Actor.Model, llmselection.ModelAliasRegular)
	}
	if resolved.Actor.ResolvedLLMBackend != llmselection.BackendMock || resolved.Actor.ExecutionMode != runtimeeffects.ExecutionModeMock {
		t.Fatalf("derived backend/mode = %q/%q, want mock/mock", resolved.Actor.ResolvedLLMBackend, resolved.Actor.ExecutionMode)
	}
	if resolved.Actor.ResolvedModel != "configured-live-model" {
		t.Fatalf("resolved model = %q, want configured live-profile alias target", resolved.Actor.ResolvedModel)
	}
}

func TestAgentRuntimeSetRejectsDescriptorSelectionDriftBeforeDispatch(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatalf("resolve claude profile: %v", err)
	}
	runtimes, err := NewAgentRuntimeSet(profile, RuntimeFactory{}, &observedClaudeRuntime{})
	if err != nil {
		t.Fatalf("NewAgentRuntimeSet: %v", err)
	}
	actor := resolvedMockAgent("drifted-agent")
	actor.LLMBackend = llmselection.BackendClaudeCLI
	if _, err := runtimes.ResolveAgentRuntime(actor); err == nil || !strings.Contains(err.Error(), "conflicts with an exact mock performance") {
		t.Fatalf("ResolveAgentRuntime drift error = %v", err)
	}
}

func TestAgentRuntimeSetRejectsUncompiledMockArtifactBeforeDispatch(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatalf("resolve claude profile: %v", err)
	}
	runtimes, err := NewAgentRuntimeSet(profile, RuntimeFactory{}, &observedClaudeRuntime{})
	if err != nil {
		t.Fatalf("NewAgentRuntimeSet: %v", err)
	}
	actor := resolvedMockAgent("uncompiled-agent")
	actor.Mock.Source = nil
	actor.Mock.Digest = ""
	if _, err := runtimes.ResolveAgentRuntime(actor); err == nil || !strings.Contains(err.Error(), "no compiled Python performance") {
		t.Fatalf("ResolveAgentRuntime uncompiled error = %v", err)
	}
}

func resolvedMockAgent(id string) models.AgentConfig {
	return models.AgentConfig{
		ID:                   id,
		LLMBackend:           llmselection.BackendMock,
		ResolvedLLMProvider:  llmselection.ProviderMock,
		ResolvedLLMTransport: llmselection.TransportMock,
		ExecutionMode:        runtimeeffects.ExecutionModeMock,
		Mock: mockperformance.Performance{
			Kind: mockperformance.KindPython, Module: "mocks/agent.py",
			Source: []byte("def handle(input):\n    return {'text': 'mock'}\n"), Digest: "sha256:test-runtime-resolver",
		},
	}
}

func resolvedClaudeAgent(id string) models.AgentConfig {
	return models.AgentConfig{
		ID:                   id,
		LLMBackend:           llmselection.BackendClaudeCLI,
		ResolvedLLMProvider:  llmselection.ProviderClaude,
		ResolvedLLMTransport: llmselection.TransportCLI,
		ExecutionMode:        runtimeeffects.ExecutionModeLive,
	}
}
