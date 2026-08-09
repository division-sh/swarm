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
	"github.com/division-sh/swarm/internal/runtime/sessions"
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
	registry := sessions.NewInMemoryRegistry(0)
	harness := effecttest.New()
	live := &observedClaudeRuntime{}
	runtimes, err := NewAgentRuntimeSet(profile, RuntimeFactory{
		Cfg:                  cfg,
		Sessions:             registry,
		LiveSessions:         NewTransientLiveSessionAcquirer(registry),
		CompletionController: runtimeeffects.NewCompletionController(harness, harness, harness, harness),
	}, live)
	if err != nil {
		t.Fatalf("NewAgentRuntimeSet: %v", err)
	}

	mockRuntime, err := runtimes.RuntimeForAgent(resolvedMockAgent("mock-agent"))
	if err != nil {
		t.Fatalf("RuntimeForAgent(mock): %v", err)
	}
	if _, ok := mockRuntime.(*MockRuntime); !ok {
		t.Fatalf("mock runtime = %T, want *MockRuntime", mockRuntime)
	}
	if live.contractCalls.Load() != 0 {
		t.Fatalf("configured live adapter was touched %d time(s) while resolving an exact mock", live.contractCalls.Load())
	}

	liveRuntime, err := runtimes.RuntimeForAgent(resolvedClaudeAgent("live-agent"))
	if err != nil {
		t.Fatalf("RuntimeForAgent(live): %v", err)
	}
	if liveRuntime != live {
		t.Fatalf("live runtime = %T, want injected configured adapter", liveRuntime)
	}
	if live.contractCalls.Load() != 1 {
		t.Fatalf("configured live adapter contract checks = %d, want 1", live.contractCalls.Load())
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
	if _, err := runtimes.RuntimeForAgent(actor); err == nil || !strings.Contains(err.Error(), "conflicts with effective selection") {
		t.Fatalf("RuntimeForAgent drift error = %v", err)
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
	if _, err := runtimes.RuntimeForAgent(actor); err == nil || !strings.Contains(err.Error(), "no compiled Python performance") {
		t.Fatalf("RuntimeForAgent uncompiled error = %v", err)
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
