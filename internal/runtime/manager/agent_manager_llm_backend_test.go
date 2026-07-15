package manager

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestAgentManagerDefaultsLLMBackendFromCanonicalProfile(t *testing.T) {
	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{LLMBackend: "openai_compatible"})
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{
		Config: models.AgentConfig{
			ExecutionMode: "live",
			ID:            "agent-1",
			Role:          "reviewer",
			Model:         "regular",
		},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}
	cfg, ok := am.GetAgentConfig("agent-1")
	if !ok {
		t.Fatal("spawned agent config is absent")
	}
	got := cfg.LLMBackend
	if got != "openai_compatible" {
		t.Fatalf("llm_backend = %q, want openai_compatible", got)
	}
}

func TestResolveAgentModelLiveBackendsDropInactiveMockArtifact(t *testing.T) {
	artifact := capturedMockAlternative()
	for _, tc := range []struct {
		backend   string
		provider  string
		transport string
	}{
		{backend: llmselection.BackendAnthropic, provider: llmselection.ProviderAnthropic, transport: llmselection.TransportAPI},
		{backend: llmselection.BackendClaudeCLI, provider: llmselection.ProviderClaude, transport: llmselection.TransportCLI},
		{backend: llmselection.BackendOpenAICompatible, provider: llmselection.ProviderOpenAICompatible, transport: llmselection.TransportAPI},
		{backend: llmselection.BackendOpenAIResponses, provider: llmselection.ProviderOpenAI, transport: llmselection.TransportAPI},
	} {
		t.Run(tc.backend, func(t *testing.T) {
			am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{LLMBackend: tc.backend})
			cfg := models.AgentConfig{ID: "agent-" + tc.backend, Model: "regular", LLMBackend: tc.backend, Mock: artifact}
			if err := am.resolveAgentModel(&cfg); err != nil {
				t.Fatalf("resolveAgentModel: %v", err)
			}
			if cfg.LLMBackend != tc.backend || cfg.ExecutionMode != runtimeeffects.ExecutionModeLive {
				t.Fatalf("selected backend/mode = %q/%q, want %q/live", cfg.LLMBackend, cfg.ExecutionMode, tc.backend)
			}
			if cfg.ResolvedLLMProvider != tc.provider || cfg.ResolvedLLMTransport != tc.transport {
				t.Fatalf("resolved provider/transport = %q/%q, want %q/%q", cfg.ResolvedLLMProvider, cfg.ResolvedLLMTransport, tc.provider, tc.transport)
			}
			if cfg.Mock.Configured() {
				t.Fatalf("live selected descriptor retained inactive mock artifact: %#v", cfg.Mock)
			}
		})
	}
}

func TestResolveAgentModelMockRetainsAndRequiresCapturedArtifact(t *testing.T) {
	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{LLMBackend: llmselection.BackendMock})
	artifact := capturedMockAlternative()
	cfg := models.AgentConfig{ID: "mock-agent", Model: "regular", LLMBackend: llmselection.BackendMock, Mock: artifact}
	if err := am.resolveAgentModel(&cfg); err != nil {
		t.Fatalf("resolveAgentModel: %v", err)
	}
	if cfg.ExecutionMode != runtimeeffects.ExecutionModeMock || !reflect.DeepEqual(cfg.Mock, artifact) {
		t.Fatalf("mock selected descriptor = mode %q artifact %#v, want exact captured artifact %#v", cfg.ExecutionMode, cfg.Mock, artifact)
	}

	missing := models.AgentConfig{ID: "mock-agent-missing", Model: "regular", LLMBackend: llmselection.BackendMock}
	if err := am.resolveAgentModel(&missing); err == nil || !strings.Contains(err.Error(), "does not declare a mock performance") {
		t.Fatalf("missing mock artifact error = %v", err)
	}
}

func TestAuthoredMockAlternativeStaticAndInstantiatedAgentsSpawnPersistRecoverLive(t *testing.T) {
	artifact := capturedMockAlternative()
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	staticCfg, err := buildStaticFlowAgentConfig(source, "static-support", "static-support", "static-worker", runtimecontracts.AgentRegistryEntry{
		ID: "static-worker", Role: "worker", Model: "regular", MemoryPlan: agentmemory.PlatformDefault(), Mock: artifact,
	}, nil)
	if err != nil {
		t.Fatalf("buildStaticFlowAgentConfig: %v", err)
	}
	instantiatedCfg, err := buildFlowAgentConfig(source, "template-support", "inst-1", "entity-1", "template-support/inst-1", "worker", runtimecontracts.AgentRegistryEntry{
		ID: "template-worker-{instance_id}", Role: "worker", Model: "regular", MemoryPlan: agentmemory.PlatformDefault(), Mock: artifact,
	}, map[string]string{"instance_id": "inst-1"}, nil, nil)
	if err != nil {
		t.Fatalf("buildFlowAgentConfig: %v", err)
	}
	for name, cfg := range map[string]models.AgentConfig{"static": staticCfg, "instantiated": instantiatedCfg} {
		if !cfg.Mock.Configured() {
			t.Fatalf("%s materialization did not carry authored mock alternative", name)
		}
	}

	store := &liveMockAlternativePersistence{}
	spawned := map[string]models.AgentConfig{}
	am := NewAgentManagerWithOptions(&recoveryTestBus{}, func(cfg models.AgentConfig) (Agent, error) {
		spawned[cfg.ID] = cfg
		return recoveryTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{LLMBackend: llmselection.BackendAnthropic}, store)
	for _, cfg := range []models.AgentConfig{staticCfg, instantiatedCfg} {
		if err := am.spawnAgentInternal(context.Background(), PersistedAgent{Config: cfg, Status: "active", StartedAt: time.Now().UTC()}, true); err != nil {
			t.Fatalf("spawnAgentInternal(%s): %v", cfg.ID, err)
		}
	}
	assertLiveMockAlternativeProjection(t, "spawned", spawned, staticCfg.ID, instantiatedCfg.ID)
	if len(store.records) != 2 {
		t.Fatalf("persisted records = %d, want 2", len(store.records))
	}
	persisted := map[string]models.AgentConfig{}
	for _, rec := range store.records {
		persisted[rec.Config.ID] = rec.Config
	}
	assertLiveMockAlternativeProjection(t, "persisted", persisted, staticCfg.ID, instantiatedCfg.ID)

	recovered := map[string]models.AgentConfig{}
	recoveryManager := NewAgentManagerWithOptions(&recoveryTestBus{}, func(cfg models.AgentConfig) (Agent, error) {
		recovered[cfg.ID] = cfg
		return recoveryTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{LLMBackend: llmselection.BackendAnthropic}, store)
	if err := recoveryManager.Recover(managedExecutionTestContext(t, context.Background())); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertLiveMockAlternativeProjection(t, "recovered", recovered, staticCfg.ID, instantiatedCfg.ID)
}

type liveMockAlternativePersistence struct {
	recoveryTestStore
	records []PersistedAgent
}

func (s *liveMockAlternativePersistence) UpsertAgent(_ context.Context, rec PersistedAgent) error {
	s.records = append(s.records, rec)
	return nil
}

func (s *liveMockAlternativePersistence) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return append([]PersistedAgent(nil), s.records...), nil
}

func capturedMockAlternative() mockperformance.Performance {
	return mockperformance.Performance{
		Kind: mockperformance.KindPython, Module: "mocks/agent.py", SourcePath: "mocks/agent.py",
		Source: []byte("def handle(input):\n    return {'text': 'mock'}\n"), Digest: "sha256:test-captured-mock-alternative",
	}
}

func assertLiveMockAlternativeProjection(t *testing.T, phase string, configs map[string]models.AgentConfig, ids ...string) {
	t.Helper()
	for _, id := range ids {
		cfg, ok := configs[id]
		if !ok {
			t.Fatalf("%s config %q missing", phase, id)
		}
		if cfg.LLMBackend != llmselection.BackendAnthropic || cfg.ExecutionMode != runtimeeffects.ExecutionModeLive || cfg.Mock.Configured() {
			t.Fatalf("%s config %q = backend %q mode %q mock %#v, want anthropic/live without inactive artifact", phase, id, cfg.LLMBackend, cfg.ExecutionMode, cfg.Mock)
		}
	}
}
