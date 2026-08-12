package manager

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestAgentManagerDefaultsLLMBackendFromCanonicalProfile(t *testing.T) {
	am := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{LLMBackend: "openai_compatible"})
	if err := am.spawnAgentInternal(testAuthorActivityContext(context.Background()), PersistedAgent{
		Config: managerTestAgentConfig(models.AgentConfig{
			ExecutionMode: "live",
			ID:            "agent-1",
			Identity:      runtimeagentidentitytest.RootRuntime(t, "agent-1", "manager-llm-backend-test"),
			Role:          "reviewer",
			Model:         "regular",
		}),
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}
	cfg, ok := testAgentConfig(t, am, "agent-1", "")
	if !ok {
		t.Fatal("spawned agent config is absent")
	}
	got := cfg.ResolvedLLMBackend
	if got != "openai_compatible" {
		t.Fatalf("llm_backend = %q, want openai_compatible", got)
	}
}

func TestResolveAgentModelUsesConfiguredLiveBackendWithoutMock(t *testing.T) {
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
			am := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{LLMBackend: tc.backend})
			cfg := models.AgentConfig{ID: "agent-" + tc.backend, Model: "regular"}
			if err := am.resolveAgentModel(&cfg); err != nil {
				t.Fatalf("resolveAgentModel: %v", err)
			}
			if cfg.ResolvedLLMBackend != tc.backend || cfg.ExecutionMode != runtimeeffects.ExecutionModeLive {
				t.Fatalf("selected backend/mode = %q/%q, want %q/live", cfg.ResolvedLLMBackend, cfg.ExecutionMode, tc.backend)
			}
			if cfg.ResolvedLLMProvider != tc.provider || cfg.ResolvedLLMTransport != tc.transport {
				t.Fatalf("resolved provider/transport = %q/%q, want %q/%q", cfg.ResolvedLLMProvider, cfg.ResolvedLLMTransport, tc.provider, tc.transport)
			}
			if cfg.Mock.Configured() {
				t.Fatalf("live selected descriptor carries mock artifact: %#v", cfg.Mock)
			}
		})
	}
}

func TestResolveAgentModelExactMockOverridesEveryConfiguredLiveBackend(t *testing.T) {
	artifact := capturedMockAlternative()
	for _, backend := range []string{
		llmselection.BackendAnthropic,
		llmselection.BackendClaudeCLI,
		llmselection.BackendOpenAICompatible,
		llmselection.BackendOpenAIResponses,
	} {
		t.Run(backend, func(t *testing.T) {
			am := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{LLMBackend: backend})
			cfg := models.AgentConfig{ID: "agent-" + backend, Model: "regular", Mock: artifact}
			if err := am.resolveAgentModel(&cfg); err != nil {
				t.Fatalf("resolveAgentModel: %v", err)
			}
			if cfg.ResolvedLLMBackend != llmselection.BackendMock || cfg.ExecutionMode != runtimeeffects.ExecutionModeMock {
				t.Fatalf("selected backend/mode = %q/%q, want mock/mock", cfg.ResolvedLLMBackend, cfg.ExecutionMode)
			}
			if cfg.ResolvedLLMProvider != llmselection.ProviderMock || cfg.ResolvedLLMTransport != llmselection.TransportMock {
				t.Fatalf("resolved provider/transport = %q/%q, want mock/in_process", cfg.ResolvedLLMProvider, cfg.ResolvedLLMTransport)
			}
			if !reflect.DeepEqual(cfg.Mock, artifact) {
				t.Fatalf("captured artifact = %#v, want %#v", cfg.Mock, artifact)
			}
		})
	}
}

func TestResolveAgentModelMaterializesSelectionWithoutOptionalModel(t *testing.T) {
	am := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{LLMBackend: llmselection.BackendClaudeCLI})
	cfg := models.AgentConfig{ID: "model-less-mock-agent", Mock: capturedMockAlternative()}
	if err := am.resolveAgentModel(&cfg); err != nil {
		t.Fatalf("resolveAgentModel: %v", err)
	}
	if cfg.ResolvedLLMBackend != llmselection.BackendMock || cfg.ExecutionMode != runtimeeffects.ExecutionModeMock {
		t.Fatalf("selected backend/mode = %q/%q, want mock/mock", cfg.ResolvedLLMBackend, cfg.ExecutionMode)
	}
	if cfg.ResolvedLLMProvider != llmselection.ProviderMock || cfg.ResolvedLLMTransport != llmselection.TransportMock {
		t.Fatalf("resolved provider/transport = %q/%q, want mock/in_process", cfg.ResolvedLLMProvider, cfg.ResolvedLLMTransport)
	}
	if cfg.ResolvedModel != "" {
		t.Fatalf("resolved model = %q, want empty for optional model", cfg.ResolvedModel)
	}
}

func TestResolveAgentModelMockRetainsAndRequiresCapturedArtifact(t *testing.T) {
	am := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{LLMBackend: llmselection.BackendMock})
	artifact := capturedMockAlternative()
	cfg := models.AgentConfig{ID: "mock-agent", Model: "regular", LLMBackend: llmselection.BackendMock, Mock: artifact}
	if err := am.resolveAgentModel(&cfg); err != nil {
		t.Fatalf("resolveAgentModel: %v", err)
	}
	if cfg.ExecutionMode != runtimeeffects.ExecutionModeMock || !reflect.DeepEqual(cfg.Mock, artifact) {
		t.Fatalf("mock selected descriptor = mode %q artifact %#v, want exact captured artifact %#v", cfg.ExecutionMode, cfg.Mock, artifact)
	}

	missing := models.AgentConfig{ID: "mock-agent-missing", Model: "regular", LLMBackend: llmselection.BackendMock}
	if err := am.resolveAgentModel(&missing); err == nil || !strings.Contains(err.Error(), "requires an exact mock performance artifact") {
		t.Fatalf("missing mock artifact error = %v", err)
	}
}

func TestAuthoredMockStaticAndInstantiatedAgentsSpawnPersistRecoverMock(t *testing.T) {
	artifact := capturedMockAlternative()
	staticFlow := runtimecontracts.FlowContractView{
		Path:  "static-support",
		Paths: runtimecontracts.FlowContractPaths{ID: "static-support", Flow: "static-support"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"static-worker": managerTestAgentEntry("static-worker", runtimecontracts.AgentRegistryEntry{ID: "static-worker"}),
		},
	}
	templateFlow := runtimecontracts.FlowContractView{
		Path:  "template-support",
		Paths: runtimecontracts.FlowContractPaths{ID: "template-support", Flow: "template-support"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": managerTestAgentEntry("worker", runtimecontracts.AgentRegistryEntry{ID: "template-worker"}),
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{staticFlow, templateFlow}}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"static-support":   &root.Children[0],
				"template-support": &root.Children[1],
			},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				"static-support/static-worker": {
					Kind: "agent", FlowID: "static-support", LocalID: "static-worker",
					Full: "test://static-support/static-worker",
				},
				"template-support/worker": {
					Kind: "agent", FlowID: "template-support", LocalID: "worker",
					Full: "test://template-support/worker",
				},
			},
		},
	})
	staticCfg, err := buildStaticFlowAgentConfig(source, managerTestFlowAgentNamePlan(t, source, "static-support", "static-worker"), "static-support", "static-support", "static-worker", managerTestAgentEntry("static-worker", runtimecontracts.AgentRegistryEntry{
		ID: "static-worker", Role: "worker", Model: "regular", MemoryPlan: agentmemory.PlatformDefault(), Mock: artifact,
	}), nil)
	if err != nil {
		t.Fatalf("buildStaticFlowAgentConfig: %v", err)
	}
	instantiatedCfg, err := buildFlowAgentConfig(source, managerTestFlowAgentNamePlan(t, source, "template-support", "worker"), "template-support", "inst-1", "entity-1", "template-support/inst-1", "worker", managerTestAgentEntry("worker", runtimecontracts.AgentRegistryEntry{
		ID: "template-worker", Role: "worker", Model: "regular", MemoryPlan: agentmemory.PlatformDefault(), Mock: artifact,
	}), map[string]string{"instance_id": "inst-1"}, nil, nil)
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
	am := newTestAgentManagerWithOptions(t, &recoveryTestBus{}, func(cfg models.AgentConfig) (Agent, error) {
		spawned[cfg.ID] = cfg
		return recoveryTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{LLMBackend: llmselection.BackendAnthropic}, store)
	for _, cfg := range []models.AgentConfig{staticCfg, instantiatedCfg} {
		if err := am.spawnAgentInternal(context.Background(), PersistedAgent{Config: cfg, Status: "active", StartedAt: time.Now().UTC()}, true); err != nil {
			t.Fatalf("spawnAgentInternal(%s): %v", cfg.ID, err)
		}
	}
	assertMockProjection(t, "spawned", artifact, spawned, staticCfg.ID, instantiatedCfg.ID)
	if len(store.records) != 2 {
		t.Fatalf("persisted records = %d, want 2", len(store.records))
	}
	persisted := map[string]models.AgentConfig{}
	for _, rec := range store.records {
		persisted[rec.Config.ID] = rec.Config
	}
	assertMockProjection(t, "persisted", artifact, persisted, staticCfg.ID, instantiatedCfg.ID)

	recovered := map[string]models.AgentConfig{}
	recoveryManager := newTestAgentManagerWithOptions(t, &recoveryTestBus{}, func(cfg models.AgentConfig) (Agent, error) {
		recovered[cfg.ID] = cfg
		return recoveryTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{LLMBackend: llmselection.BackendAnthropic}, store)
	if err := recoveryManager.Recover(managedExecutionTestContext(t, context.Background())); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertMockProjection(t, "recovered", artifact, recovered, staticCfg.ID, instantiatedCfg.ID)
}

func TestAuthoredMockSelectionSurvivesReconfigureRestartAndClone(t *testing.T) {
	artifact := capturedMockAlternative()
	built := map[string]models.AgentConfig{}
	am := newTestAgentManagerWithOptions(t, &recoveryTestBus{}, func(cfg models.AgentConfig) (Agent, error) {
		built[cfg.ID] = cfg
		return recoveryTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{LLMBackend: llmselection.BackendClaudeCLI})
	if err := am.SpawnAgent(managerTestAgentConfig(models.AgentConfig{ID: "mock-lifecycle-agent", Role: "worker", Model: "regular", Mock: artifact})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	base, err := am.ResolveAgentConfig("mock-lifecycle-agent", "")
	if err != nil {
		t.Fatalf("ResolveAgentConfig(spawn): %v", err)
	}
	assertMockProjection(t, "spawn", artifact, map[string]models.AgentConfig{base.ID: base}, base.ID)

	if err := reconfigureAgentThroughLifecycleForTest(t, am, base.ID, "", models.AgentConfig{Tools: []string{"schedule"}}); err != nil {
		t.Fatalf("lifecycle reconfigure: %v", err)
	}
	reconfigured, err := am.ResolveAgentConfig(base.ID, "")
	if err != nil {
		t.Fatalf("ResolveAgentConfig(reconfigure): %v", err)
	}
	assertMockProjection(t, "reconfigure", artifact, map[string]models.AgentConfig{reconfigured.ID: reconfigured}, reconfigured.ID)

	if _, err := am.Restart(testAuthorActivityContext(context.Background()), runtimeagentcontrol.RestartRequest{AgentID: base.ID}); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	restarted, err := am.ResolveAgentConfig(base.ID, "")
	if err != nil {
		t.Fatalf("ResolveAgentConfig(restart): %v", err)
	}
	assertMockProjection(t, "restart", artifact, map[string]models.AgentConfig{restarted.ID: restarted}, restarted.ID)

	if err := am.SpawnEphemeralClone(restarted.Identity, "mock-lifecycle-clone"); err != nil {
		t.Fatalf("SpawnEphemeralClone: %v", err)
	}
	clone, err := am.ResolveAgentConfig("mock-lifecycle-clone", "")
	if err != nil {
		t.Fatalf("ResolveAgentConfig(clone): %v", err)
	}
	assertMockProjection(t, "clone", artifact, map[string]models.AgentConfig{clone.ID: clone}, clone.ID)
	assertMockProjection(t, "constructed", artifact, built, base.ID, clone.ID)
}

func TestAgentRuntimeSetRecoveryRederivesUnpinnedBackendFromCurrentConfiguration(t *testing.T) {
	store := &liveMockAlternativePersistence{}
	initialBuilt := map[string]models.AgentConfig{}
	initial := newRuntimeSetBackedManager(t, llmselection.BackendAnthropic, store, initialBuilt)
	if err := initial.SpawnAgent(managerTestAgentConfig(models.AgentConfig{ID: "backend-change-agent", Role: "worker"})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if len(store.records) != 1 {
		t.Fatalf("persisted records = %d, want 1", len(store.records))
	}
	persisted := store.records[0].Config
	if persisted.LLMBackend != "" || persisted.ResolvedLLMBackend != llmselection.BackendAnthropic {
		t.Fatalf("persisted authored/resolved backend = %q/%q, want blank/%q", persisted.LLMBackend, persisted.ResolvedLLMBackend, llmselection.BackendAnthropic)
	}

	recoveredBuilt := map[string]models.AgentConfig{}
	recovered := newRuntimeSetBackedManager(t, llmselection.BackendOpenAIResponses, store, recoveredBuilt)
	if err := recovered.Recover(managedExecutionTestContext(t, context.Background())); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	got, err := recovered.ResolveAgentConfig("backend-change-agent", "")
	if err != nil {
		t.Fatalf("ResolveAgentConfig: %v", err)
	}
	if got.LLMBackend != "" || got.ResolvedLLMBackend != llmselection.BackendOpenAIResponses {
		t.Fatalf("recovered authored/resolved backend = %q/%q, want blank/%q", got.LLMBackend, got.ResolvedLLMBackend, llmselection.BackendOpenAIResponses)
	}
	if built := recoveredBuilt[got.ID]; built.LLMBackend != "" || built.ResolvedLLMBackend != llmselection.BackendOpenAIResponses {
		t.Fatalf("constructed recovered authored/resolved backend = %q/%q", built.LLMBackend, built.ResolvedLLMBackend)
	}
}

func TestAgentRuntimeSetReconfigureHonorsOrRejectsAuthoredBackendPatch(t *testing.T) {
	store := &liveMockAlternativePersistence{}
	built := map[string]models.AgentConfig{}
	manager := newRuntimeSetBackedManager(t, llmselection.BackendAnthropic, store, built)
	if err := manager.SpawnAgent(managerTestAgentConfig(models.AgentConfig{ID: "backend-patch-agent", Role: "worker"})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err := reconfigureAgentThroughLifecycleForTest(t, manager, "backend-patch-agent", "", models.AgentConfig{LLMBackend: llmselection.BackendAnthropic})
	if err != nil {
		t.Fatalf("lifecycle reconfigure (matching backend): %v", err)
	}
	accepted, err := manager.ResolveAgentConfig("backend-patch-agent", "")
	if err != nil {
		t.Fatalf("ResolveAgentConfig(accepted): %v", err)
	}
	if accepted.LLMBackend != llmselection.BackendAnthropic || accepted.ResolvedLLMBackend != llmselection.BackendAnthropic {
		t.Fatalf("accepted authored/resolved backend = %q/%q", accepted.LLMBackend, accepted.ResolvedLLMBackend)
	}
	persistedCount := len(store.records)
	if persistedCount < 2 {
		t.Fatalf("persisted records after accepted patch = %d, want at least 2", persistedCount)
	}
	lastPersisted := store.records[persistedCount-1].Config
	if lastPersisted.LLMBackend != llmselection.BackendAnthropic || lastPersisted.ResolvedLLMBackend != llmselection.BackendAnthropic {
		t.Fatalf("persisted authored/resolved backend = %q/%q", lastPersisted.LLMBackend, lastPersisted.ResolvedLLMBackend)
	}

	err = reconfigureAgentThroughLifecycleForTest(t, manager, "backend-patch-agent", "", models.AgentConfig{LLMBackend: llmselection.BackendOpenAIResponses})
	if err == nil || !strings.Contains(err.Error(), "conflicts with configured runtime backend") {
		t.Fatalf("lifecycle reconfigure (conflicting backend) error = %v", err)
	}
	if len(store.records) != persistedCount {
		t.Fatalf("conflicting patch persisted %d new record(s)", len(store.records)-persistedCount)
	}
	current, resolveErr := manager.ResolveAgentConfig("backend-patch-agent", "")
	if resolveErr != nil {
		t.Fatalf("ResolveAgentConfig: %v", resolveErr)
	}
	if !reflect.DeepEqual(current, accepted) {
		t.Fatalf("current config changed after rejected patch\n got: %#v\nwant: %#v", current, accepted)
	}
}

func newRuntimeSetBackedManager(t *testing.T, backend string, store ManagerPersistence, built map[string]models.AgentConfig) *AgentManager {
	t.Helper()
	profile, err := llmselection.ResolveActiveBackend(backend)
	if err != nil {
		t.Fatalf("ResolveActiveBackend(%s): %v", backend, err)
	}
	runtimes, err := runtimellm.NewAgentRuntimeSet(profile, runtimellm.RuntimeFactory{}, runtimellm.NoopRuntime{})
	if err != nil {
		t.Fatalf("NewAgentRuntimeSet(%s): %v", backend, err)
	}
	return newTestAgentManagerWithOptions(t, &recoveryTestBus{}, func(cfg models.AgentConfig) (Agent, error) {
		resolved, resolveErr := runtimes.ResolveAgentRuntime(cfg)
		if resolveErr != nil {
			return nil, resolveErr
		}
		built[cfg.ID] = resolved.Actor
		return recoveryTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{LLMBackend: backend}, store)
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

func assertMockProjection(t *testing.T, phase string, artifact mockperformance.Performance, configs map[string]models.AgentConfig, ids ...string) {
	t.Helper()
	for _, id := range ids {
		cfg, ok := configs[id]
		if !ok {
			t.Fatalf("%s config %q missing", phase, id)
		}
		if cfg.ResolvedLLMBackend != llmselection.BackendMock || cfg.ExecutionMode != runtimeeffects.ExecutionModeMock || !reflect.DeepEqual(cfg.Mock, artifact) {
			t.Fatalf("%s config %q = backend %q mode %q mock %#v, want mock/mock with exact artifact %#v", phase, id, cfg.ResolvedLLMBackend, cfg.ExecutionMode, cfg.Mock, artifact)
		}
	}
}
