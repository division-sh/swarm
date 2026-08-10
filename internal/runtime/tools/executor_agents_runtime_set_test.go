package tools_test

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeeventreceiver "github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

type runtimeSetHireAgent struct {
	id string
}

func (a runtimeSetHireAgent) ID() string                      { return a.id }
func (runtimeSetHireAgent) Type() string                      { return "test" }
func (runtimeSetHireAgent) Subscriptions() []events.EventType { return nil }
func (runtimeSetHireAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

type runtimeSetHireLiveRuntime struct {
	runtimellm.NoopRuntime
}

func (runtimeSetHireLiveRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.ClaudeCLIProviderContract()
}

func (runtimeSetHireLiveRuntime) ProbeStartupVisibleToolSurface(context.Context, models.AgentConfig, string, []runtimellm.ToolDefinition) (*runtimellm.Response, error) {
	return &runtimellm.Response{}, nil
}

func TestExecAgentHireResolvesMockBeforeNativeToolAdmissionWithProductionRuntimeSet(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager": {ID: "manager", Role: "manager"},
			"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager-1"},
		},
	})
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	harness := effecttest.New()
	runtimes, err := runtimellm.NewAgentRuntimeSet(profile, runtimellm.RuntimeFactory{
		Cfg:                  &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendClaudeCLI}},
		CompletionController: runtimeeffects.NewCompletionController(harness, harness, harness, harness),
	}, runtimeSetHireLiveRuntime{})
	if err != nil {
		t.Fatalf("NewAgentRuntimeSet: %v", err)
	}

	constructed := map[string]models.AgentConfig{}
	var executor *runtimetools.Executor
	manager := runtimemanager.NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (runtimemanager.Agent, error) {
		resolved, resolveErr := runtimes.ResolveAgentRuntime(cfg)
		if resolveErr != nil {
			return nil, resolveErr
		}
		constructed[cfg.ID] = resolved.Actor
		return runtimeSetHireAgent{id: cfg.ID}, nil
	}, runtimemanager.AgentManagerOptions{
		LLMBackend:        llmselection.BackendClaudeCLI,
		ReceiverExecution: runtimeeventreceiver.NormalExecution(),
		SemanticSource:    source,
		NativeToolAdmissionValidator: func(ctx context.Context, cfg models.AgentConfig) error {
			return executor.ValidateNativeToolAdmission(ctx, cfg)
		},
	})
	provider := runtimeauthority.NewSourceProvider(source)
	executor = runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: provider,
		WorkflowSource:    source,
		ModelRuntimes:     runtimes,
	})

	managerIdentity := runtimeagentidentitytest.Runtime(t, "manager-1", "runtime-set-hire", "review", "inst-1", "review/inst-1")
	managerCfg := models.AgentConfig{
		ID:          "manager-1",
		Identity:    managerIdentity,
		Role:        "manager",
		FlowID:      "review",
		FlowPath:    "review/inst-1",
		Permissions: []string{"agent_hire"},
	}
	if err := manager.SpawnAgent(managerCfg); err != nil {
		t.Fatalf("SpawnAgent(manager): %v", err)
	}
	managerCfg, err = manager.ResolveAgentConfig(managerCfg.ID, managerCfg.FlowPath)
	if err != nil {
		t.Fatalf("ResolveAgentConfig(manager): %v", err)
	}
	managerCfg.NativeTools.FileIO = true

	sourceBytes := []byte("def handle(input):\n    return {'text': 'mock hire'}\n")
	if _, err := executor.ExecAgentHireDirect(managerCfg, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "manager-1",
			"native_tools": map[string]any{
				"file_io": true,
			},
			"mock": map[string]any{
				"kind":        "python",
				"module":      "mocks/worker.py",
				"source":      base64.StdEncoding.EncodeToString(sourceBytes),
				"digest":      "sha256:runtime-set-hire",
				"source_path": "mocks/worker.py",
			},
		},
	}); err != nil {
		t.Fatalf("ExecAgentHireDirect: %v", err)
	}

	worker, err := manager.ResolveAgentConfig("worker-1", "review/inst-1")
	if err != nil {
		t.Fatalf("ResolveAgentConfig(worker): %v", err)
	}
	if worker.ResolvedLLMBackend != llmselection.BackendMock || worker.ExecutionMode != runtimeeffects.ExecutionModeMock {
		t.Fatalf("worker selection = %q/%q, want mock/mock", worker.ResolvedLLMBackend, worker.ExecutionMode)
	}
	if !worker.NativeTools.FileIO {
		t.Fatal("worker native_tools.file_io was not retained")
	}
	if got := constructed[worker.ID]; got.ResolvedLLMBackend != llmselection.BackendMock {
		t.Fatalf("constructed worker backend = %q, want mock", got.ResolvedLLMBackend)
	}
}
