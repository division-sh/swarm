package manager

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type reconfigureTestAgent struct{ id string }

func (a reconfigureTestAgent) ID() string                      { return a.id }
func (reconfigureTestAgent) Type() string                      { return "generic" }
func (reconfigureTestAgent) Subscriptions() []events.EventType { return nil }
func (reconfigureTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func reconfigureMemoryIdentity(t testing.TB, am *AgentManager, agentID, flowInstance string) agentmemory.Identity {
	t.Helper()
	return agentmemory.Identity{
		RunID: "run-reconfigure",
		Agent: testAgentIdentity(t, am, agentID, flowInstance),
	}
}

func acquireReconfigureMemory(t *testing.T, am *AgentManager, registry *sessions.InMemoryRegistry, cfg models.AgentConfig) *sessions.Lease {
	t.Helper()
	ctx := effects.WithDifferentOwner(models.WithActor(testAuthorActivityContext(context.Background()), cfg), effects.OwnerBuildTestInfrastructure)
	lease, err := registry.Acquire(ctx, reconfigureMemoryIdentity(t, am, cfg.ID, cfg.CanonicalFlowPath()), "reconfigure-test")
	if err != nil {
		t.Fatalf("Acquire memory: %v", err)
	}
	return lease
}

func TestReconfigureAgent_PersistenceFailureLeavesPriorProjection(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	am := newTestAgentManagerWithOptions(t, nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{LifecycleStore: probe})
	cfg := managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "worker",
		FlowPath:      "review/inst-1",
		Tools:         []string{"tool-old"},
	})
	if err := spawnManagerTestAgent(am, cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	beforeGeneration := lifecycleGenerationForTest(t, am, cfg.ID)
	persistenceErr := errors.New("injected reconfigure persistence failure")
	probe.mu.Lock()
	probe.failNext = persistenceErr
	probe.mu.Unlock()
	err := reconfigureAgentThroughLifecycleForTest(t, am, cfg.ID, cfg.FlowPath, models.AgentConfig{
		Tools: []string{"tool-new"},
	})
	if !errors.Is(err, persistenceErr) {
		t.Fatalf("lifecycle reconfigure error = %v, want %v", err, persistenceErr)
	}
	visible, err := am.ResolveAgentConfig(cfg.ID, cfg.FlowPath)
	if err != nil {
		t.Fatalf("ResolveAgentConfig after persistence failure: %v", err)
	}
	if !reflect.DeepEqual(visible.Tools, cfg.Tools) {
		t.Fatalf("visible tools after persistence failure = %v, want %v", visible.Tools, cfg.Tools)
	}
	if got := lifecycleGenerationForTest(t, am, cfg.ID); got != beforeGeneration {
		t.Fatalf("generation after persistence failure = %d, want %d", got, beforeGeneration)
	}
}

func TestReconfigureAgent_SameCurrentPreservesExecutionIdentityWithoutFactoryInvocation(t *testing.T) {
	builds := 0
	registry := sessions.NewInMemoryRegistry(0)
	am := newTestAgentManagerWithOptions(t, newProjectionTestBus(), func(cfg models.AgentConfig) (Agent, error) {
		builds++
		return &reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "same-current-agent", Tools: []string{"tool-a"},
		Memory: agentmemory.Authored(true), FlowPath: "same-current/instance",
	})
	if err := spawnManagerTestAgent(am, cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))
	lease := acquireReconfigureMemory(t, am, registry, cfg)
	beforeExecution, ok := testExecutionSnapshot(t, am, cfg.ID, cfg.FlowPath)
	if !ok {
		t.Fatal("spawned execution is absent")
	}
	beforeSession, ok := registry.Snapshot(reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath))
	if !ok {
		t.Fatal("memory session is absent before reconfigure")
	}
	beforeGeneration := lifecycleGenerationForTest(t, am, cfg.ID)

	if err := reconfigureAgentThroughLifecycleForTest(t, am, cfg.ID, cfg.FlowPath, models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-a"}}); err != nil {
		t.Fatalf("ReconfigureAgent(same current): %v", err)
	}

	if builds != 1 {
		t.Fatalf("factory builds = %d, want 1", builds)
	}
	afterExecution, ok := testExecutionSnapshot(t, am, cfg.ID, cfg.FlowPath)
	if !ok || afterExecution.Agent != beforeExecution.Agent || !reflect.DeepEqual(afterExecution.Config, beforeExecution.Config) {
		t.Fatalf("same-current execution changed: before=%#v after=%#v ok=%v", beforeExecution, afterExecution, ok)
	}
	if got := lifecycleGenerationForTest(t, am, cfg.ID); got != beforeGeneration {
		t.Fatalf("generation = %d, want unchanged %d", got, beforeGeneration)
	}
	afterSession, ok := registry.Snapshot(reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath))
	if !ok || !reflect.DeepEqual(afterSession, beforeSession) || afterSession.SessionID != lease.SessionID {
		t.Fatalf("same-current memory changed: before=%#v after=%#v ok=%v", beforeSession, afterSession, ok)
	}
}

func TestReconfigureAgent_MemoryEnabledConfigChangeRotatesExactIdentity(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := newTestAgentManagerWithOptions(t, nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := managerTestAgentConfig(models.AgentConfig{ExecutionMode: "live", ID: "memory-agent", Memory: agentmemory.Authored(true), FlowPath: "review/inst-1"})
	if err := spawnManagerTestAgent(am, cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	lease := acquireReconfigureMemory(t, am, registry, cfg)

	if err := reconfigureAgentThroughLifecycleForTest(t, am, cfg.ID, cfg.FlowPath, models.AgentConfig{ExecutionMode: "live", Tools: []string{"schedule"}, Permissions: []string{"schedule"}}); err != nil {
		t.Fatalf("ReconfigureAgent: %v", err)
	}
	rec, ok := registry.Snapshot(reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath))
	if !ok || rec.SessionID == lease.SessionID {
		t.Fatalf("memory session = %#v ok=%v, want rotated successor", rec, ok)
	}
	if rec.Identity != reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath) {
		t.Fatalf("memory identity = %+v, want exact run/agent/flow identity", rec.Identity)
	}
}

func TestReconfigureAgent_ExplicitFalseTerminatesReusableMemory(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := newTestAgentManagerWithOptions(t, nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := managerTestAgentConfig(models.AgentConfig{ExecutionMode: "live", ID: "disable-memory-agent", Memory: agentmemory.Authored(true), FlowPath: "support/inst-1"})
	if err := spawnManagerTestAgent(am, cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	lease := acquireReconfigureMemory(t, am, registry, cfg)

	if err := reconfigureAgentThroughLifecycleForTest(t, am, cfg.ID, cfg.FlowPath, models.AgentConfig{ExecutionMode: "live", Memory: agentmemory.Authored(false)}); err != nil {
		t.Fatalf("ReconfigureAgent(memory false): %v", err)
	}
	if _, ok := registry.Snapshot(reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath)); ok {
		t.Fatal("reusable memory survived explicit false")
	}
	history := registry.History(reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath))
	if len(history) != 1 || history[0].SessionID != lease.SessionID || history[0].Status != "terminated" || history[0].SuccessorSessionID != "" {
		t.Fatalf("memory history = %#v, want exact terminated predecessor", history)
	}
	got, _ := testAgentConfig(t, am, cfg.ID, cfg.FlowPath)
	if got.Memory != agentmemory.Authored(false) {
		t.Fatalf("memory plan = %+v, want authored false", got.Memory)
	}
}

func TestReconfigureAgent_ExplicitTrueStartsFreshAndOmissionRetains(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := newTestAgentManagerWithOptions(t, nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := managerTestAgentConfig(models.AgentConfig{ExecutionMode: "live", ID: "enable-memory-agent", Memory: agentmemory.Authored(false), FlowPath: "support/inst-1"})
	if err := spawnManagerTestAgent(am, cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if err := reconfigureAgentThroughLifecycleForTest(t, am, cfg.ID, cfg.FlowPath, models.AgentConfig{ExecutionMode: "live", Memory: agentmemory.Authored(true)}); err != nil {
		t.Fatalf("ReconfigureAgent(memory true): %v", err)
	}
	if _, ok := registry.Snapshot(reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath)); ok || len(registry.History(reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath))) != 0 {
		t.Fatal("enabling memory revived or synthesized prior state")
	}
	lease := acquireReconfigureMemory(t, am, registry, models.AgentConfig{ExecutionMode: "live", ID: cfg.ID, FlowPath: cfg.FlowPath})
	if err := reconfigureAgentThroughLifecycleForTest(t, am, cfg.ID, cfg.FlowPath, models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-a"}}); err != nil {
		t.Fatalf("ReconfigureAgent(omitted memory): %v", err)
	}
	got, _ := testAgentConfig(t, am, cfg.ID, cfg.FlowPath)
	if got.Memory != agentmemory.Authored(true) {
		t.Fatalf("omitted memory changed plan to %+v", got.Memory)
	}
	current, ok := registry.Snapshot(reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath))
	if !ok || current.SessionID == lease.SessionID {
		t.Fatalf("retained enabled memory did not rotate on config change: %#v ok=%v", current, ok)
	}
}

func lifecycleGenerationForTest(t *testing.T, am *AgentManager, agentID string) uint64 {
	t.Helper()
	cell, _ := testLifecycleCell(t, am.lifecycle, agentID, "")
	if cell == nil {
		t.Fatalf("lifecycle cell %q is absent", agentID)
	}
	return cell.generation
}
