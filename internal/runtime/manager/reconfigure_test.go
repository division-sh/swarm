package manager

import (
	"context"
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

func reconfigureMemoryIdentity(agentID, flowInstance string) agentmemory.Identity {
	return agentmemory.Identity{RunID: "run-reconfigure", AgentID: agentID, FlowInstance: flowInstance}
}

func acquireReconfigureMemory(t *testing.T, registry *sessions.InMemoryRegistry, cfg models.AgentConfig) *sessions.Lease {
	t.Helper()
	ctx := effects.WithDifferentOwner(models.WithActor(context.Background(), cfg), effects.OwnerBuildTestInfrastructure)
	lease, err := registry.Acquire(ctx, reconfigureMemoryIdentity(cfg.ID, cfg.CanonicalFlowPath()), "reconfigure-test")
	if err != nil {
		t.Fatalf("Acquire memory: %v", err)
	}
	return lease
}

func TestReconfigureAgent_SameCurrentPreservesExecutionIdentityWithoutFactoryInvocation(t *testing.T) {
	builds := 0
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(newProjectionTestBus(), func(cfg models.AgentConfig) (Agent, error) {
		builds++
		return &reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "same-current-agent", Tools: []string{"tool-a"},
		Memory: agentmemory.Authored(true), FlowPath: "same-current/instance",
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))
	lease := acquireReconfigureMemory(t, registry, cfg)
	beforeExecution, ok := am.lifecycle.executionSnapshot(cfg.ID)
	if !ok {
		t.Fatal("spawned execution is absent")
	}
	beforeSession, ok := registry.Snapshot(cfg.ID)
	if !ok {
		t.Fatal("memory session is absent before reconfigure")
	}
	beforeGeneration := lifecycleGenerationForTest(t, am, cfg.ID)

	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-a"}}); err != nil {
		t.Fatalf("ReconfigureAgent(same current): %v", err)
	}

	if builds != 1 {
		t.Fatalf("factory builds = %d, want 1", builds)
	}
	afterExecution, ok := am.lifecycle.executionSnapshot(cfg.ID)
	if !ok || afterExecution.Agent != beforeExecution.Agent || !reflect.DeepEqual(afterExecution.Config, beforeExecution.Config) {
		t.Fatalf("same-current execution changed: before=%#v after=%#v ok=%v", beforeExecution, afterExecution, ok)
	}
	if got := lifecycleGenerationForTest(t, am, cfg.ID); got != beforeGeneration {
		t.Fatalf("generation = %d, want unchanged %d", got, beforeGeneration)
	}
	afterSession, ok := registry.Snapshot(cfg.ID)
	if !ok || !reflect.DeepEqual(afterSession, beforeSession) || afterSession.SessionID != lease.SessionID {
		t.Fatalf("same-current memory changed: before=%#v after=%#v ok=%v", beforeSession, afterSession, ok)
	}
}

func TestReconfigureAgent_MemoryEnabledConfigChangeRotatesExactIdentity(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := models.AgentConfig{ExecutionMode: "live", ID: "memory-agent", Memory: agentmemory.Authored(true), FlowPath: "review/inst-1"}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	lease := acquireReconfigureMemory(t, registry, cfg)

	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{ExecutionMode: "live", Tools: []string{"agent_message"}}); err != nil {
		t.Fatalf("ReconfigureAgent: %v", err)
	}
	rec, ok := registry.Snapshot(cfg.ID)
	if !ok || rec.SessionID == lease.SessionID {
		t.Fatalf("memory session = %#v ok=%v, want rotated successor", rec, ok)
	}
	if rec.Identity != reconfigureMemoryIdentity(cfg.ID, cfg.FlowPath) {
		t.Fatalf("memory identity = %+v, want exact run/agent/flow identity", rec.Identity)
	}
}

func TestReconfigureAgent_ExplicitFalseTerminatesReusableMemory(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := models.AgentConfig{ExecutionMode: "live", ID: "disable-memory-agent", Memory: agentmemory.Authored(true), FlowPath: "support/inst-1"}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	lease := acquireReconfigureMemory(t, registry, cfg)

	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{ExecutionMode: "live", Memory: agentmemory.Authored(false)}); err != nil {
		t.Fatalf("ReconfigureAgent(memory false): %v", err)
	}
	if _, ok := registry.Snapshot(cfg.ID); ok {
		t.Fatal("reusable memory survived explicit false")
	}
	history := registry.History(cfg.ID)
	if len(history) != 1 || history[0].SessionID != lease.SessionID || history[0].Status != "terminated" || history[0].SuccessorSessionID != "" {
		t.Fatalf("memory history = %#v, want exact terminated predecessor", history)
	}
	got, _ := am.GetAgentConfig(cfg.ID)
	if got.Memory != agentmemory.Authored(false) {
		t.Fatalf("memory plan = %+v, want authored false", got.Memory)
	}
}

func TestReconfigureAgent_ExplicitTrueStartsFreshAndOmissionRetains(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := models.AgentConfig{ExecutionMode: "live", ID: "enable-memory-agent", Memory: agentmemory.Authored(false), FlowPath: "support/inst-1"}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{ExecutionMode: "live", Memory: agentmemory.Authored(true)}); err != nil {
		t.Fatalf("ReconfigureAgent(memory true): %v", err)
	}
	if _, ok := registry.Snapshot(cfg.ID); ok || len(registry.History(cfg.ID)) != 0 {
		t.Fatal("enabling memory revived or synthesized prior state")
	}
	lease := acquireReconfigureMemory(t, registry, models.AgentConfig{ExecutionMode: "live", ID: cfg.ID, FlowPath: cfg.FlowPath})
	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{ExecutionMode: "live", Tools: []string{"tool-a"}}); err != nil {
		t.Fatalf("ReconfigureAgent(omitted memory): %v", err)
	}
	got, _ := am.GetAgentConfig(cfg.ID)
	if got.Memory != agentmemory.Authored(true) {
		t.Fatalf("omitted memory changed plan to %+v", got.Memory)
	}
	current, ok := registry.Snapshot(cfg.ID)
	if !ok || current.SessionID == lease.SessionID {
		t.Fatalf("retained enabled memory did not rotate on config change: %#v ok=%v", current, ok)
	}
}

func lifecycleGenerationForTest(t *testing.T, am *AgentManager, agentID string) uint64 {
	t.Helper()
	am.lifecycle.mu.Lock()
	defer am.lifecycle.mu.Unlock()
	cell := am.lifecycle.cells[agentID]
	if cell == nil {
		t.Fatalf("lifecycle cell %q is absent", agentID)
	}
	return cell.generation
}
