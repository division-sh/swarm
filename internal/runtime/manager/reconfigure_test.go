package manager

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
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
	return testAgentIdentity(t, am, agentID, flowInstance)
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
		Identity:      managerRuntimeAgentIdentityForFlowPath("worker", "review/inst-1"),
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
	visible, err := am.ResolveAgentConfig(cfg.Identity.RunID, cfg.ID, cfg.FlowPath)
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
		Memory: agentmemory.Plan{Enabled: true}, FlowPath: "same-current/instance",
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

func TestReconfigureAgent_TopologyOnlyRebindPreservesMemory(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := newTestAgentManagerWithOptions(t, newProjectionTestBus(), func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})
	cfg := managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live", ID: "topology-memory-agent",
		Memory: agentmemory.Plan{Enabled: true}, FlowPath: "review/instance-1",
	})
	if err := spawnManagerTestAgent(am, cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	am.Run(managedExecutionTestContext(t, runCtx))
	lease := acquireReconfigureMemory(t, am, registry, cfg)
	currentConfig, err := am.ResolveAgentConfig(managerIdentityTestRunID, cfg.ID, cfg.FlowPath)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := currentConfig.ConcreteIdentity()
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := runtimeagenttopology.FlowReadinessAdmission(identity.RunID, cfg.CanonicalFlowPath(), "plan-v1")
	if err != nil {
		t.Fatal(err)
	}
	preparation, err = preparation.WithFlowPreparationAttempt(1)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := preparation.WithFlowActivationAttempt("1")
	if err != nil {
		t.Fatal(err)
	}
	for _, topology := range []runtimeagenttopology.Admission{preparation, attempt} {
		if err := am.reconfigureAgentIdentityExactWithTopology(am.runtimeContext(), am.semanticSource, identity, currentConfig, &topology); err != nil {
			t.Fatalf("rebind topology: %v", err)
		}
		current, ok := registry.Snapshot(reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath))
		if !ok || current.SessionID != lease.SessionID {
			t.Fatalf("topology-only rebind changed memory: current=%#v present=%t want session=%s", current, ok, lease.SessionID)
		}
	}
}

type ReconfigureMemoryNativeTestStore interface {
	AgentLifecycleStateReader
	sessions.Registry
	LoadActiveConversation(context.Context, agentmemory.Identity) (llm.ConversationRecord, bool, error)
}

func ReconfigureMemoryNativeFixture(t testing.TB, enabled bool) PersistedAgent {
	t.Helper()
	rec := lifecycleTestPersistedAgent(t)
	rec.Config.FlowID, rec.Config.FlowPath = "review", "review/inst-1"
	rec.Config.Identity = managerRuntimeAgentIdentityForFlowPath(rec.Config.ID, rec.Config.FlowPath)
	rec.Config.Memory.Enabled = enabled
	rec.Config.LLMBackend = "anthropic"
	rec.Config.Config = []byte(`{}`)
	if err := resolveAgentModel(&rec.Config, executionposture.Live, "anthropic", llmselection.ModelAliases{}, true); err != nil {
		t.Fatal(err)
	}
	rec.Topology = runtimeagenttopology.Admission{}
	return rec
}

func newReconfigureMemoryNativeManager(t *testing.T, persistence AgentLifecyclePersistence, selected ReconfigureMemoryNativeTestStore, rec PersistedAgent) *AgentManager {
	t.Helper()
	am := newTestAgentManagerWithOptions(t, newProjectionTestBus(), func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{LLMBackend: "anthropic", LifecycleStore: persistence, Sessions: selected, PersistenceRoles: PersistenceRoles{LifecycleState: selected}})
	ctx := testAuthorActivityContext(context.Background())
	if err := am.lifecycle.registerExecution(ctx, rec, false, reconfigureTestAgent{id: rec.Config.ID}, testManagerSubscriptionAdmission(t, rec.Config)); err != nil {
		t.Fatal(err)
	}
	if err := am.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatal(err)
	}
	return am
}

func acquireNativeReconfigureMemory(t *testing.T, am *AgentManager, selected ReconfigureMemoryNativeTestStore, cfg models.AgentConfig) *sessions.Lease {
	t.Helper()
	execution, err := am.lifecycle.acquireExecutionIdentity(testAuthorActivityContext(context.Background()), cfg.Identity, "native-memory-reconfigure-proof", true)
	if err != nil {
		t.Fatal(err)
	}
	defer execution.Release()
	lease, err := selected.Acquire(models.WithActor(execution.Context, cfg), agentmemory.Identity(cfg.Identity), "reconfigure-test")
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

type ReconfigureMemoryNativeAdmission func(models.AgentConfig) (AgentLifecyclePersistence, runtimeagenttopology.Admission)

func reconfigureNativeMemoryCandidate(t *testing.T, am *AgentManager, candidate models.AgentConfig, admit ReconfigureMemoryNativeAdmission) error {
	t.Helper()
	if err := am.resolveAgentModel(&candidate); err != nil {
		return err
	}
	owner, topology := admit(candidate)
	am.lifecycle.replacePersistence(owner)
	return am.reconfigureAgentIdentityExactWithTopology(am.runtimeContext(), am.semanticSource, candidate.Identity, candidate, &topology)
}

func ProveReconfigureMemoryEnabledConfigChange(t *testing.T, persistence AgentLifecyclePersistence, selected ReconfigureMemoryNativeTestStore, record PersistedAgent, history func(agentmemory.Identity) []sessions.Record, admit ReconfigureMemoryNativeAdmission) {
	am := newReconfigureMemoryNativeManager(t, persistence, selected, record)
	cfg := record.Config
	lease := acquireNativeReconfigureMemory(t, am, selected, cfg)

	candidate := cfg
	candidate.Tools, candidate.Permissions = []string{"schedule"}, []string{"schedule"}
	if err := reconfigureNativeMemoryCandidate(t, am, candidate, admit); err != nil {
		t.Fatalf("ReconfigureAgent: %v", err)
	}
	rec, ok, err := selected.LoadActiveConversation(context.Background(), agentmemory.Identity(cfg.Identity))
	if err != nil || !ok || rec.SessionID == lease.SessionID {
		t.Fatalf("memory session = %#v ok=%v err=%v, want rotated successor", rec, ok, err)
	}
	if rec.Identity != reconfigureMemoryIdentity(t, am, cfg.ID, cfg.FlowPath) {
		t.Fatalf("memory identity = %+v, want exact run/agent/flow identity", rec.Identity)
	}
	rows := history(agentmemory.Identity(cfg.Identity))
	if len(rows) != 2 {
		t.Fatalf("native rotation history = %#v", rows)
	}
	for _, prior := range rows {
		if prior.SessionID == lease.SessionID && (prior.Status != "terminated" || prior.SuccessorSessionID != rec.SessionID) {
			t.Fatalf("native predecessor lineage = %#v", prior)
		}
	}
}

func ProveReconfigureExplicitFalse(t *testing.T, persistence AgentLifecyclePersistence, selected ReconfigureMemoryNativeTestStore, record PersistedAgent, history func(agentmemory.Identity) []sessions.Record, admit ReconfigureMemoryNativeAdmission) {
	am := newReconfigureMemoryNativeManager(t, persistence, selected, record)
	cfg := record.Config
	lease := acquireNativeReconfigureMemory(t, am, selected, cfg)

	candidate := cfg
	candidate.Memory.Enabled = false
	if err := reconfigureNativeMemoryCandidate(t, am, candidate, admit); err != nil {
		t.Fatalf("ReconfigureAgent(memory false): %v", err)
	}
	if _, ok, err := selected.LoadActiveConversation(context.Background(), agentmemory.Identity(cfg.Identity)); err != nil || ok {
		t.Fatal("reusable memory survived explicit false")
	}
	rows := history(agentmemory.Identity(cfg.Identity))
	if len(rows) != 1 || rows[0].SessionID != lease.SessionID || rows[0].Status != "terminated" || rows[0].SuccessorSessionID != "" {
		t.Fatalf("memory history = %#v, want exact terminated predecessor", rows)
	}
	got, _ := testAgentConfig(t, am, cfg.ID, cfg.FlowPath)
	if got.Memory != (agentmemory.Plan{Enabled: false}) {
		t.Fatalf("memory plan = %+v, want authored false", got.Memory)
	}
}

func ProveReconfigureExplicitTrue(t *testing.T, persistence AgentLifecyclePersistence, selected ReconfigureMemoryNativeTestStore, record PersistedAgent, history func(agentmemory.Identity) []sessions.Record, admit ReconfigureMemoryNativeAdmission) {
	am := newReconfigureMemoryNativeManager(t, persistence, selected, record)
	cfg := record.Config
	candidate := cfg
	candidate.Memory.Enabled = true
	if err := reconfigureNativeMemoryCandidate(t, am, candidate, admit); err != nil {
		t.Fatalf("ReconfigureAgent(memory true): %v", err)
	}
	if _, ok, err := selected.LoadActiveConversation(context.Background(), agentmemory.Identity(cfg.Identity)); err != nil || ok || len(history(agentmemory.Identity(cfg.Identity))) != 0 {
		t.Fatal("enabling memory revived or synthesized prior state")
	}
	currentConfig, err := am.ResolveAgentConfig(cfg.Identity.RunID, cfg.ID, cfg.FlowPath)
	if err != nil {
		t.Fatal(err)
	}
	lease := acquireNativeReconfigureMemory(t, am, selected, currentConfig)
	candidate = currentConfig
	candidate.Tools = []string{"tool-a"}
	if err := reconfigureNativeMemoryCandidate(t, am, candidate, admit); err != nil {
		t.Fatalf("ReconfigureAgent(omitted memory): %v", err)
	}
	got, _ := testAgentConfig(t, am, cfg.ID, cfg.FlowPath)
	if got.Memory != (agentmemory.Plan{Enabled: true}) {
		t.Fatalf("omitted memory changed plan to %+v", got.Memory)
	}
	current, ok, err := selected.LoadActiveConversation(context.Background(), agentmemory.Identity(cfg.Identity))
	if err != nil || !ok || current.SessionID == lease.SessionID {
		t.Fatalf("retained enabled memory did not rotate on config change: %#v ok=%v err=%v", current, ok, err)
	}
}

func lifecycleGenerationForTest(t *testing.T, am *AgentManager, agentID string) uint64 {
	t.Helper()
	cell, _ := testLifecycleCell(t, am.lifecycle, agentID, "")
	if cell == nil {
		t.Fatalf("lifecycle cell %q is absent", agentID)
	}
	return cell.view.Generation
}
