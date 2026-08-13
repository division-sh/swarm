package manager

import (
	"context"
	"testing"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

const removedManagerTestTopologyBundleHash = "bundle-v1:sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

func TestPreparedStaticTopologySourceSetRebindPreservesExecutionLifecycle(t *testing.T) {
	source := loadPackageBackedStaticAgentSource(t)
	oldStore := newLifecyclePersistenceProbe()
	manager := newTestAgentManager(t, &recoveryTestBus{}, nil)
	manager.semanticSource = source

	records, err := manager.resolvedStaticTopologyRecords(source)
	if err != nil {
		t.Fatalf("resolve static topology records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("static topology records = %d, want 1", len(records))
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash, BundleSource: "ephemeral"}
	removed := runtimeagenttopology.SourceCoordinate{BundleHash: removedManagerTestTopologyBundleHash, BundleSource: "persisted"}
	desired, err := desiredAgentsFromRecords(records, coordinate)
	if err != nil {
		t.Fatalf("compile desired static agents: %v", err)
	}
	predecessorPlan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate, removed}, desired)
	if err != nil {
		t.Fatalf("construct predecessor source set: %v", err)
	}
	successorPlan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatalf("construct successor source set: %v", err)
	}
	predecessorAdmission, err := runtimeagenttopology.StaticAdmission(
		predecessorPlan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatalf("construct predecessor admission: %v", err)
	}
	successorAdmission, err := runtimeagenttopology.StaticAdmission(
		successorPlan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatalf("construct successor admission: %v", err)
	}
	if err := manager.InstallStartupTopology(oldStore, predecessorAdmission, predecessorPlan); err != nil {
		t.Fatalf("install predecessor topology: %v", err)
	}

	identity, err := records[0].Config.ConcreteIdentity()
	if err != nil {
		t.Fatalf("resolve static identity: %v", err)
	}
	revision, err := lifecycleConfigRevision(records[0])
	if err != nil {
		t.Fatalf("resolve static config revision: %v", err)
	}
	const epoch int64 = 19
	const generation uint64 = 7
	manager.lifecycle.mu.Lock()
	manager.lifecycle.cells[identity] = &agentLifecycleCell{
		identity: identity, epoch: epoch, generation: generation, phase: AgentLifecycleRunning,
		configRevision: revision, runMode: AgentRunModeStandard, topology: predecessorAdmission,
	}
	manager.lifecycle.mu.Unlock()
	t.Cleanup(func() {
		manager.lifecycle.mu.Lock()
		delete(manager.lifecycle.cells, identity)
		manager.lifecycle.mu.Unlock()
	})

	successorStore := newLifecyclePersistenceProbe()
	successorStore.exists = true
	successorStore.cell = lifecycleProbeCell{Epoch: epoch, Generation: generation, Phase: AgentLifecycleRunning}
	prepared, err := manager.PrepareStaticTopologySourceSetRebind(successorAdmission, successorPlan, source)
	if err != nil {
		t.Fatalf("prepare source-set rebind: %v", err)
	}
	if err := prepared.Commit(context.Background(), successorStore, "55555555-5555-4555-8555-555555555555"); err != nil {
		t.Fatalf("commit source-set rebind: %v", err)
	}

	requests := successorStore.requestsFor("source_set_rebind")
	if len(requests) != 1 {
		t.Fatalf("source-set rebind requests = %d, want 1", len(requests))
	}
	request := requests[0]
	if request.ExpectedEpoch != epoch || request.TargetEpoch != epoch ||
		request.ExpectedGeneration != generation || request.TargetGeneration != generation ||
		request.ExpectedPhase != AgentLifecycleRunning || request.TargetPhase != AgentLifecycleRunning ||
		request.ConfigRevision != revision || request.RunMode != AgentRunModeStandard {
		t.Fatalf("source-set rebind changed execution lifecycle: %#v", request)
	}
	if !request.Topology.Equal(successorAdmission) || request.Agent != nil {
		t.Fatalf("source-set rebind request = %#v, want topology-only successor admission", request)
	}
	state, exists := manager.lifecycle.stateByIdentity(identity)
	if !exists || state.RuntimeEpoch != epoch || state.Generation != generation || state.Phase != AgentLifecycleRunning ||
		state.ConfigRevision != revision || state.RunMode != AgentRunModeStandard || !state.Topology.Equal(successorAdmission) {
		t.Fatalf("rebound lifecycle state = %#v", state)
	}
	if manager.lifecycle.persistence() != successorStore {
		t.Fatal("lifecycle persistence did not rotate to the successor generation owner")
	}
}

func TestPreparedStaticTopologySourceSetRebindPreservesFailedDeclaration(t *testing.T) {
	source := loadPackageBackedStaticAgentSource(t)
	manager := newTestAgentManager(t, &recoveryTestBus{}, nil)
	manager.semanticSource = source
	records, err := manager.resolvedStaticTopologyRecords(source)
	if err != nil || len(records) != 1 {
		t.Fatalf("resolve static topology records: count=%d err=%v", len(records), err)
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash, BundleSource: "ephemeral"}
	removed := runtimeagenttopology.SourceCoordinate{BundleHash: removedManagerTestTopologyBundleHash, BundleSource: "persisted"}
	desired, err := desiredAgentsFromRecords(records, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	predecessorPlan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate, removed}, desired)
	if err != nil {
		t.Fatal(err)
	}
	successorPlan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatal(err)
	}
	predecessorAdmission, err := runtimeagenttopology.StaticAdmission(predecessorPlan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	successorAdmission, err := runtimeagenttopology.StaticAdmission(successorPlan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	oldStore := newLifecyclePersistenceProbe()
	if err := manager.InstallStartupTopology(oldStore, predecessorAdmission, predecessorPlan); err != nil {
		t.Fatal(err)
	}
	identity, _ := records[0].Config.ConcreteIdentity()
	revision, _ := lifecycleConfigRevision(records[0])
	const epoch int64 = 23
	const generation uint64 = 11
	manager.lifecycle.mu.Lock()
	manager.lifecycle.cells[identity] = &agentLifecycleCell{
		identity: identity, epoch: epoch, generation: generation, phase: AgentLifecycleFailed,
		configRevision: revision, runMode: AgentRunModeStopped, topology: predecessorAdmission,
	}
	manager.lifecycle.mu.Unlock()

	successorStore := newLifecyclePersistenceProbe()
	successorStore.exists = true
	successorStore.cell = lifecycleProbeCell{Epoch: epoch, Generation: generation, Phase: AgentLifecycleFailed}
	prepared, err := manager.PrepareStaticTopologySourceSetRebind(successorAdmission, successorPlan, source)
	if err != nil {
		t.Fatalf("prepare failed-cell source-set rebind: %v", err)
	}
	if err := prepared.Commit(context.Background(), successorStore, "66666666-6666-4666-8666-666666666666"); err != nil {
		t.Fatalf("commit failed-cell source-set rebind: %v", err)
	}
	requests := successorStore.requestsFor("source_set_rebind")
	if len(requests) != 1 || requests[0].ExpectedPhase != AgentLifecycleFailed || requests[0].TargetPhase != AgentLifecycleFailed {
		t.Fatalf("failed-cell topology-only request = %#v", requests)
	}
	state, exists := manager.lifecycle.stateByIdentity(identity)
	if !exists || state.Phase != AgentLifecycleFailed || state.Generation != generation || !state.Topology.Equal(successorAdmission) {
		t.Fatalf("failed-cell rebound state = %#v exists=%v", state, exists)
	}
}

type staticStartupReconcileStore struct {
	recoveryTestStore
	transitions []AgentLifecycleTransition
}

func (s *staticStartupReconcileStore) CommitAgentLifecycleTransition(_ context.Context, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	s.transitions = append(s.transitions, req)
	for i := range s.agents {
		identity, err := s.agents[i].Config.ConcreteIdentity()
		if err != nil || identity.Normalize() != req.Identity.Normalize() {
			continue
		}
		s.agents[i].LifecycleEpoch = req.TargetEpoch
		s.agents[i].LifecycleGeneration = req.TargetGeneration
		s.agents[i].LifecyclePhase = req.TargetPhase
		s.agents[i].LifecycleRunMode = req.RunMode
		s.agents[i].Topology = req.Topology
	}
	return AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: "77777777-7777-4777-8777-777777777777",
		Identity: req.Identity, AgentID: req.AgentID,
		PreviousEpoch: req.ExpectedEpoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: req.ExpectedGeneration, Generation: req.TargetGeneration,
		PreviousPhase: req.ExpectedPhase, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode, Topology: req.Topology,
	}, nil
}

func TestStaticTopologyStartupReintroducesDesiredFailedDeclaration(t *testing.T) {
	source := loadPackageBackedStaticAgentSource(t)
	store := &staticStartupReconcileStore{}
	manager := newTestAgentManager(t, &recoveryTestBus{}, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, store)
	manager.semanticSource = source
	records, err := manager.resolvedStaticTopologyRecords(source)
	if err != nil || len(records) != 1 {
		t.Fatalf("resolve static records: count=%d err=%v", len(records), err)
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash, BundleSource: "ephemeral"}
	desired, err := desiredAgentsFromRecords(records, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := runtimeagenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	failed := records[0]
	failed.Topology = admission
	failed.LifecycleEpoch = 31
	failed.LifecycleGeneration = 5
	failed.LifecyclePhase = AgentLifecycleFailed
	failed.LifecycleRunMode = AgentRunModeStopped
	failed.StartedAt = time.Now().UTC()
	store.agents = []PersistedAgent{failed}
	if err := manager.InstallStartupTopology(store, admission, plan); err != nil {
		t.Fatalf("install startup topology: %v", err)
	}
	manager.mu.Lock()
	manager.startupAgentsHydrated = false
	manager.mu.Unlock()
	if err := manager.ReconcileStaticTopologyForStartup(context.Background(), source); err != nil {
		t.Fatalf("reconcile desired failed declaration: %v", err)
	}
	if len(store.transitions) != 1 {
		t.Fatalf("startup transitions=%d, want 1", len(store.transitions))
	}
	transition := store.transitions[0]
	if transition.ExpectedPhase != AgentLifecycleFailed || transition.TargetPhase != AgentLifecycleRegistered || transition.TargetGeneration != 6 {
		t.Fatalf("failed declaration reintroduction = %#v", transition)
	}
	identity, _ := failed.Config.ConcreteIdentity()
	if _, ok := manager.lifecycle.executionSnapshotByIdentity(identity); !ok {
		t.Fatal("reintroduced declaration has no executable startup projection")
	}
}
