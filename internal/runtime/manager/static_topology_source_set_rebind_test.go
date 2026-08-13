package manager

import (
	"context"
	"testing"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
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
