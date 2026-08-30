package manager

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

const removedManagerTestTopologyBundleHash = "bundle-v2:sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

type sourceSetLifecycleCensusProbe struct {
	states []AgentLifecycleState
}

func (p sourceSetLifecycleCensusProbe) ListDurableAgentLifecycleStates(context.Context) ([]AgentLifecycleState, error) {
	return append([]AgentLifecycleState(nil), p.states...), nil
}

type sourceSetTransitionAdmissionProbe struct {
	mu           sync.Mutex
	id           string
	revision     string
	predecessors map[string]ProcessExecutionBinding
	done         chan struct{}
	once         sync.Once
	reads        atomic.Int64
}

func newSourceSetTransitionAdmissionProbe(revision string) *sourceSetTransitionAdmissionProbe {
	return &sourceSetTransitionAdmissionProbe{
		id: uuid.NewString(), revision: revision,
		predecessors: make(map[string]ProcessExecutionBinding), done: make(chan struct{}),
	}
}

func (p *sourceSetTransitionAdmissionProbe) TransitionID() string      { return p.id }
func (p *sourceSetTransitionAdmissionProbe) SourceSetRevision() string { return p.revision }
func (p *sourceSetTransitionAdmissionProbe) Done() <-chan struct{} {
	p.reads.Add(1)
	return p.done
}
func (p *sourceSetTransitionAdmissionProbe) RecordPredecessorProcessBinding(binding ProcessExecutionBinding) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := strings.Join([]string{
		strings.TrimSpace(binding.BundleHash),
		strings.TrimSpace(binding.RuntimeInstanceID),
	}, "\x00")
	if current, exists := p.predecessors[key]; exists && !current.Equal(binding) {
		return errors.New("predecessor process binding changed")
	}
	p.predecessors[key] = binding
	return nil
}
func (p *sourceSetTransitionAdmissionProbe) PredecessorProcessBinding(current ProcessExecutionBinding) (ProcessExecutionBinding, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := strings.Join([]string{
		strings.TrimSpace(current.BundleHash),
		strings.TrimSpace(current.RuntimeInstanceID),
	}, "\x00")
	binding, ok := p.predecessors[key]
	return binding, ok
}
func (p *sourceSetTransitionAdmissionProbe) release()         { p.once.Do(func() { close(p.done) }) }
func (p *sourceSetTransitionAdmissionProbe) readCount() int64 { return p.reads.Load() }

func sourceSetSuccessorProcessBinding() ProcessExecutionBinding {
	binding := lifecycleProbeProcessBinding()
	binding.GenerationGrantID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	binding.RuntimeGeneration++
	return binding
}

type sourceSetLifecycleCommitProbe struct {
	*lifecyclePersistenceProbe
	binding ProcessExecutionBinding
}

type durableSourceSetPreparationFixture struct {
	manager   *AgentManager
	source    semanticview.Source
	plan      runtimeagenttopology.SourceSetPlan
	admission runtimeagenttopology.Admission
	binding   ProcessExecutionBinding
	states    []AgentLifecycleState
}

func newDurableSourceSetPreparationFixture(t *testing.T) *durableSourceSetPreparationFixture {
	t.Helper()
	source := loadFilesystemStaticAgentSource(t)
	manager := newTestAgentManager(t, &recoveryTestBus{}, nil)
	manager.semanticSource = source
	records, err := manager.resolvedStaticTopologyRecords(source)
	if err != nil || len(records) != 1 {
		t.Fatalf("resolve static topology fixture: count=%d err=%v", len(records), err)
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash}
	desired, err := desiredAgentsFromRecords(records, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := runtimeagenttopology.StaticAdmission(
		plan.Revision, coordinate.BundleHash, runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.InstallStartupTopology(newLifecyclePersistenceProbe(), admission, plan); err != nil {
		t.Fatal(err)
	}
	identity, _ := records[0].Config.ConcreteIdentity()
	revision, _ := lifecycleConfigRevision(records[0])
	binding := lifecycleProbeProcessBinding()
	manager.lifecycle.mu.Lock()
	manager.lifecycle.cells[identity] = &agentLifecycleCell{
		identity: identity, epoch: 41, generation: 17, phase: AgentLifecycleRunning,
		configRevision: revision, runMode: AgentRunModeStandard, topology: admission, processBinding: binding,
	}
	manager.lifecycle.mu.Unlock()
	return &durableSourceSetPreparationFixture{
		manager: manager, source: source, plan: plan, admission: admission, binding: binding,
		states: []AgentLifecycleState{{
			Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: 41, Generation: 17,
			Phase: AgentLifecycleRunning, ConfigRevision: revision, RunMode: AgentRunModeStandard,
			Topology: admission, ProcessBinding: binding,
		}},
	}
}

func (f *durableSourceSetPreparationFixture) prepare(t *testing.T) (*PreparedDurableTopologySourceSetRebind, *sourceSetTransitionAdmissionProbe, error) {
	t.Helper()
	f.manager.roles.LifecycleCensus = sourceSetLifecycleCensusProbe{states: f.states}
	gate := newSourceSetTransitionAdmissionProbe(f.plan.Revision)
	prepared, err := f.manager.PrepareDurableTopologySourceSetRebind(
		context.Background(), f.admission, f.plan, f.source, f.binding, f.binding, false, gate, false,
	)
	return prepared, gate, err
}

func (p *sourceSetLifecycleCommitProbe) ProcessExecutionBinding() (ProcessExecutionBinding, error) {
	return p.binding, p.binding.Validate()
}

func (p *sourceSetLifecycleCommitProbe) CommitAgentLifecycleTransition(ctx context.Context, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	result, err := p.lifecyclePersistenceProbe.CommitAgentLifecycleTransition(ctx, req)
	if err != nil {
		return AgentLifecycleTransitionResult{}, err
	}
	result.ProcessBinding = p.binding
	return result, nil
}

func TestPreparedDurableTopologySourceSetRebindPreservesExecutionLifecycle(t *testing.T) {
	source := loadFilesystemStaticAgentSource(t)
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
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash}
	removed := runtimeagenttopology.SourceCoordinate{BundleHash: removedManagerTestTopologyBundleHash}
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
		predecessorPlan.Revision, coordinate.BundleHash, runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatalf("construct predecessor admission: %v", err)
	}
	successorAdmission, err := runtimeagenttopology.StaticAdmission(
		successorPlan.Revision, coordinate.BundleHash, runtimeagenttopology.LifetimeDurableManaged,
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
		processBinding: lifecycleProbeProcessBinding(),
	}
	manager.lifecycle.mu.Unlock()
	manager.roles.LifecycleCensus = sourceSetLifecycleCensusProbe{states: []AgentLifecycleState{{
		Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: epoch, Generation: generation,
		Phase: AgentLifecycleRunning, ConfigRevision: revision, RunMode: AgentRunModeStandard,
		Topology: predecessorAdmission, ProcessBinding: lifecycleProbeProcessBinding(),
	}}}
	t.Cleanup(func() {
		manager.lifecycle.mu.Lock()
		delete(manager.lifecycle.cells, identity)
		manager.lifecycle.mu.Unlock()
	})

	successorStore := &sourceSetLifecycleCommitProbe{
		lifecyclePersistenceProbe: newLifecyclePersistenceProbe(), binding: sourceSetSuccessorProcessBinding(),
	}
	successorStore.exists = true
	successorStore.cell = lifecycleProbeCell{Epoch: epoch, Generation: generation, Phase: AgentLifecycleRunning}
	transitionAdmission := newSourceSetTransitionAdmissionProbe(successorPlan.Revision)
	defer transitionAdmission.release()
	prepared, err := manager.PrepareDurableTopologySourceSetRebind(
		context.Background(), successorAdmission, successorPlan, source,
		lifecycleProbeProcessBinding(), lifecycleProbeProcessBinding(), false, transitionAdmission, false,
	)
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

func TestPrepareDurableTopologySourceSetRebindRejectsForeignGrantAtValidPredecessorRank(t *testing.T) {
	for _, test := range []struct {
		name          string
		mutateProcess bool
		mutateDurable bool
	}{
		{name: "process projection", mutateProcess: true},
		{name: "durable projection", mutateDurable: true},
		{name: "matching process and durable projections", mutateProcess: true, mutateDurable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDurableSourceSetPreparationFixture(t)
			predecessor := fixture.binding
			successor := sourceSetSuccessorProcessBinding()
			foreign := predecessor
			foreign.GenerationGrantID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
			if test.mutateProcess {
				fixture.manager.lifecycle.mu.Lock()
				for _, cell := range fixture.manager.lifecycle.cells {
					cell.processBinding = foreign
				}
				fixture.manager.lifecycle.mu.Unlock()
			}
			if test.mutateDurable {
				fixture.states[0].ProcessBinding = foreign
			}
			fixture.manager.roles.LifecycleCensus = sourceSetLifecycleCensusProbe{states: fixture.states}
			gate := newSourceSetTransitionAdmissionProbe(fixture.plan.Revision)
			defer gate.release()
			prepared, err := fixture.manager.PrepareDurableTopologySourceSetRebind(
				context.Background(), fixture.admission, fixture.plan, fixture.source,
				successor, predecessor, true, gate, false,
			)
			if prepared != nil {
				prepared.Abort()
			}
			if err == nil || !strings.Contains(err.Error(), "predecessor or successor") {
				t.Fatalf("foreign grant error=%v, want exact predecessor/successor refusal", err)
			}
		})
	}
}

func TestPreparedDurableTopologySourceSetRebindPreservesFailedDeclaration(t *testing.T) {
	source := loadFilesystemStaticAgentSource(t)
	manager := newTestAgentManager(t, &recoveryTestBus{}, nil)
	manager.semanticSource = source
	records, err := manager.resolvedStaticTopologyRecords(source)
	if err != nil || len(records) != 1 {
		t.Fatalf("resolve static topology records: count=%d err=%v", len(records), err)
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash}
	removed := runtimeagenttopology.SourceCoordinate{BundleHash: removedManagerTestTopologyBundleHash}
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
	predecessorAdmission, err := runtimeagenttopology.StaticAdmission(predecessorPlan.Revision, coordinate.BundleHash, runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	successorAdmission, err := runtimeagenttopology.StaticAdmission(successorPlan.Revision, coordinate.BundleHash, runtimeagenttopology.LifetimeDurableManaged)
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
		processBinding: lifecycleProbeProcessBinding(),
	}
	manager.lifecycle.mu.Unlock()
	manager.roles.LifecycleCensus = sourceSetLifecycleCensusProbe{states: []AgentLifecycleState{{
		Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: epoch, Generation: generation,
		Phase: AgentLifecycleFailed, ConfigRevision: revision, RunMode: AgentRunModeStopped,
		Topology: predecessorAdmission, ProcessBinding: lifecycleProbeProcessBinding(),
	}}}

	successorStore := &sourceSetLifecycleCommitProbe{
		lifecyclePersistenceProbe: newLifecyclePersistenceProbe(), binding: sourceSetSuccessorProcessBinding(),
	}
	successorStore.exists = true
	successorStore.cell = lifecycleProbeCell{Epoch: epoch, Generation: generation, Phase: AgentLifecycleFailed}
	transitionAdmission := newSourceSetTransitionAdmissionProbe(successorPlan.Revision)
	defer transitionAdmission.release()
	prepared, err := manager.PrepareDurableTopologySourceSetRebind(
		context.Background(), successorAdmission, successorPlan, source,
		lifecycleProbeProcessBinding(), lifecycleProbeProcessBinding(), false, transitionAdmission, false,
	)
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

func TestPreparedDurableTopologySourceSetRebindPreservesFlowReadinessAdmission(t *testing.T) {
	source := loadFilesystemStaticAgentSource(t)
	manager := newTestAgentManager(t, &recoveryTestBus{}, nil)
	manager.semanticSource = source
	records, err := manager.resolvedStaticTopologyRecords(source)
	if err != nil || len(records) != 1 {
		t.Fatalf("resolve static topology records: count=%d err=%v", len(records), err)
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash}
	removed := runtimeagenttopology.SourceCoordinate{BundleHash: removedManagerTestTopologyBundleHash}
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
	predecessorAdmission, err := runtimeagenttopology.StaticAdmission(
		predecessorPlan.Revision, coordinate.BundleHash, runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatal(err)
	}
	successorAdmission, err := runtimeagenttopology.StaticAdmission(
		successorPlan.Revision, coordinate.BundleHash, runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.InstallStartupTopology(newLifecyclePersistenceProbe(), predecessorAdmission, predecessorPlan); err != nil {
		t.Fatal(err)
	}
	staticIdentity, _ := records[0].Config.ConcreteIdentity()
	staticRevision, _ := lifecycleConfigRevision(records[0])
	flowIdentity := runtimeagentidentitytest.RootRuntime(t, "readiness-agent", "bundle-delete-source-set-rebind")
	flowTopology, err := runtimeagenttopology.FlowReadinessAdmission(uuid.NewString(), "review/instance-1", "plan-v1")
	if err != nil {
		t.Fatal(err)
	}
	const (
		epoch        int64  = 37
		generation   uint64 = 13
		flowRevision        = "flow-config-v1"
	)
	predecessorBinding := lifecycleProbeProcessBinding()
	manager.lifecycle.mu.Lock()
	manager.lifecycle.cells[staticIdentity] = &agentLifecycleCell{
		identity: staticIdentity, epoch: epoch, generation: generation, phase: AgentLifecycleRunning,
		configRevision: staticRevision, runMode: AgentRunModeStandard, topology: predecessorAdmission,
		processBinding: predecessorBinding,
	}
	manager.lifecycle.cells[flowIdentity] = &agentLifecycleCell{
		identity: flowIdentity, epoch: epoch, generation: generation, phase: AgentLifecycleRunning,
		configRevision: flowRevision, runMode: AgentRunModeAuthoritativeDeliveryOnly, topology: flowTopology,
		processBinding: predecessorBinding,
	}
	manager.lifecycle.mu.Unlock()
	manager.roles.LifecycleCensus = sourceSetLifecycleCensusProbe{states: []AgentLifecycleState{
		{
			Identity: staticIdentity, AgentID: staticIdentity.AgentID(), RuntimeEpoch: epoch, Generation: generation,
			Phase: AgentLifecycleRunning, ConfigRevision: staticRevision, RunMode: AgentRunModeStandard,
			Topology: predecessorAdmission, ProcessBinding: predecessorBinding,
		},
		{
			Identity: flowIdentity, AgentID: flowIdentity.AgentID(), RuntimeEpoch: epoch, Generation: generation,
			Phase: AgentLifecycleRunning, ConfigRevision: flowRevision, RunMode: AgentRunModeAuthoritativeDeliveryOnly,
			Topology: flowTopology, ProcessBinding: predecessorBinding,
		},
	}}
	successorStore := &sourceSetLifecycleCommitProbe{
		lifecyclePersistenceProbe: newLifecyclePersistenceProbe(), binding: sourceSetSuccessorProcessBinding(),
	}
	successorStore.exists = true
	successorStore.cell = lifecycleProbeCell{Epoch: epoch, Generation: generation, Phase: AgentLifecycleRunning}
	transitionAdmission := newSourceSetTransitionAdmissionProbe(successorPlan.Revision)
	defer transitionAdmission.release()
	prepared, err := manager.PrepareDurableTopologySourceSetRebind(
		context.Background(), successorAdmission, successorPlan, source,
		predecessorBinding, predecessorBinding, false, transitionAdmission, false,
	)
	if err != nil {
		t.Fatalf("prepare mixed survivor rebind: %v", err)
	}
	if err := prepared.Commit(context.Background(), successorStore, "99999999-9999-4999-8999-999999999999"); err != nil {
		t.Fatalf("commit mixed survivor rebind: %v", err)
	}
	requests := successorStore.requestsFor("source_set_rebind")
	if len(requests) != 2 {
		t.Fatalf("mixed survivor requests=%d, want 2", len(requests))
	}
	for _, request := range requests {
		switch request.Identity.Normalize() {
		case staticIdentity.Normalize():
			if !request.Topology.Equal(successorAdmission) {
				t.Fatalf("static target topology=%#v, want successor admission", request.Topology)
			}
		case flowIdentity.Normalize():
			if !request.Topology.Equal(flowTopology) {
				t.Fatalf("flow target topology=%#v, want preserved readiness admission", request.Topology)
			}
		default:
			t.Fatalf("unexpected survivor request for %s", request.Identity.Description())
		}
	}
	manager.lifecycle.mu.Lock()
	defer manager.lifecycle.mu.Unlock()
	if !manager.lifecycle.cells[staticIdentity].topology.Equal(successorAdmission) ||
		!manager.lifecycle.cells[flowIdentity].topology.Equal(flowTopology) ||
		!manager.lifecycle.cells[staticIdentity].processBinding.Equal(sourceSetSuccessorProcessBinding()) ||
		!manager.lifecycle.cells[flowIdentity].processBinding.Equal(sourceSetSuccessorProcessBinding()) {
		t.Fatal("mixed durable process projection did not publish atomically")
	}
}

func TestPrepareDurableTopologySourceSetRebindRejectsCensusProjectionMismatch(t *testing.T) {
	for _, test := range []struct {
		name           string
		addCensus      bool
		addProcessCell bool
		malformed      bool
		want           string
	}{
		{name: "durable row missing process cell", addCensus: true, want: "missing process projection"},
		{name: "process cell missing durable row", addProcessCell: true, want: "missing census row"},
		{name: "malformed durable authority", addCensus: true, malformed: true, want: "topology"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDurableSourceSetPreparationFixture(t)
			identity := runtimeagentidentitytest.RootRuntime(t, "readiness-mismatch", "bundle-delete-source-set-rebind")
			topology, err := runtimeagenttopology.FlowReadinessAdmission(uuid.NewString(), "review/mismatch", "plan-mismatch")
			if err != nil {
				t.Fatal(err)
			}
			if test.malformed {
				topology = runtimeagenttopology.Admission{}
			}
			state := AgentLifecycleState{
				Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: 43, Generation: 19,
				Phase: AgentLifecycleRunning, ConfigRevision: "flow-mismatch-v1", RunMode: AgentRunModeStandard,
				Topology: topology, ProcessBinding: fixture.binding,
			}
			if test.addCensus {
				fixture.states = append(fixture.states, state)
			}
			if test.addProcessCell {
				fixture.manager.lifecycle.mu.Lock()
				fixture.manager.lifecycle.cells[identity] = &agentLifecycleCell{
					identity: identity, epoch: state.RuntimeEpoch, generation: state.Generation, phase: state.Phase,
					configRevision: state.ConfigRevision, runMode: state.RunMode,
					topology: topology, processBinding: fixture.binding,
				}
				fixture.manager.lifecycle.mu.Unlock()
			}
			prepared, gate, prepareErr := fixture.prepare(t)
			if prepared != nil {
				prepared.Abort()
			}
			gate.release()
			if prepareErr == nil || !strings.Contains(prepareErr.Error(), test.want) {
				t.Fatalf("prepare error=%v, want %q", prepareErr, test.want)
			}
		})
	}
}

func TestPrepareDurableTopologySourceSetRebindLeavesTerminatedRowsAndEphemeralCellsInert(t *testing.T) {
	fixture := newDurableSourceSetPreparationFixture(t)
	terminatedIdentity := runtimeagentidentitytest.RootRuntime(t, "terminated-readiness", "bundle-delete-source-set-rebind")
	terminatedTopology, err := runtimeagenttopology.FlowReadinessAdmission(uuid.NewString(), "review/terminated", "plan-terminated")
	if err != nil {
		t.Fatal(err)
	}
	fixture.states = append(fixture.states, AgentLifecycleState{
		Identity: terminatedIdentity, AgentID: terminatedIdentity.AgentID(), RuntimeEpoch: 47, Generation: 23,
		Phase: AgentLifecycleTerminated, ConfigRevision: "flow-terminated-v1", RunMode: AgentRunModeStopped,
		Topology: terminatedTopology, ProcessBinding: fixture.binding,
	})
	ephemeralIdentity := runtimeagentidentitytest.RootRuntime(t, "ephemeral-agent", "bundle-delete-source-set-rebind")
	ephemeralTopology, err := runtimeagenttopology.NewEphemeralAdmission(uuid.NewString(), "runtime_shard")
	if err != nil {
		t.Fatal(err)
	}
	fixture.manager.lifecycle.mu.Lock()
	fixture.manager.lifecycle.cells[ephemeralIdentity] = &agentLifecycleCell{
		identity: ephemeralIdentity, epoch: 53, generation: 29, phase: AgentLifecycleRunning,
		configRevision: "ephemeral-v1", runMode: AgentRunModeStandard, topology: ephemeralTopology,
	}
	fixture.manager.lifecycle.mu.Unlock()
	prepared, gate, err := fixture.prepare(t)
	if err != nil {
		gate.release()
		t.Fatalf("prepare with inert terminal/ephemeral siblings: %v", err)
	}
	if len(prepared.bindings) != 1 {
		prepared.Abort()
		gate.release()
		t.Fatalf("prepared durable survivors=%d, want only the live static row", len(prepared.bindings))
	}
	prepared.Abort()
	gate.release()
}

func TestPrepareDurableTopologySourceSetRebindSerializesDurableRegistration(t *testing.T) {
	fixture := newDurableSourceSetPreparationFixture(t)
	prepared, gate, err := fixture.prepare(t)
	if err != nil {
		gate.release()
		t.Fatalf("prepare source-set transition: %v", err)
	}
	identity := runtimeagentidentitytest.RootRuntime(t, "late-readiness", "bundle-delete-source-set-rebind")
	topology, err := runtimeagenttopology.FlowReadinessAdmission(uuid.NewString(), "review/late", "plan-late")
	if err != nil {
		t.Fatal(err)
	}
	rec := startupTakeoverAgent(t, identity.AgentID(), topology, fixture.binding)
	rec.Config.Identity = identity
	admission := testManagerSubscriptionAdmission(t, rec.Config)
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		result <- fixture.manager.lifecycle.registerExecutionWithTopology(
			context.Background(), rec, true, reconfigureTestAgent{id: rec.Config.ID}, admission, topology,
		)
	}()
	<-started
	select {
	case registerErr := <-result:
		prepared.RetainForRetry()
		gate.release()
		t.Fatalf("registration escaped the publication lock: %v", registerErr)
	case <-time.After(100 * time.Millisecond):
	}
	prepared.RetainForRetry()
	select {
	case registerErr := <-result:
		if registerErr == nil || !strings.Contains(registerErr.Error(), "source_set_transition_pending") {
			gate.release()
			t.Fatalf("registration after retained partial transition error=%v", registerErr)
		}
	case <-time.After(time.Second):
		gate.release()
		t.Fatal("serialized registration did not return after local locks released")
	}
	gate.release()
}

type failSecondStaticTopologyRebindStore struct {
	calls   int
	binding ProcessExecutionBinding
	err     error
}

func (s *failSecondStaticTopologyRebindStore) CommitAgentLifecycleTransition(_ context.Context, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	s.calls++
	if s.calls == 2 {
		return AgentLifecycleTransitionResult{}, s.err
	}
	return AgentLifecycleTransitionResult{
		OperationID: req.OperationID, Identity: req.Identity, AgentID: req.AgentID,
		RuntimeEpoch: req.TargetEpoch, Generation: req.TargetGeneration, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode, Topology: req.Topology,
		ProcessBinding: s.binding,
	}, nil
}

func (s *failSecondStaticTopologyRebindStore) ProcessExecutionBinding() (ProcessExecutionBinding, error) {
	return s.binding, s.binding.Validate()
}

func TestPreparedDurableTopologySourceSetRebindPublishesBindingsOnlyAfterEveryCommit(t *testing.T) {
	manager := newTestAgentManager(t, &recoveryTestBus{}, nil)
	record := lifecycleTestPersistedAgent(t)
	identity, err := record.Config.ConcreteIdentity()
	if err != nil {
		t.Fatalf("resolve lifecycle identity: %v", err)
	}
	revision, err := lifecycleConfigRevision(record)
	if err != nil {
		t.Fatalf("resolve lifecycle config revision: %v", err)
	}
	admission := managerTestTopologyAdmission(t)
	first := &agentLifecycleCell{
		identity: identity, epoch: 31, generation: 7, phase: AgentLifecycleRunning,
		configRevision: revision, runMode: AgentRunModeStandard, topology: admission,
		processBinding: lifecycleProbeProcessBinding(),
	}
	second := &agentLifecycleCell{
		identity: identity, epoch: 32, generation: 8, phase: AgentLifecycleFailed,
		configRevision: revision, runMode: AgentRunModeStopped, topology: admission,
		processBinding: lifecycleProbeProcessBinding(),
	}
	manager.lifecycle.sourceSetPublishMu.Lock()
	first.opMu.Lock()
	second.opMu.Lock()
	prepared := &PreparedDurableTopologySourceSetRebind{
		manager: manager, admission: admission, coordinate: runtimeagenttopology.SourceCoordinate{
			BundleHash: managerTestTopologyBundleHash,
		}, currentBinding: lifecycleProbeProcessBinding(), currentIsSuccessor: true,
		plan: runtimeagenttopology.SourceSetPlan{Revision: admission.Authority.Static.SourceSetRevision},
		bindings: []durableTopologySourceSetBinding{
			{identity: identity, identityKey: "first", cell: first, epoch: first.epoch, generation: first.generation, phase: first.phase, runMode: first.runMode, revision: revision, currentTopology: admission, targetTopology: admission, currentBinding: lifecycleProbeProcessBinding()},
			{identity: identity, identityKey: "second", cell: second, epoch: second.epoch, generation: second.generation, phase: second.phase, runMode: second.runMode, revision: revision, currentTopology: admission, targetTopology: admission, currentBinding: lifecycleProbeProcessBinding()},
		},
		locked: []*agentLifecycleCell{first, second},
	}
	injected := errors.New("injected second lifecycle commit failure")
	store := &failSecondStaticTopologyRebindStore{binding: lifecycleProbeProcessBinding(), err: injected}
	if err := prepared.Commit(context.Background(), store, "88888888-8888-4888-8888-888888888888"); !errors.Is(err, injected) {
		t.Fatalf("source-set rebind error = %v, want injected second commit failure", err)
	}
	manager.lifecycle.mu.Lock()
	defer manager.lifecycle.mu.Unlock()
	if !first.processBinding.Equal(lifecycleProbeProcessBinding()) || !second.processBinding.Equal(lifecycleProbeProcessBinding()) {
		t.Fatalf("failed source-set rebind published partial bindings: first=%#v second=%#v", first.processBinding, second.processBinding)
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
		ProcessBinding: lifecycleProbeProcessBinding(),
	}, nil
}

func TestStaticTopologyStartupReintroducesDesiredFailedDeclaration(t *testing.T) {
	source := loadFilesystemStaticAgentSource(t)
	store := &staticStartupReconcileStore{}
	manager := newTestAgentManager(t, &recoveryTestBus{}, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, store)
	manager.semanticSource = source
	records, err := manager.resolvedStaticTopologyRecords(source)
	if err != nil || len(records) != 1 {
		t.Fatalf("resolve static records: count=%d err=%v", len(records), err)
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash}
	desired, err := desiredAgentsFromRecords(records, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := runtimeagenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, runtimeagenttopology.LifetimeDurableManaged)
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
