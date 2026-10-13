package manager

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/google/uuid"
)

type agentRowResponseLoss struct {
	AgentLifecyclePersistence
	binding  processExecutionBindingProvider
	runOwner RunExecutionOwner
	loseNext bool
	failNext bool
	commits  int
}

func (s *agentRowResponseLoss) ProcessExecutionBinding() (ProcessExecutionBinding, error) {
	return s.binding.ProcessExecutionBinding()
}

func (s *agentRowResponseLoss) InspectRunExecutionOwnership(ctx context.Context, runID string) (RunExecutionOwnership, error) {
	return s.runOwner.InspectRunExecutionOwnership(ctx, runID)
}

func (s *agentRowResponseLoss) CommitAgentLifecycleTransition(ctx context.Context, request AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	if s.failNext {
		s.failNext = false
		return AgentLifecycleTransitionResult{}, errors.New("injected pre-commit failure")
	}
	result, err := s.AgentLifecyclePersistence.CommitAgentLifecycleTransition(ctx, request)
	if err != nil {
		return result, err
	}
	s.commits++
	if s.loseNext {
		s.loseNext = false
		return AgentLifecycleTransitionResult{}, errors.New("injected response loss after native acknowledgment")
	}
	return result, nil
}

func ProveAgentRowNativeLostAckCacheAdoption(t *testing.T, store AgentLifecyclePersistence, reader AgentLifecycleStateReader, outcomes AgentLifecycleOutcomeReader, record PersistedAgent, snapshot func() any) {
	t.Helper()
	binding, ok := store.(processExecutionBindingProvider)
	if !ok {
		t.Fatal("native lifecycle fixture lacks exact process binding")
	}
	runOwner, ok := store.(RunExecutionOwner)
	if !ok {
		t.Fatal("native lifecycle fixture lacks run execution admission")
	}
	fault := &agentRowResponseLoss{AgentLifecyclePersistence: store, binding: binding, runOwner: runOwner}
	am := newTestAgentManagerWithOptions(t, newProjectionTestBus(), nil, AgentManagerOptions{LifecycleStore: fault, PersistenceRoles: PersistenceRoles{LifecycleState: reader, LifecycleOutcome: outcomes}})
	ctx := testAuthorActivityContext(context.Background())
	if err := am.lifecycle.registerExecution(ctx, record, false, reconfigureTestAgent{id: record.Config.ID}, testManagerSubscriptionAdmission(t, record.Config)); err != nil {
		t.Fatal(err)
	}
	if err := am.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := am.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	before, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found {
		t.Fatalf("running native row: found=%v err=%v", found, err)
	}
	old, live := am.lifecycle.tokenIdentity(record.Config.Identity)
	if !live {
		t.Fatal("original native execution is not live")
	}
	operation := uuid.NewString()
	fault.loseNext = true
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", operation, nil, am.semanticSource, &record.Topology, nil); err == nil {
		t.Fatal("lost acknowledgment reported successful replacement")
	}
	committed, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || committed.Generation != before.Generation+1 {
		t.Fatalf("response loss did not reach the native successor: %+v found=%v err=%v", committed, found, err)
	}
	afterCommit := snapshot()
	count := fault.commits
	if lease, err := am.lifecycle.acquireExecutionIdentity(ctx, record.Config.Identity, "native lost-ack negative", true); err == nil || lease != nil {
		if lease != nil {
			lease.Release()
		}
		t.Fatal("old projection admitted after immutable native outcome confirmed the successor")
	}
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", operation, nil, am.semanticSource, &record.Topology, nil); err != nil {
		t.Fatalf("exact immutable-outcome adoption: %v", err)
	}
	if fault.commits != count || !reflect.DeepEqual(afterCommit, snapshot()) {
		t.Fatal("exact lost-ack adoption repeated native mutation or changed durable evidence")
	}
	if token, live := am.lifecycle.tokenIdentity(record.Config.Identity); !live || token.Generation != committed.Generation || token == old {
		t.Fatalf("exact adoption did not install its native successor: %+v live=%v", token, live)
	}
	fault.failNext = true
	rollbackSnapshot := snapshot()
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", uuid.NewString(), nil, am.semanticSource, &record.Topology, nil); err == nil {
		t.Fatal("pre-commit failure reported success")
	}
	lease, err := am.lifecycle.acquireExecutionIdentity(ctx, record.Config.Identity, "native rollback control", true)
	if err != nil {
		t.Fatalf("absent outcome rejected unchanged native execution: %v", err)
	}
	lease.Release()
	if !reflect.DeepEqual(rollbackSnapshot, snapshot()) {
		t.Fatal("rollback/outcome absence changed durable evidence")
	}
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", uuid.NewString(), nil, am.semanticSource, &record.Topology, nil); err != nil {
		t.Fatal(err)
	}
	historicalSnapshot := snapshot()
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", operation, nil, am.semanticSource, &record.Topology, nil); err == nil {
		t.Fatal("historical operation resurrected a superseded execution")
	}
	if !reflect.DeepEqual(historicalSnapshot, snapshot()) {
		t.Fatal("historical replay changed durable evidence")
	}
	view, _, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil {
		t.Fatal(err)
	}
	// A discarded process view is reconstructed solely from the current native row.
	reconstructed := newAgentLifecycleCoordinator(store, nil, reader, nil)
	record.LifecycleEpoch, record.LifecycleGeneration = 99, 88
	record.LifecyclePhase, record.LifecycleRunMode = AgentLifecycleFailed, AgentRunModeStopped
	if err := reconstructed.registerExecution(ctx, record, false, nil, testManagerSubscriptionAdmission(t, record.Config)); err != nil {
		t.Fatal(err)
	}
	adopted, found := reconstructed.stateByIdentity(record.Config.Identity)
	if !found || !sameAgentRowObservation(view, adopted) || !reflect.DeepEqual(historicalSnapshot, snapshot()) {
		t.Fatal("cache reconstruction replaced native currentness or wrote evidence")
	}
	if lease, err := reconstructed.acquireExecutionIdentity(ctx, record.Config.Identity, "reconstructed cache", true); lease != nil || err == nil {
		if lease != nil {
			lease.Release()
		}
		t.Fatal("row reconstruction fabricated executable process admission")
	}
	reconstructed.mu.Lock()
	projection := reconstructed.cells[record.Config.Identity].execution
	if projection == nil || projection.agent != nil || projection.route != nil || projection.loopDone != nil || len(projection.leases) != 0 {
		reconstructed.mu.Unlock()
		t.Fatal("row reconstruction fabricated running process resources")
	}
	projection.cancelGeneration()
	reconstructed.mu.Unlock()
}

func ProveAgentRowIndividualSourceSetReplacement(t *testing.T, previous AgentLifecyclePersistence, reader AgentLifecycleStateReader, record PersistedAgent, advance func() (AgentLifecyclePersistence, agenttopology.Admission)) {
	t.Helper()
	bus := newProjectionTestBus()
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{LifecycleStore: previous, PersistenceRoles: PersistenceRoles{LifecycleState: reader}})
	ctx := testAuthorActivityContext(context.Background())
	agent := &reconfigureTestAgent{id: record.Config.ID}
	if err := am.lifecycle.registerExecution(ctx, record, false, agent, testManagerSubscriptionAdmission(t, record.Config)); err != nil {
		t.Fatal(err)
	}
	if err := am.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatalf("install original running Manager projection: %v", err)
	}
	t.Cleanup(func() {
		if err := am.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	before, _, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil {
		t.Fatal(err)
	}
	am.lifecycle.mu.Lock()
	predecessor := am.lifecycle.cells[record.Config.Identity].execution
	am.lifecycle.mu.Unlock()
	successor, topology := advance()
	am.lifecycle.replacePersistence(successor)
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", uuid.NewString(), nil, am.semanticSource, &topology, nil); err != nil {
		t.Fatalf("real individual source-set replacement: %v", err)
	}
	after, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || after.Generation != before.Generation+1 || after.Phase != AgentLifecycleRunning || !after.Topology.Equal(topology) {
		t.Fatalf("native successor row: before=%#v after=%#v found=%v err=%v", before, after, found, err)
	}
	am.lifecycle.mu.Lock()
	cell := am.lifecycle.cells[record.Config.Identity]
	valid := cell.execution != predecessor && cell.execution.agent == agent && predecessor.fenced &&
		cell.execution.token == lifecycleToken(record.Config.Identity, after.RuntimeEpoch, after.Generation) &&
		am.lifecycle.executionRunnableCurrentOccurrenceLocked(cell)
	am.lifecycle.mu.Unlock()
	if !valid {
		t.Fatal("native generation advanced without its installed runnable successor projection")
	}
	if err := am.Shutdown(); err != nil {
		t.Fatal(err)
	}
}
