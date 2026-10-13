package manager

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type agentRowResponseLoss struct {
	AgentLifecyclePersistence
	binding     processExecutionBindingProvider
	runOwner    RunExecutionOwner
	loseNext    bool
	failNext    bool
	commits     int
	lastRequest AgentLifecycleTransition
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
	s.lastRequest = request
	if s.loseNext {
		s.loseNext = false
		return AgentLifecycleTransitionResult{}, errors.New("injected response loss after native acknowledgment")
	}
	return result, nil
}

func ProveAgentRowNativeLostAckCacheAdoption(t *testing.T, store AgentLifecyclePersistence, reader AgentLifecycleStateReader, outcomes AgentLifecycleOutcomeReader, record PersistedAgent, snapshot func() any, admit ReconfigureMemoryNativeAdmission, publicRestart func(agentcontrol.Controller)) {
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
	bus := newProjectionTestBus()
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{LLMBackend: record.Config.LLMBackend, LifecycleStore: fault, PersistenceRoles: PersistenceRoles{LifecycleState: reader, LifecycleOutcome: outcomes}})
	if am.lifecycle.persistence() != fault || am.lifecycle.stateReader != reader || am.lifecycle.outcomeReader != outcomes {
		t.Fatal("fixture constructor replaced an explicitly supplied native row/outcome owner")
	}
	if _, unit := am.lifecycle.persistence().(*ephemeralLifecyclePersistenceProbe); unit {
		t.Fatal("supplied native authority selected the unit-only row model")
	}
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
	patch := record.Config
	patch.MaxTurnsPerTask++
	builds := 0
	am.factory = func(config runtimeactors.AgentConfig) (Agent, error) {
		builds++
		return reconfigureTestAgent{id: config.ID}, nil
	}
	successor, topology := admit(patch)
	fault.AgentLifecyclePersistence = successor
	fault.binding, ok = successor.(processExecutionBindingProvider)
	if !ok {
		t.Fatal("native replacement grant lacks exact process binding")
	}
	fault.runOwner, ok = successor.(RunExecutionOwner)
	if !ok {
		t.Fatal("native replacement grant lacks run execution admission")
	}
	record.Topology = topology
	fault.loseNext = true
	_, lostErr := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "reconfigure", operation, &patch, am.semanticSource, &record.Topology, nil)
	if lostErr == nil {
		t.Fatal("lost acknowledgment reported successful replacement")
	}
	committed, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || committed.Generation != before.Generation+1 {
		t.Fatalf("response loss did not reach the native successor: before=%+v after=%+v found=%v err=%v replacement=%v", before, committed, found, err, lostErr)
	}
	afterCommit := snapshot()
	count := fault.commits
	beforeNoOpBuilds := builds
	_, noOpErr := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "reconfigure", uuid.NewString(), &record.Config, am.semanticSource, &before.Topology, nil)
	var conflict *runtimefailures.Error
	if !errors.As(noOpErr, &conflict) || conflict.Failure.Detail.Code != "lifecycle_unacknowledged_request_conflict" {
		t.Fatalf("predecessor configuration no-op did not refuse the pending outcome: %v", noOpErr)
	}
	if builds != beforeNoOpBuilds || fault.commits != count || !reflect.DeepEqual(afterCommit, snapshot()) {
		t.Fatal("predecessor configuration no-op built execution or changed native evidence")
	}
	if lease, err := am.lifecycle.acquireExecutionIdentity(ctx, record.Config.Identity, "native lost-ack negative", true); err == nil || lease != nil {
		if lease != nil {
			lease.Release()
		}
		t.Fatal("old projection admitted after immutable native outcome confirmed the successor")
	}
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "reconfigure", operation, &patch, am.semanticSource, &record.Topology, nil); err != nil {
		t.Fatalf("exact immutable-outcome adoption: %v", err)
	}
	if fault.commits != count || !reflect.DeepEqual(afterCommit, snapshot()) {
		t.Fatal("exact lost-ack adoption repeated native mutation or changed durable evidence")
	}
	if token, live := am.lifecycle.tokenIdentity(record.Config.Identity); !live || token.Generation != committed.Generation || token == old {
		t.Fatalf("exact adoption did not install its native successor: %+v live=%v", token, live)
	}
	record.Config = patch
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
	historicalOperation := uuid.NewString()
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", historicalOperation, nil, am.semanticSource, &record.Topology, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", uuid.NewString(), nil, am.semanticSource, &record.Topology, nil); err != nil {
		t.Fatal(err)
	}
	historicalSnapshot := snapshot()
	if _, err := am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", historicalOperation, nil, am.semanticSource, &record.Topology, nil); err == nil {
		t.Fatal("historical operation resurrected a superseded execution")
	}
	if !reflect.DeepEqual(historicalSnapshot, snapshot()) {
		t.Fatal("historical replay changed durable evidence")
	}
	publicRestart(am)
	proveAgentRowNativePublicationPanic(t, ctx, am, bus, reader, record)
	record = proveAgentRowNativeCompleteReplacements(t, ctx, am, bus, fault, reader, outcomes, record, snapshot, admit)
	historicalSnapshot = snapshot()
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

func proveAgentRowNativeCompleteReplacements(t *testing.T, ctx context.Context, am *AgentManager, bus *projectionTestBus, fault *agentRowResponseLoss, reader AgentLifecycleStateReader, outcomes AgentLifecycleOutcomeReader, record PersistedAgent, snapshot func() any, admit ReconfigureMemoryNativeAdmission) PersistedAgent {
	t.Helper()
	before, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found {
		t.Fatalf("complete replacement predecessor: found=%v err=%v", found, err)
	}
	install := func(config runtimeactors.AgentConfig) agenttopology.Admission {
		t.Helper()
		owner, topology := admit(config)
		fault.AgentLifecyclePersistence = owner
		var ok bool
		fault.binding, ok = owner.(processExecutionBindingProvider)
		if !ok {
			t.Fatal("complete replacement lacks native process binding")
		}
		fault.runOwner, ok = owner.(RunExecutionOwner)
		if !ok {
			t.Fatal("complete replacement lacks native run authority")
		}
		return topology
	}
	loader, ok := reader.(interface {
		LoadAgents(context.Context) ([]PersistedAgent, error)
	})
	if !ok {
		t.Fatal("native replacement proof lacks the descriptor reader")
	}
	stored, err := loader.LoadAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var base runtimeactors.AgentConfig
	for _, agent := range stored {
		if agent.Config.Identity == record.Config.Identity {
			base = agent.Config
		}
	}
	if base.ID == "" {
		t.Fatal("native replacement has no complete predecessor descriptor")
	}
	visible, err := am.ResolveAgentConfig(record.Config.Identity.RunID, record.Config.ID, record.Config.CanonicalFlowPath())
	if err != nil {
		t.Fatal(err)
	}
	base.Prompt = visible.Prompt
	first, second := base, base
	first.MaxTurnsPerTask += 11
	first.BudgetEnvelope += 1
	second.MaxTurnsPerTask += 22
	second.BudgetEnvelope += 2
	firstTopology := install(first)
	prove := func(expected runtimeactors.AgentConfig, generation uint64) AgentLifecycleState {
		t.Helper()
		row, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
		if err != nil || !found || row.Generation != generation || row.Phase != AgentLifecycleRunning {
			t.Fatalf("complete replacement row: found=%v row=%+v err=%v", found, row, err)
		}
		visible, err := am.ResolveAgentConfig(record.Config.Identity.RunID, record.Config.ID, record.Config.CanonicalFlowPath())
		if err != nil || !reflect.DeepEqual(visible, expected) {
			t.Fatalf("complete replacement mixed process configuration: got=%+v want=%+v err=%v", visible, expected, err)
		}
		agents, err := loader.LoadAgents(ctx)
		if err != nil {
			t.Fatal(err)
		}
		matched := false
		for _, agent := range agents {
			if agent.Config.Identity == record.Config.Identity {
				actualBytes, actualErr := canonicaljson.Bytes(agent.Config)
				expectedBytes, expectedErr := canonicaljson.Bytes(expected)
				if actualErr != nil || expectedErr != nil || string(actualBytes) != string(expectedBytes) {
					t.Fatalf("complete replacement mixed durable configuration: actual=%s expected=%s errors=%v/%v", actualBytes, expectedBytes, actualErr, expectedErr)
				}
				matched = true
			}
		}
		if !matched {
			t.Fatal("complete replacement lost its native descriptor")
		}
		result, found, err := outcomes.ReadAgentLifecycleOutcome(ctx, fault.lastRequest)
		if err != nil || !found || result.TransitionID != row.LastTransitionID || result.Generation != generation || !result.Replayed {
			t.Fatalf("complete replacement lacks its exact immutable generation receipt: found=%v result=%+v err=%v", found, result, err)
		}
		return row
	}
	building, releaseBuild := make(chan struct{}), make(chan struct{})
	var buildOnce sync.Once
	am.factory = func(config runtimeactors.AgentConfig) (Agent, error) {
		if config.MaxTurnsPerTask == first.MaxTurnsPerTask {
			buildOnce.Do(func() { close(building) })
			<-releaseBuild
		}
		return reconfigureTestAgent{id: config.ID}, nil
	}
	defer func() {
		select {
		case <-releaseBuild:
		default:
			close(releaseBuild)
		}
	}()
	oldResult := make(chan error, 1)
	go func() {
		oldResult <- am.reconfigureAgentIdentityExactWithTopology(ctx, am.semanticSource, record.Config.Identity, first, &firstTopology)
	}()
	select {
	case <-building:
	case err := <-oldResult:
		t.Fatalf("old complete candidate never reached held preparation: %v", err)
	}
	secondTopology := install(second)
	newStarted, newResult := make(chan struct{}), make(chan error, 1)
	go func() {
		close(newStarted)
		newResult <- am.reconfigureAgentIdentityExactWithTopology(ctx, am.semanticSource, record.Config.Identity, second, &secondTopology)
	}()
	<-newStarted
	commits := fault.commits
	close(releaseBuild)
	oldErr, newErr := <-oldResult, <-newResult
	if oldErr == nil || newErr != nil || fault.commits != commits+1 {
		t.Fatalf("concurrent complete candidates: old=%v new=%v native commits=%d want=%d", oldErr, newErr, fault.commits, commits+1)
	}
	if oldErr.Error() != "static lifecycle topology authority differs from runtime generation grant" {
		t.Fatalf("held complete candidate did not reach the existing generation-grant binding refusal: %v", oldErr)
	}
	current := prove(second, before.Generation+1)
	unchanged := snapshot()
	previousRoutes := bus.routeHistory(record.Config.ID)
	am.lifecycle.mu.Lock()
	previousExecution := am.lifecycle.cells[record.Config.Identity].execution
	am.lifecycle.mu.Unlock()
	for _, stale := range []struct {
		name         string
		config       runtimeactors.AgentConfig
		topology     agenttopology.Admission
		nativeConfig bool
	}{
		{name: "old_config_and_topology", config: first, topology: firstTopology},
		{name: "old_config_current_topology", config: first, topology: secondTopology, nativeConfig: true},
		{name: "current_config_old_topology", config: second, topology: firstTopology},
	} {
		staleErr := am.reconfigureAgentIdentityExactWithTopology(ctx, am.semanticSource, record.Config.Identity, stale.config, &stale.topology)
		if stale.nativeConfig {
			var sourceConflict *runtimefailures.Error
			if !errors.As(staleErr, &sourceConflict) || sourceConflict.Failure.Detail.Code != "agent_topology_authority_conflict" || !strings.Contains(staleErr.Error(), "static_config_revision_mismatch") {
				t.Fatalf("%s did not reach native transaction-time declaration refusal: %v", stale.name, staleErr)
			}
		} else if staleErr == nil || staleErr.Error() != "static lifecycle topology authority differs from runtime generation grant" {
			t.Fatalf("%s did not reach generation-grant binding refusal: %v", stale.name, staleErr)
		}
		am.lifecycle.mu.Lock()
		cell := am.lifecycle.cells[record.Config.Identity]
		conserved := cell.execution == previousExecution && sameAgentRowObservation(current, cell.view) && !cell.execution.fenced && am.lifecycle.executionRunnableCurrentOccurrenceLocked(cell)
		am.lifecycle.mu.Unlock()
		if !conserved || !reflect.DeepEqual(unchanged, snapshot()) || !reflect.DeepEqual(previousRoutes, bus.routeHistory(record.Config.ID)) {
			t.Fatalf("%s changed lifecycle/session/route/cache evidence", stale.name)
		}
	}
	third := second
	third.MaxTurnsPerTask++
	third.BudgetEnvelope++
	thirdTopology := install(third)
	if err := am.reconfigureAgentIdentityExactWithTopology(ctx, am.semanticSource, record.Config.Identity, third, &thirdTopology); err != nil {
		t.Fatalf("next legal complete source-owned replacement: %v", err)
	}
	prove(third, before.Generation+2)
	unchanged, previousRoutes = snapshot(), bus.routeHistory(record.Config.ID)
	am.factory = func(runtimeactors.AgentConfig) (Agent, error) {
		t.Fatal("same-current complete candidate invoked the factory")
		return nil, nil
	}
	commits = fault.commits
	if err := am.reconfigureAgentIdentityExactWithTopology(ctx, am.semanticSource, record.Config.Identity, third, &thirdTopology); err != nil {
		t.Fatal(err)
	}
	if fault.commits != commits || !reflect.DeepEqual(unchanged, snapshot()) || !reflect.DeepEqual(previousRoutes, bus.routeHistory(record.Config.ID)) {
		t.Fatal("same-current complete candidate repeated native or route publication")
	}
	am.factory = func(config runtimeactors.AgentConfig) (Agent, error) { return reconfigureTestAgent{id: config.ID}, nil }
	record.Config, record.Topology = third, thirdTopology
	return record
}

func proveAgentRowNativePublicationPanic(t *testing.T, ctx context.Context, am *AgentManager, bus *projectionTestBus, reader AgentLifecycleStateReader, record PersistedAgent) {
	t.Helper()
	before, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || before.Phase != AgentLifecycleRunning {
		t.Fatalf("native publication-panic predecessor: found=%v row=%+v err=%v", found, before, err)
	}
	var done, settled <-chan struct{}
	bus.beforePublish = func() {
		committed, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
		if err != nil || !found || committed.Generation != before.Generation+1 || committed.Phase != AgentLifecycleRunning {
			t.Fatalf("publication cut did not follow native running CAS: found=%v row=%+v err=%v", found, committed, err)
		}
		am.lifecycle.mu.Lock()
		execution := am.lifecycle.cells[record.Config.Identity].execution
		done, settled = execution.loopDone, execution.loopSettled
		am.lifecycle.mu.Unlock()
		panic("native post-CAS route publication")
	}
	_, err = am.replaceExecutionTargetConfigWithTopology(ctx, record.Config.Identity, "restart", uuid.NewString(), nil, am.semanticSource, &record.Topology, nil)
	bus.beforePublish = nil
	if err == nil || !strings.Contains(err.Error(), "native post-CAS route publication") {
		t.Fatalf("publication panic was not returned: %v", err)
	}
	after, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || after.Generation != before.Generation+1 || after.Phase != AgentLifecycleRegistered || after.RunMode != AgentRunModeStopped || after.RuntimeEpoch != before.RuntimeEpoch || after.ConfigRevision != before.ConfigRevision {
		t.Fatalf("native publication compensation: before=%+v after=%+v found=%v err=%v", before, after, found, err)
	}
	for name, completion := range map[string]<-chan struct{}{"loop": done, "settlement": settled} {
		if completion == nil {
			t.Fatalf("publication cut omitted its %s completion", name)
		}
		select {
		case <-completion:
		default:
			t.Fatalf("unlaunched %s completion retained after compensation", name)
		}
	}
	if _, installed := bus.current(record.Config.ID); installed {
		t.Fatal("publication compensation retained an installed route")
	}
	am.lifecycle.mu.Lock()
	cell := am.lifecycle.cells[record.Config.Identity]
	execution := cell.execution
	clean := sameAgentRowObservation(after, cell.view) && execution.fenced && execution.generationCtx.Err() != nil &&
		execution.route == nil && execution.loopDone == nil && execution.loopSettled == nil && len(execution.leases) == 0 && cell.retirement == nil
	am.lifecycle.mu.Unlock()
	if !clean {
		t.Fatal("publication compensation retained executable resources or contradicted the native row")
	}
	if err := am.Shutdown(); err != nil {
		t.Fatalf("publication compensation left a Manager lease or join: %v", err)
	}
	if err := am.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatalf("restart after exact publication cleanup: %v", err)
	}
	current, found, err := reader.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err != nil || !found || current.Generation != after.Generation+1 || current.Phase != AgentLifecycleRunning {
		t.Fatalf("publication cleanup recovery: found=%v row=%+v err=%v", found, current, err)
	}
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
