package manager

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

type lifecyclePersistenceProbe struct {
	mu         sync.Mutex
	cell       lifecycleProbeCell
	exists     bool
	operations map[string]AgentLifecycleTransitionResult
	requests   []AgentLifecycleTransition
	failNext   error
	failAfter  error
}

type lifecycleProbeCell struct {
	Epoch      int64
	Generation uint64
	Phase      AgentLifecyclePhase
}

func newLifecyclePersistenceProbe() *lifecyclePersistenceProbe {
	return &lifecyclePersistenceProbe{operations: map[string]AgentLifecycleTransitionResult{}}
}

func (p *lifecyclePersistenceProbe) CommitAgentLifecycleTransition(_ context.Context, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, req)
	if p.failNext != nil {
		err := p.failNext
		p.failNext = nil
		return AgentLifecycleTransitionResult{}, err
	}
	if result, ok := p.operations[req.OperationID]; ok {
		result.Replayed = true
		return result, nil
	}
	if p.exists {
		if p.cell.Epoch != req.ExpectedEpoch || p.cell.Generation != req.ExpectedGeneration || p.cell.Phase != req.ExpectedPhase {
			return AgentLifecycleTransitionResult{}, fmt.Errorf("probe lifecycle conflict")
		}
	} else if req.OperationKind != "spawn" {
		return AgentLifecycleTransitionResult{}, fmt.Errorf("probe lifecycle cell absent")
	}
	result := AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: uuid.NewString(), AgentID: req.AgentID,
		PreviousEpoch: p.cell.Epoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: p.cell.Generation, Generation: req.TargetGeneration,
		PreviousPhase: p.cell.Phase, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode,
	}
	p.cell = lifecycleProbeCell{Epoch: req.TargetEpoch, Generation: req.TargetGeneration, Phase: req.TargetPhase}
	p.exists = true
	p.operations[req.OperationID] = result
	if p.failAfter != nil {
		err := p.failAfter
		p.failAfter = nil
		return AgentLifecycleTransitionResult{}, err
	}
	return result, nil
}

func (p *lifecyclePersistenceProbe) requestsFor(kind string) []AgentLifecycleTransition {
	p.mu.Lock()
	defer p.mu.Unlock()
	requests := make([]AgentLifecycleTransition, 0, len(p.requests))
	for _, req := range p.requests {
		if req.OperationKind == kind {
			requests = append(requests, req)
		}
	}
	return requests
}

func TestLifecycleCoordinatorReplayDoesNotReplaceSuccessfulGeneration(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	operationID := uuid.NewString()
	loopCtx, token, done, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "restart", operationID, nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil || loopCtx == nil {
		t.Fatalf("first replacement ctx=%v token=%+v err=%v", loopCtx, token, err)
	}
	replayedCtx, replayedToken, replayedDone, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "restart", operationID, nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("replay replacement: %v", err)
	}
	if replayedCtx != nil || replayedDone != nil || replayedToken != token {
		t.Fatalf("replay created another owner: ctx=%v token=%+v done=%v want token=%+v", replayedCtx, replayedToken, replayedDone, token)
	}
	select {
	case <-loopCtx.Done():
		t.Fatal("replay cancelled the successful generation")
	default:
	}
	coordinator.cancelShutdownWork()
	if err := coordinator.releaseLoop(token, done); err != nil {
		t.Fatalf("release loop: %v", err)
	}
}

func TestLifecycleCoordinatorReconfigureOperationIdentityTracksTransitionOccurrence(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	base := lifecycleTestPersistedAgent()
	if err := coordinator.register(testAuthorActivityContext(context.Background()), base, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	plan := runtimesessions.LifecycleMutationPlan{
		Action:            runtimesessions.LifecycleMutationRotateCurrentSet,
		TerminationReason: runtimesessions.TerminationReasonNormal,
		TerminationDetail: "agent_reconfigured",
		CheckpointSummary: "agent reconfigured",
	}
	recA := base
	recA.Config.Tools = []string{"tool-a"}
	recB := base
	recB.Config.Tools = []string{"tool-b"}
	for i, rec := range []*PersistedAgent{&recA, &recB, &recA} {
		if _, _, _, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), base.Config.ID, "reconfigure", "", rec, plan); err != nil {
			t.Fatalf("reconfigure occurrence %d: %v", i+1, err)
		}
	}
	if _, _, _, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), base.Config.ID, "reconfigure", "", &recA, plan); err != nil {
		t.Fatalf("same-current reconfigure: %v", err)
	}

	requests := probe.requestsFor("reconfigure")
	if len(requests) != 3 {
		t.Fatalf("reconfigure requests = %d, want 3 committed occurrences", len(requests))
	}
	seen := map[string]struct{}{}
	for i, req := range requests {
		if _, duplicate := seen[req.OperationID]; duplicate {
			t.Fatalf("occurrence %d reused operation_id %q", i+1, req.OperationID)
		}
		seen[req.OperationID] = struct{}{}
		if i > 0 && req.ExpectedGeneration != requests[i-1].TargetGeneration {
			t.Fatalf("occurrence %d expected generation = %d, want %d", i+1, req.ExpectedGeneration, requests[i-1].TargetGeneration)
		}
	}
	if requests[0].ConfigRevision != requests[2].ConfigRevision {
		t.Fatalf("A -> B -> A revisions differ: first=%q third=%q", requests[0].ConfigRevision, requests[2].ConfigRevision)
	}
	if requests[0].Subordinate.Action != requests[2].Subordinate.Action {
		t.Fatalf("A -> B -> A plans differ: first=%q third=%q", requests[0].Subordinate.Action, requests[2].Subordinate.Action)
	}
}

func TestLifecycleCoordinatorReconfigureOperationIdentityIsStableBeforeCommit(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	base := lifecycleTestPersistedAgent()
	if err := coordinator.register(testAuthorActivityContext(context.Background()), base, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	target := base
	target.Config.Tools = []string{"tool-a"}
	probe.mu.Lock()
	probe.failNext = fmt.Errorf("injected persistence failure")
	probe.mu.Unlock()
	if _, _, _, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), base.Config.ID, "reconfigure", "", &target, runtimesessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("first reconfigure succeeded despite persistence failure")
	}
	if _, _, _, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), base.Config.ID, "reconfigure", "", &target, runtimesessions.LifecycleMutationPlan{}); err != nil {
		t.Fatalf("retry reconfigure: %v", err)
	}
	requests := probe.requestsFor("reconfigure")
	if len(requests) != 2 {
		t.Fatalf("reconfigure attempts = %d, want 2", len(requests))
	}
	if requests[0].OperationID != requests[1].OperationID {
		t.Fatalf("retry operation ids differ: first=%q retry=%q", requests[0].OperationID, requests[1].OperationID)
	}
}

func TestLifecycleCoordinatorReconfigureRetryAdoptsCommittedOccurrence(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	base := lifecycleTestPersistedAgent()
	if err := coordinator.register(testAuthorActivityContext(context.Background()), base, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	target := base
	target.Config.Tools = []string{"tool-a"}
	probe.mu.Lock()
	probe.failAfter = fmt.Errorf("injected response loss after commit")
	probe.mu.Unlock()
	if _, _, _, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), base.Config.ID, "reconfigure", "", &target, runtimesessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("first reconfigure observed success despite injected response loss")
	}
	coordinator.mu.Lock()
	beforeRetry := runtimeeffects.LifecycleToken{
		RuntimeEpoch: coordinator.cells[base.Config.ID].epoch,
		AgentID:      base.Config.ID,
		Generation:   coordinator.cells[base.Config.ID].generation,
	}
	coordinator.mu.Unlock()
	if _, _, _, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), base.Config.ID, "reconfigure", "", &target, runtimesessions.LifecycleMutationPlan{}); err != nil {
		t.Fatalf("retry reconfigure: %v", err)
	}
	coordinator.mu.Lock()
	afterRetry := coordinator.cells[base.Config.ID].generation
	coordinator.mu.Unlock()
	if afterRetry != beforeRetry.Generation+1 {
		t.Fatalf("retry generation = %d, want committed successor %d", afterRetry, beforeRetry.Generation+1)
	}
	requests := probe.requestsFor("reconfigure")
	if len(requests) != 2 || requests[0].OperationID != requests[1].OperationID {
		t.Fatalf("response-loss retry requests = %#v, want one stable operation identity", requests)
	}
}

func TestLifecycleCoordinatorPersistenceFailureLeavesPriorGenerationOwned(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	loopCtx, token, done, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	probe.mu.Lock()
	probe.failNext = fmt.Errorf("injected persistence failure")
	probe.mu.Unlock()
	if _, _, _, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("restart succeeded despite persistence failure")
	}
	select {
	case <-loopCtx.Done():
		t.Fatal("persistence failure cancelled the prior generation")
	default:
	}
	current, ok := coordinator.token(rec.Config.ID)
	if !ok || current != token {
		t.Fatalf("current token = %+v ok=%v, want %+v", current, ok, token)
	}
	coordinator.cancelShutdownWork()
	if err := coordinator.releaseLoop(token, done); err != nil {
		t.Fatalf("release loop: %v", err)
	}
}

func TestLifecycleCoordinatorSpawnPersistenceFailurePublishesNoCell(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	probe.failNext = fmt.Errorf("injected spawn persistence failure")
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err == nil {
		t.Fatal("register succeeded despite persistence failure")
	}
	coordinator.mu.Lock()
	_, exists := coordinator.cells[rec.Config.ID]
	coordinator.mu.Unlock()
	if exists {
		t.Fatal("spawn persistence failure published a lifecycle cell")
	}
}

func TestLifecycleCoordinatorRecoveredGenerationZeroAdvancesFromDurableValue(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	epoch := runtimebus.CurrentRuntimeEpoch()
	probe.cell = lifecycleProbeCell{Epoch: epoch, Generation: 0, Phase: AgentLifecycleRegistered}
	probe.exists = true
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	rec := lifecycleTestPersistedAgent()
	rec.LifecycleEpoch = epoch
	rec.LifecycleGeneration = 0
	rec.LifecyclePhase = AgentLifecycleRegistered
	rec.LifecycleRunMode = AgentRunModeStopped
	if err := coordinator.registerExecution(testAuthorActivityContext(context.Background()), rec, false, reconfigureTestAgent{id: rec.Config.ID}); err != nil {
		t.Fatalf("register recovered agent: %v", err)
	}
	coordinator.beginRun(testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	loopCtx, token, done, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start recovered generation zero: %v", err)
	}
	if token.Generation != 1 {
		t.Fatalf("recovered generation = %d, want 1 after transition from durable zero", token.Generation)
	}
	coordinator.cancelShutdownWork()
	<-loopCtx.Done()
	if err := coordinator.releaseLoop(token, done); err != nil {
		t.Fatalf("release recovered loop: %v", err)
	}
}

func TestLifecycleCoordinatorInMemoryEffectContextCarriesCurrentToken(t *testing.T) {
	registry := runtimesessions.NewInMemoryRegistry(0)
	coordinator := newAgentLifecycleCoordinator(nil, registry)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.registerExecution(testAuthorActivityContext(context.Background()), rec, false, reconfigureTestAgent{id: rec.Config.ID}); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(managedExecutionTestContext(t, testAuthorActivityContext(context.Background())), AgentRunModeStandard)
	loopCtx, token, done, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	lease, err := coordinator.acquireExecution(testAuthorActivityContext(context.Background()), rec.Config.ID, "test_effect_context", true)
	if err != nil {
		t.Fatalf("acquireExecution: %v", err)
	}
	got, ok := runtimeeffects.LifecycleTokenFromContext(lease.Context)
	if !ok || got != token {
		t.Fatalf("effect token = %+v ok=%v, want %+v", got, ok, token)
	}
	if admission, ok := managedexecution.FromContext(lease.Context); !ok || !admission.AuthorizesNormal() {
		t.Fatalf("effect admission = %+v ok=%v, want normal runtime admission", admission, ok)
	}
	lease.Release()
	coordinator.cancelShutdownWork()
	<-loopCtx.Done()
	if err := coordinator.releaseLoop(token, done); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestLifecycleCoordinatorTeardownPersistenceFailureLeavesLoopOwned(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	loopCtx, token, done, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	probe.mu.Lock()
	probe.failNext = fmt.Errorf("injected teardown persistence failure")
	probe.mu.Unlock()
	if err := coordinator.terminate(testAuthorActivityContext(context.Background()), rec.Config.ID, "teardown", AgentLifecycleTerminated); err == nil {
		t.Fatal("teardown succeeded despite persistence failure")
	}
	select {
	case <-loopCtx.Done():
		t.Fatal("teardown persistence failure cancelled the current loop")
	default:
	}
	if current, ok := coordinator.token(rec.Config.ID); !ok || current != token {
		t.Fatalf("current token = %+v ok=%v, want %+v", current, ok, token)
	}
	coordinator.cancelShutdownWork()
	if err := coordinator.releaseLoop(token, done); err != nil {
		t.Fatalf("release loop: %v", err)
	}
}

func TestLifecycleCoordinatorRestartVersusTeardownNeverResurrectsLoop(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	initialCtx, initialToken, initialDone, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	go func() {
		<-initialCtx.Done()
		_ = coordinator.releaseLoop(initialToken, initialDone)
	}()

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		loopCtx, token, done, restartErr := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
		if restartErr == nil && loopCtx != nil {
			go func() {
				<-loopCtx.Done()
				_ = coordinator.releaseLoop(token, done)
			}()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		_ = coordinator.terminate(testAuthorActivityContext(context.Background()), rec.Config.ID, "teardown", AgentLifecycleTerminated)
	}()
	close(start)
	wg.Wait()

	if token, ok := coordinator.token(rec.Config.ID); ok {
		t.Fatalf("restart-versus-teardown left live token %+v", token)
	}
	coordinator.mu.Lock()
	cell := coordinator.cells[rec.Config.ID]
	var phase AgentLifecyclePhase
	var cancel context.CancelFunc
	var done chan struct{}
	if cell != nil {
		phase = cell.phase
		if cell.execution != nil {
			cancel, done = cell.execution.loopCancel, cell.execution.loopDone
		}
	}
	coordinator.mu.Unlock()
	if cell == nil || phase != AgentLifecycleTerminated || cancel != nil || done != nil {
		t.Fatalf("final lifecycle cell phase=%s cancel=%v done=%v, want terminated without loop owner", phase, cancel != nil, done != nil)
	}
}

func TestLifecycleCoordinatorSelfReleasePersistenceFailureFailsClosed(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	_, token, done, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	probe.mu.Lock()
	probe.failNext = fmt.Errorf("injected self-release persistence failure")
	probe.mu.Unlock()
	if err := coordinator.releaseLoop(token, done); err == nil {
		t.Fatal("self-release succeeded despite persistence failure")
	}
	if _, ok := coordinator.token(rec.Config.ID); ok {
		t.Fatal("failed self-release remained available as a running generation")
	}
	if _, _, _, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("restart admitted over failed self-release")
	}
}

func TestLifecycleCoordinatorConcurrentReplacementsCommitAdjacentGenerations(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	initialCtx, initialToken, initialDone, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("initial start: %v", err)
	}
	go func() {
		<-initialCtx.Done()
		_ = coordinator.releaseLoop(initialToken, initialDone)
	}()

	const replacements = 8
	var wg sync.WaitGroup
	generations := make(chan uint64, replacements)
	errs := make(chan error, replacements)
	for i := 0; i < replacements; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loopCtx, token, done, err := coordinator.replaceLoop(testAuthorActivityContext(context.Background()), rec.Config.ID, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
			if err != nil {
				errs <- err
				return
			}
			generations <- token.Generation
			go func() {
				<-loopCtx.Done()
				_ = coordinator.releaseLoop(token, done)
			}()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent replacement: %v", err)
	}
	close(generations)
	got := make([]int, 0, replacements)
	for generation := range generations {
		got = append(got, int(generation))
	}
	sort.Ints(got)
	for i, generation := range got {
		want := int(initialToken.Generation) + i + 1
		if generation != want {
			t.Fatalf("generation[%d] = %d, want adjacent %d; all=%v", i, generation, want, got)
		}
	}
	coordinator.beginShutdownAdmission()
	coordinator.cancelShutdownWork()
	coordinator.finishShutdown()
}

func lifecycleTestPersistedAgent() PersistedAgent {
	return PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "agent-lifecycle-test", Role: "worker", Type: "sonnet", Model: "regular", FlowID: "global"},
		Status: "active", HiredBy: "test", StartedAt: time.Now().UTC(),
	}
}
