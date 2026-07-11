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
	"github.com/google/uuid"
)

type lifecyclePersistenceProbe struct {
	mu         sync.Mutex
	cell       lifecycleProbeCell
	exists     bool
	operations map[string]AgentLifecycleTransitionResult
	failNext   error
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
	return result, nil
}

func TestLifecycleCoordinatorReplayDoesNotReplaceSuccessfulGeneration(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(context.Background(), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(context.Background(), AgentRunModeStandard)
	operationID := uuid.NewString()
	loopCtx, token, done, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "restart", operationID, nil)
	if err != nil || loopCtx == nil {
		t.Fatalf("first replacement ctx=%v token=%+v err=%v", loopCtx, token, err)
	}
	replayedCtx, replayedToken, replayedDone, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "restart", operationID, nil)
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

func TestLifecycleCoordinatorPersistenceFailureLeavesPriorGenerationOwned(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(context.Background(), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(context.Background(), AgentRunModeStandard)
	loopCtx, token, done, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "start", uuid.NewString(), nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	probe.mu.Lock()
	probe.failNext = fmt.Errorf("injected persistence failure")
	probe.mu.Unlock()
	if _, _, _, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "restart", uuid.NewString(), nil); err == nil {
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
	coordinator := newAgentLifecycleCoordinator(probe)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(context.Background(), rec, true); err == nil {
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
	coordinator := newAgentLifecycleCoordinator(probe)
	rec := lifecycleTestPersistedAgent()
	rec.LifecycleEpoch = epoch
	rec.LifecycleGeneration = 0
	rec.LifecyclePhase = AgentLifecycleRegistered
	rec.LifecycleRunMode = AgentRunModeStopped
	if err := coordinator.register(context.Background(), rec, false); err != nil {
		t.Fatalf("register recovered agent: %v", err)
	}
	coordinator.beginRun(context.Background(), AgentRunModeStandard)
	loopCtx, token, done, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "start", uuid.NewString(), nil)
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

func TestLifecycleCoordinatorTeardownPersistenceFailureLeavesLoopOwned(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(context.Background(), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(context.Background(), AgentRunModeStandard)
	loopCtx, token, done, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "start", uuid.NewString(), nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	probe.mu.Lock()
	probe.failNext = fmt.Errorf("injected teardown persistence failure")
	probe.mu.Unlock()
	if err := coordinator.terminate(context.Background(), rec.Config.ID, "teardown", AgentLifecycleTerminated); err == nil {
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
	coordinator := newAgentLifecycleCoordinator(probe)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(context.Background(), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(context.Background(), AgentRunModeStandard)
	initialCtx, initialToken, initialDone, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "start", uuid.NewString(), nil)
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
		loopCtx, token, done, restartErr := coordinator.replaceLoop(context.Background(), rec.Config.ID, "restart", uuid.NewString(), nil)
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
		_ = coordinator.terminate(context.Background(), rec.Config.ID, "teardown", AgentLifecycleTerminated)
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
		phase, cancel, done = cell.phase, cell.cancel, cell.done
	}
	coordinator.mu.Unlock()
	if cell == nil || phase != AgentLifecycleTerminated || cancel != nil || done != nil {
		t.Fatalf("final lifecycle cell phase=%s cancel=%v done=%v, want terminated without loop owner", phase, cancel != nil, done != nil)
	}
}

func TestLifecycleCoordinatorSelfReleasePersistenceFailureFailsClosed(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(context.Background(), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(context.Background(), AgentRunModeStandard)
	_, token, done, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "start", uuid.NewString(), nil)
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
	if _, _, _, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "restart", uuid.NewString(), nil); err == nil {
		t.Fatal("restart admitted over failed self-release")
	}
}

func TestLifecycleCoordinatorConcurrentReplacementsCommitAdjacentGenerations(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe)
	rec := lifecycleTestPersistedAgent()
	if err := coordinator.register(context.Background(), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	coordinator.beginRun(context.Background(), AgentRunModeStandard)
	initialCtx, initialToken, initialDone, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "start", uuid.NewString(), nil)
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
			loopCtx, token, done, err := coordinator.replaceLoop(context.Background(), rec.Config.ID, "restart", uuid.NewString(), nil)
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
		Config: runtimeactors.AgentConfig{ID: "agent-lifecycle-test", Role: "worker", Type: "sonnet", Model: "regular", Mode: "global"},
		Status: "active", HiredBy: "test", StartedAt: time.Now().UTC(),
	}
}
