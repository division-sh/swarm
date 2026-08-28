package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/google/uuid"
)

type runLifecycleCandidateInterceptStore struct {
	delegate runtimerunlifecycle.CandidateStore

	blockFirstList chan struct{}
	releaseList    chan struct{}
	executed       chan runtimerunlifecycle.Candidate
	listOnce       sync.Once
}

type runLifecycleSameRevisionInterceptStore struct {
	delegate       runtimerunlifecycle.CandidateStore
	firstStarted   chan struct{}
	releaseFirst   chan struct{}
	secondExecuted chan struct{}
	calls          int
	candidates     []runtimerunlifecycle.Candidate
	results        []runtimerunlifecycle.CompletionResult
	mu             sync.Mutex
}

type runLifecycleExecutorImmediateClock struct{}

func (runLifecycleExecutorImmediateClock) Now() time.Time {
	return time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
}

func (runLifecycleExecutorImmediateClock) After(time.Duration) <-chan time.Time {
	ready := make(chan time.Time, 1)
	ready <- time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	return ready
}

func (s *runLifecycleCandidateInterceptStore) ListCompletionCandidates(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	cursor runtimerunlifecycle.CandidateCursor,
	limit int,
) (runtimerunlifecycle.CandidatePage, error) {
	if s.blockFirstList != nil && cursor.RunID == "" {
		s.listOnce.Do(func() { close(s.blockFirstList) })
		select {
		case <-ctx.Done():
			return runtimerunlifecycle.CandidatePage{}, context.Cause(ctx)
		case <-s.releaseList:
		}
	}
	return s.delegate.ListCompletionCandidates(ctx, scope, cursor, limit)
}

func (s *runLifecycleCandidateInterceptStore) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate runtimerunlifecycle.Candidate,
	_ runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	select {
	case <-ctx.Done():
		return runtimerunlifecycle.CompletionResult{}, context.Cause(ctx)
	case s.executed <- candidate:
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeAwaitMutation}, nil
	}
}

func (s *runLifecycleSameRevisionInterceptStore) ListCompletionCandidates(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	cursor runtimerunlifecycle.CandidateCursor,
	limit int,
) (runtimerunlifecycle.CandidatePage, error) {
	return s.delegate.ListCompletionCandidates(ctx, scope, cursor, limit)
}

func (s *runLifecycleSameRevisionInterceptStore) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.candidates = append(s.candidates, candidate)
	s.mu.Unlock()
	if call == 1 {
		close(s.firstStarted)
		select {
		case <-ctx.Done():
			return runtimerunlifecycle.CompletionResult{}, context.Cause(ctx)
		case <-s.releaseFirst:
		}
		result := runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeAwaitMutation}
		s.mu.Lock()
		if len(s.results) < 16 {
			s.results = append(s.results, result)
		}
		s.mu.Unlock()
		return result, nil
	}
	result, err := s.delegate.ExecuteCompletionCandidate(ctx, candidate, catalog)
	s.mu.Lock()
	if len(s.results) < 16 {
		s.results = append(s.results, result)
	}
	s.mu.Unlock()
	if call == 2 {
		close(s.secondExecuted)
	}
	return result, err
}

func TestRunLifecycleSameRevisionCommittedHandoffParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			registrar := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
			owner := fixture.store.(runtimerunlifecycle.OperationOwner)
			ctx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC))
			if err := materializeCompletedRunEntityForTest(ctx, fixture.store, runID); err != nil {
				t.Fatalf("materialize terminal entity: %v", err)
			}
			if disposition, err := owner.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil || disposition != runtimerunlifecycle.CandidateRequested {
				t.Fatalf("request initial candidate = %s/%v", disposition, err)
			}

			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(ctx, occurrence)
			intercept := &runLifecycleSameRevisionInterceptStore{
				delegate:       fixture.store,
				firstStarted:   make(chan struct{}),
				releaseFirst:   make(chan struct{}),
				secondExecuted: make(chan struct{}),
			}
			executor := newRunLifecycleParityExecutorWithCatalog(
				t,
				intercept,
				occurrence,
				runtimerunlifecycle.NewTerminalCatalog(nil, map[string][]string{semanticRunFixtureFlow: {"completed"}}),
			)
			registration, err := registrar.RegisterCompletionCandidateSink(
				runtimeCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				executor,
			)
			if err != nil {
				t.Fatalf("register candidate executor: %v", err)
			}
			defer registration.Release()
			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}
			awaitRunLifecycleSignal(t, intercept.firstStarted, "first candidate execution")

			if disposition, err := owner.RequestCompletionCandidate(runtimeCtx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil || disposition != runtimerunlifecycle.CandidateAlreadyCurrent {
				t.Fatalf("request same-revision candidate = %s/%v", disposition, err)
			}
			close(intercept.releaseFirst)
			awaitRunLifecycleSignal(t, intercept.secondExecuted, "same-revision selected-store recheck")
			awaitRunLifecycleState(t, fixture.store, runID, runtimerunlifecycle.StateCompleted)

			intercept.mu.Lock()
			calls := intercept.calls
			intercept.mu.Unlock()
			if calls != 2 {
				t.Fatalf("candidate executions = %d, want 2", calls)
			}
			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func TestRunLifecyclePauseResumeSuccessorRaceParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			registrar := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
			owner := fixture.store.(runtimerunlifecycle.OperationOwner)
			ctx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			startedAt := time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC)
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, startedAt)
			if err := materializeCompletedRunEntityForTest(ctx, fixture.store, runID); err != nil {
				t.Fatalf("materialize terminal entity: %v", err)
			}
			if disposition, err := owner.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil || disposition != runtimerunlifecycle.CandidateRequested {
				t.Fatalf("request initial candidate = %s/%v", disposition, err)
			}

			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(ctx, occurrence)
			intercept := &runLifecycleSameRevisionInterceptStore{
				delegate:       fixture.store,
				firstStarted:   make(chan struct{}),
				releaseFirst:   make(chan struct{}),
				secondExecuted: make(chan struct{}),
			}
			executor := newRunLifecycleParityExecutorWithCatalog(
				t,
				intercept,
				occurrence,
				runtimerunlifecycle.NewTerminalCatalog(nil, map[string][]string{semanticRunFixtureFlow: {"completed"}}),
			)
			registration, err := registrar.RegisterCompletionCandidateSink(
				runtimeCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				executor,
			)
			if err != nil {
				t.Fatalf("register candidate executor: %v", err)
			}
			defer registration.Release()
			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}
			awaitRunLifecycleSignal(t, intercept.firstStarted, "pre-pause candidate execution")

			if disposition, err := transitionRunLifecycleParity(fixture, runtimeCtx, runID, runtimerunlifecycle.StatePaused); err != nil || disposition != runtimerunlifecycle.MutationApplied {
				t.Fatalf("pause run = %s/%v", disposition, err)
			}
			if disposition, err := transitionRunLifecycleParity(fixture, runtimeCtx, runID, runtimerunlifecycle.StateRunning); err != nil || disposition != runtimerunlifecycle.MutationApplied {
				t.Fatalf("resume run = %s/%v", disposition, err)
			}
			close(intercept.releaseFirst)
			awaitRunLifecycleSignal(t, intercept.secondExecuted, "post-resume successor execution")
			awaitRunLifecycleState(t, fixture.store, runID, runtimerunlifecycle.StateCompleted)

			intercept.mu.Lock()
			candidates := append([]runtimerunlifecycle.Candidate(nil), intercept.candidates...)
			intercept.mu.Unlock()
			if len(candidates) != 2 || candidates[1].Revision <= candidates[0].Revision {
				t.Fatalf("executed candidates = %#v, want serialized newer successor", candidates)
			}
			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func TestRunLifecycleTerminalClearDuringAttemptDoesNotReviveCandidateParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			registrar := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
			owner := fixture.store.(runtimerunlifecycle.OperationOwner)
			ctx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			startedAt := time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC)
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, startedAt)
			if disposition, err := owner.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil || disposition != runtimerunlifecycle.CandidateRequested {
				t.Fatalf("request initial candidate = %s/%v", disposition, err)
			}

			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(ctx, occurrence)
			intercept := &runLifecycleSameRevisionInterceptStore{
				delegate:       fixture.store,
				firstStarted:   make(chan struct{}),
				releaseFirst:   make(chan struct{}),
				secondExecuted: make(chan struct{}),
			}
			executor := newRunLifecycleParityExecutor(t, intercept, occurrence)
			registration, err := registrar.RegisterCompletionCandidateSink(
				runtimeCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				executor,
			)
			if err != nil {
				t.Fatalf("register candidate executor: %v", err)
			}
			defer registration.Release()
			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}
			awaitRunLifecycleSignal(t, intercept.firstStarted, "pre-terminal candidate execution")

			if _, disposition, err := terminalizeRunLifecycleCandidateParity(
				fixture,
				runtimeCtx,
				runID,
				runtimerunlifecycle.StateCancelled,
				startedAt.Add(time.Minute),
			); err != nil || disposition != runtimerunlifecycle.MutationApplied {
				t.Fatalf("terminalize run = %s/%v", disposition, err)
			}
			close(intercept.releaseFirst)
			awaitRunLifecycleExecutorCandidates(t, executor, 0)
			select {
			case <-intercept.secondExecuted:
				t.Fatal("terminal clearing started a successor candidate execution")
			default:
			}
			state, duePresent, _ := loadRunLifecycleCandidateFacts(t, fixture, ctx, runID)
			if state != string(runtimerunlifecycle.StateCancelled) || duePresent {
				t.Fatalf("terminal candidate state = state:%s due:%v, want cancelled/false", state, duePresent)
			}
			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func TestRunLifecycleCrossBundleSameRevisionHandoffParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			registrar := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
			owner := fixture.store.(runtimerunlifecycle.OperationOwner)
			ctx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC))
			if err := materializeCompletedRunEntityForTest(ctx, fixture.store, runID); err != nil {
				t.Fatalf("materialize terminal entity: %v", err)
			}
			if disposition, err := owner.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil || disposition != runtimerunlifecycle.CandidateRequested {
				t.Fatalf("request initial candidate = %s/%v", disposition, err)
			}

			process := worklifetime.NewProcess()
			oldOccurrence := newRunLifecycleExecutorOccurrenceForBundle(t, process, runLifecycleCandidateParityBundleHash)
			newOccurrence := newRunLifecycleExecutorOccurrenceForBundle(t, process, runLifecycleCandidateParityReplacementHash)
			oldCtx := worklifetime.WithRuntimeOccurrence(ctx, oldOccurrence)
			intercept := &runLifecycleSameRevisionInterceptStore{
				delegate:       fixture.store,
				firstStarted:   make(chan struct{}),
				releaseFirst:   make(chan struct{}),
				secondExecuted: make(chan struct{}),
			}
			catalog := runtimerunlifecycle.NewTerminalCatalog(nil, map[string][]string{semanticRunFixtureFlow: {"completed"}})
			oldExecutor := newRunLifecycleParityExecutorForScope(t, intercept, oldOccurrence, runLifecycleCandidateParityBundleHash, catalog)
			newExecutor := newRunLifecycleParityExecutorForScope(t, intercept, newOccurrence, runLifecycleCandidateParityReplacementHash, catalog)
			oldRegistration, err := registrar.RegisterCompletionCandidateSink(
				oldCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				oldExecutor,
			)
			if err != nil {
				t.Fatalf("register old-bundle candidate executor: %v", err)
			}
			defer oldRegistration.Release()
			newCtx := worklifetime.WithRuntimeOccurrence(ctx, newOccurrence)
			newRegistration, err := registrar.RegisterCompletionCandidateSink(
				newCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityReplacementHash},
				newExecutor,
			)
			if err != nil {
				t.Fatalf("register new-bundle candidate executor: %v", err)
			}
			defer newRegistration.Release()
			if err := oldExecutor.Start(oldCtx); err != nil {
				t.Fatalf("start old-bundle executor: %v", err)
			}
			if err := newExecutor.Start(newCtx); err != nil {
				t.Fatalf("start new-bundle executor: %v", err)
			}
			awaitRunLifecycleSignal(t, intercept.firstStarted, "old-bundle candidate execution")

			replacement, err := runtimecorrelation.NewEphemeralBundleSourceFact(runLifecycleCandidateParityReplacementHash)
			if err != nil {
				t.Fatal(err)
			}
			if disposition, err := reviseRunLifecycleSourceParity(fixture, oldCtx, runID, replacement); err != nil || disposition != runtimerunlifecycle.MutationApplied {
				t.Fatalf("revise run source = %s/%v", disposition, err)
			}
			awaitRunLifecycleSignal(t, intercept.secondExecuted, "new-bundle same-revision execution")
			awaitRunLifecycleState(t, fixture.store, runID, runtimerunlifecycle.StateCompleted)
			close(intercept.releaseFirst)
			awaitRunLifecycleExecutorCandidates(t, oldExecutor, 0)

			intercept.mu.Lock()
			candidates := append([]runtimerunlifecycle.Candidate(nil), intercept.candidates...)
			intercept.mu.Unlock()
			if len(candidates) != 2 || candidates[0].Revision != candidates[1].Revision ||
				candidates[0].BundleHash == candidates[1].BundleHash ||
				candidates[1].BundleHash != runLifecycleCandidateParityReplacementHash {
				t.Fatalf("cross-bundle candidate identities = %#v", candidates)
			}
			if err := oldExecutor.Retire(context.Background()); err != nil {
				t.Fatalf("retire old-bundle executor: %v", err)
			}
			if err := newExecutor.Retire(context.Background()); err != nil {
				t.Fatalf("retire new-bundle executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, oldOccurrence)
			retireRunLifecycleExecutorOccurrence(t, newOccurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func TestRunLifecycleServedDeliverySameRevisionHandoffParity(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			registrar := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
			ctx := testAuthorActivityContext()
			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(ctx, occurrence)
			intercept := &runLifecycleSameRevisionInterceptStore{
				delegate:       fixture.store.(runtimerunlifecycle.CandidateStore),
				firstStarted:   make(chan struct{}),
				releaseFirst:   make(chan struct{}),
				secondExecuted: make(chan struct{}),
			}
			executor, err := runtimerunlifecycle.NewExecutor(
				intercept,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				runtimerunlifecycle.TerminalCatalog{},
				occurrence,
				runtimerunlifecycle.ExecutorOptions{},
			)
			if err != nil {
				t.Fatalf("create candidate executor: %v", err)
			}
			registration, err := registrar.RegisterCompletionCandidateSink(
				runtimeCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				executor,
			)
			if err != nil {
				t.Fatalf("register candidate executor: %v", err)
			}
			defer registration.Release()
			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}

			eventBus, err := newRunConvergenceEventBus(t, fixture.store)
			if err != nil {
				t.Fatalf("create EventBus: %v", err)
			}
			agentID := "candidate-handoff-served-agent"
			agentIdentity := runtimebustest.Identity(t, agentID, "")
			seedStandaloneConvergenceAgent(t, fixture.store, runtimeCtx, agentIdentity)
			event := eventtest.RuntimeControl(
				uuid.NewString(),
				"platform.boot",
				"runtime",
				"",
				json.RawMessage(`{}`),
				0,
				"",
				"",
				events.EventEnvelope{},
				time.Date(2025, 7, 29, 12, 0, 0, 0, time.UTC),
			)
			delivery := runtimebustest.Subscribe(t, eventBus, agentID, event.Type())
			defer runtimebustest.Unsubscribe(eventBus, agentID)
			if err := eventBus.Publish(runtimeCtx, event); err != nil {
				t.Fatalf("publish routed standalone event: %v", err)
			}
			awaitRunLifecycleSignal(t, intercept.firstStarted, "pre-settlement candidate execution")
			var delivered events.Event
			select {
			case local := <-delivery:
				delivered = local.Event()
				_ = local.Complete()
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for routed standalone delivery")
			}
			route := events.DeliveryRoute{
				Recipient:     events.MustAgentDeliveryRecipient(agentID),
				AgentIdentity: agentIdentity,
			}
			claimed, err := claimDeliveryFixture(runtimeCtx, fixture.store, delivered, route)
			if err != nil {
				t.Fatalf("claim routed standalone delivery: %v", err)
			}
			if _, err := fixture.store.SettleSuccess(
				runtimeCtx,
				claimed.Claim,
				nil,
				0,
				runtimedelivery.NotApplicableHandlerRuleSelection(),
			); err != nil {
				t.Fatalf("settle routed standalone delivery: %v", err)
			}
			close(intercept.releaseFirst)
			awaitRunLifecycleSignal(t, intercept.secondExecuted, "post-settlement candidate execution")
			stateDeadline := time.Now().Add(5 * time.Second)
			for {
				snapshot, loadErr := fixture.store.(runLifecycleCandidateParityStore).LoadRunLifecycleSnapshot(context.Background(), delivered.RunID())
				if loadErr == nil && snapshot.Status == string(runtimerunlifecycle.StateCompleted) {
					break
				}
				if time.Now().After(stateDeadline) {
					intercept.mu.Lock()
					results := append([]runtimerunlifecycle.CompletionResult(nil), intercept.results...)
					intercept.mu.Unlock()
					t.Fatalf("served run state = %#v/%v, want completed; candidate results = %#v", snapshot, loadErr, results)
				}
				runtime.Gosched()
			}

			intercept.mu.Lock()
			candidates := append([]runtimerunlifecycle.Candidate(nil), intercept.candidates...)
			intercept.mu.Unlock()
			if len(candidates) != 2 || !candidates[0].SameIdentity(candidates[1]) {
				t.Fatalf("served candidate executions = %#v, want two exact same-revision identities", candidates)
			}
			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func TestRunLifecycleExecutorNoGapParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			registrar, ok := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
			if !ok {
				t.Fatalf("%s store does not expose candidate registration", backend)
			}
			baseCtx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			startedAt := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC)
			ensureRunLifecycleCandidateParityRun(t, fixture, baseCtx, runID, startedAt)
			if got, err := transitionRunLifecycleParity(
				fixture, baseCtx, runID, runtimerunlifecycle.StatePaused,
			); err != nil || got != runtimerunlifecycle.MutationApplied {
				t.Fatalf("prepare paused run = %s/%v", got, err)
			}

			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(baseCtx, occurrence)
			intercept := &runLifecycleCandidateInterceptStore{
				delegate:       fixture.store,
				blockFirstList: make(chan struct{}),
				releaseList:    make(chan struct{}),
				executed:       make(chan runtimerunlifecycle.Candidate, 4),
			}
			executor := newRunLifecycleParityExecutor(t, intercept, occurrence)
			registration, err := registrar.RegisterCompletionCandidateSink(
				runtimeCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				executor,
			)
			if err != nil {
				t.Fatalf("register candidate executor: %v", err)
			}

			started := make(chan error, 1)
			go func() { started <- executor.Start(runtimeCtx) }()
			awaitRunLifecycleSignal(t, intercept.blockFirstList, "startup enumeration")

			if got, err := transitionRunLifecycleParity(
				fixture, runtimeCtx, runID, runtimerunlifecycle.StateRunning,
			); err != nil || got != runtimerunlifecycle.MutationApplied {
				t.Fatalf("resume during startup enumeration = %s/%v", got, err)
			}
			first := awaitRunLifecycleCandidate(t, intercept.executed, "pre-readiness candidate handoff")
			if first.RunID != runID {
				t.Fatalf("pre-readiness candidate run_id = %s, want %s", first.RunID, runID)
			}
			close(intercept.releaseList)
			if err := awaitRunLifecycleError(t, started, "candidate executor readiness"); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}
			if !executor.Ready() {
				t.Fatal("candidate executor did not publish readiness after complete enumeration")
			}

			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)

			if got, err := transitionRunLifecycleParity(
				fixture, baseCtx, runID, runtimerunlifecycle.StatePaused,
			); err != nil || got != runtimerunlifecycle.MutationApplied {
				t.Fatalf("pause after retirement = %s/%v", got, err)
			}
			if got, err := transitionRunLifecycleParity(
				fixture, baseCtx, runID, runtimerunlifecycle.StateRunning,
			); !errors.Is(err, worklifetime.ErrRetired) || got != "" {
				t.Fatalf("post-retirement resume = %s/%v, want retired rejection", got, err)
			}
			state, duePresent, _ := loadRunLifecycleCandidateFacts(t, fixture, baseCtx, runID)
			if state != string(runtimerunlifecycle.StatePaused) || duePresent {
				t.Fatalf("post-retirement rollback = state:%s due:%v", state, duePresent)
			}
			registration.Release()

			if got, err := transitionRunLifecycleParity(
				fixture, baseCtx, runID, runtimerunlifecycle.StateRunning,
			); err != nil || got != runtimerunlifecycle.MutationApplied {
				t.Fatalf("durable successor handoff = %s/%v", got, err)
			}
			successorProcess := worklifetime.NewProcess()
			successorOccurrence := newRunLifecycleExecutorOccurrence(t, successorProcess)
			successorCtx := worklifetime.WithRuntimeOccurrence(baseCtx, successorOccurrence)
			successorStore := &runLifecycleCandidateInterceptStore{
				delegate: fixture.store,
				executed: make(chan runtimerunlifecycle.Candidate, 2),
			}
			successor := newRunLifecycleParityExecutor(t, successorStore, successorOccurrence)
			successorRegistration, err := registrar.RegisterCompletionCandidateSink(
				successorCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				successor,
			)
			if err != nil {
				t.Fatalf("register successor candidate executor: %v", err)
			}
			if err := successor.Start(successorCtx); err != nil {
				t.Fatalf("start successor candidate executor: %v", err)
			}
			recovered := awaitRunLifecycleCandidate(t, successorStore.executed, "successor startup rehydration")
			if recovered.RunID != runID {
				t.Fatalf("successor candidate run_id = %s, want %s", recovered.RunID, runID)
			}
			if err := successor.Retire(context.Background()); err != nil {
				t.Fatalf("retire successor candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, successorOccurrence)
			successorRegistration.Release()
			retireRunLifecycleProcess(t, process)
			retireRunLifecycleProcess(t, successorProcess)
		})
	}
}

func TestRunLifecycleDirectHandoffCommitAcrossSinkRegistrationParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			registrar, ok := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
			if !ok {
				t.Fatalf("%s store does not expose candidate registration", backend)
			}
			baseCtx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(
				t,
				fixture,
				baseCtx,
				runID,
				time.Date(2026, 7, 29, 16, 45, 0, 0, time.UTC),
			)

			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(baseCtx, occurrence)
			intercept := &runLifecycleCandidateInterceptStore{
				delegate: fixture.store,
				executed: make(chan runtimerunlifecycle.Candidate, 2),
			}
			executor := newRunLifecycleParityExecutor(t, intercept, occurrence)
			registry := candidateRegistryForFixture(t, fixture)

			candidatePrepared := make(chan struct{})
			allowCommit := make(chan struct{})
			mutationDone := make(chan error, 1)
			go func() {
				handoff, err := reserveRunLifecycleCandidateHandoff(runtimeCtx)
				if err != nil {
					mutationDone <- err
					return
				}
				defer handoff.Rollback()
				var tx *sql.Tx
				switch store := fixture.store.(type) {
				case *PostgresStore:
					tx, err = store.backend.BeginTx(runtimeCtx, nil)
				case *SQLiteRuntimeStore:
					tx, err = store.backend.BeginTx(runtimeCtx, nil)
				default:
					err = errors.New("unsupported direct candidate handoff store")
				}
				if err != nil {
					mutationDone <- err
					return
				}
				defer func() { _ = tx.Rollback() }()
				var result runtimerunlifecycle.CandidateRequestResult
				switch store := fixture.store.(type) {
				case *PostgresStore:
					result, err = requestPostgresCompletionCandidateTx(runtimeCtx, tx, runID, nil, false)
				case *SQLiteRuntimeStore:
					result, err = requestSQLiteCompletionCandidateTx(runtimeCtx, tx, runID, nil, store.now(), false)
				}
				if err == nil {
					err = handoff.Prepare(registry, result)
				}
				if err != nil {
					mutationDone <- err
					return
				}
				close(candidatePrepared)
				select {
				case <-runtimeCtx.Done():
					mutationDone <- context.Cause(runtimeCtx)
					return
				case <-allowCommit:
				}
				if err := tx.Commit(); err != nil {
					mutationDone <- err
					return
				}
				mutationDone <- handoff.Commit()
			}()
			awaitRunLifecycleSignal(t, candidatePrepared, "uncommitted direct candidate preparation")

			type registrationResult struct {
				registration runtimerunlifecycle.CandidateRegistration
				err          error
			}
			registered := make(chan registrationResult, 1)
			go func() {
				registration, err := registrar.RegisterCompletionCandidateSink(
					runtimeCtx,
					runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
					executor,
				)
				registered <- registrationResult{registration: registration, err: err}
			}()
			awaitRunLifecycleSinkInstallation(t, fixture, executor)
			select {
			case result := <-registered:
				if result.registration != nil {
					result.registration.Release()
				}
				t.Fatalf("registration returned before the direct candidate transaction settled: %v", result.err)
			default:
			}

			close(allowCommit)
			if err := awaitRunLifecycleError(t, mutationDone, "direct candidate commit"); err != nil {
				t.Fatalf("commit direct candidate across sink registration: %v", err)
			}
			result := <-registered
			if result.err != nil {
				t.Fatalf("register candidate executor: %v", result.err)
			}
			defer result.registration.Release()

			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}
			candidate := awaitRunLifecycleCandidate(t, intercept.executed, "direct post-registration reconciliation")
			if candidate.RunID != runID {
				t.Fatalf("reconciled direct candidate run_id = %s, want %s", candidate.RunID, runID)
			}

			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func TestPostgresPipelineCompletionHandoffSurvivesPostCommitCleanupError(t *testing.T) {
	for _, operation := range []string{"mark_decision_processed", "settle"} {
		operation := operation
		t.Run(operation, func(t *testing.T) {
			fixture := openPostgresAuthorActivityReceiptFixture(t)
			selected, ok := fixture.store.(*PostgresStore)
			if !ok {
				t.Fatalf("fixture store = %T, want *PostgresStore", fixture.store)
			}
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))
			owner := selected.PipelineObligations()

			var claim runtimepipelineobligation.Claim
			var settlementOutcome runtimepipelineobligation.SettlementOutcome
			switch operation {
			case "mark_decision_processed":
				insertProducerIdentityDecisionObligation(t, fixture, ctx, eventID, runID, time.Now().UTC().Add(-time.Minute))
				work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeDecisionRoute)
				if err != nil {
					t.Fatalf("claim decision route: %v", err)
				}
				claim = work.Claim
			case "settle":
				work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
				if err != nil {
					t.Fatalf("claim recovery work: %v", err)
				}
				claim = work.Claim
			default:
				t.Fatalf("unsupported operation %q", operation)
			}

			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(ctx, occurrence)
			intercept := &runLifecycleCandidateInterceptStore{
				delegate: selected,
				executed: make(chan runtimerunlifecycle.Candidate, 2),
			}
			executor := newRunLifecycleParityExecutor(t, intercept, occurrence)
			registration, err := selected.RegisterCompletionCandidateSink(
				runtimeCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				executor,
			)
			if err != nil {
				t.Fatalf("register candidate executor: %v", err)
			}
			defer registration.Release()
			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}

			state, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claim)
			if err != nil {
				t.Fatalf("load pipeline claim state: %v", err)
			}
			injectedErr := errors.New("injected post-commit session cleanup failure")
			session := state.LeaseForTest().Session()
			session.SetEndTxErrorForTest(func() error { return injectedErr })

			switch operation {
			case "mark_decision_processed":
				err = owner.MarkDecisionProcessed(runtimeCtx, claim)
			case "settle":
				settlementOutcome, err = owner.Settle(runtimeCtx, claim, runtimepipelineobligation.Acknowledged("processed"))
			}
			session.SetEndTxErrorForTest(nil)
			if !errors.Is(err, injectedErr) {
				t.Fatalf("%s error = %v, want injected cleanup failure", operation, err)
			}
			if operation == "settle" && (!settlementOutcome.Committed() || !settlementOutcome.DeliveryHandoffCommitted()) {
				t.Fatalf("settlement outcome after cleanup failure = committed:%v handoff:%v, want true/true",
					settlementOutcome.Committed(), settlementOutcome.DeliveryHandoffCommitted())
			}

			candidate := awaitRunLifecycleCandidate(t, intercept.executed, operation+" live candidate handoff")
			if candidate.RunID != runID {
				t.Fatalf("%s candidate run_id = %s, want %s", operation, candidate.RunID, runID)
			}
			lifecycleFixture := runLifecycleCandidateParityFixture{
				store: selected, db: fixture.db, postgres: true,
			}
			stateName, duePresent, revision := loadRunLifecycleCandidateFacts(
				t, lifecycleFixture, ctx, runID,
			)
			if stateName != string(runtimerunlifecycle.StateRunning) || !duePresent || revision != candidate.Revision {
				t.Fatalf(
					"%s durable candidate = state:%s due:%v revision:%d, want running/true/%d",
					operation, stateName, duePresent, revision, candidate.Revision,
				)
			}

			if operation == "settle" {
				if _, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery); !errors.Is(err, runtimepipelineobligation.ErrIneligible) {
					t.Fatalf("settled claim remained eligible after cleanup failure: %v", err)
				}
			} else {
				count, outcome, reason := readExactPipelineReceipt(t, ctx, fixture, eventID)
				if count != 1 || outcome != "success" || reason != "decision_route_processed" {
					t.Fatalf(
						"decision route receipt = count:%d outcome:%q reason:%q",
						count, outcome, reason,
					)
				}
				if err := owner.Release(ctx, claim); err != nil {
					t.Fatalf("release processed decision claim: %v", err)
				}
			}

			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func candidateRegistryForFixture(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
) *storerunhandoff.CandidateCoordinator {
	t.Helper()
	switch store := fixture.store.(type) {
	case *PostgresStore:
		return store.runLifecycleCandidates
	case *SQLiteRuntimeStore:
		return store.runLifecycleCandidates
	default:
		t.Fatal("unsupported candidate registry store")
		return nil
	}
}

func awaitRunLifecycleSinkInstallation(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	sink runtimerunlifecycle.CandidateSink,
) {
	t.Helper()
	registry := candidateRegistryForFixture(t, fixture)
	deadline := time.Now().Add(5 * time.Second)
	for {
		installed := registry.Registered(runLifecycleCandidateParityBundleHash, sink)
		if installed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for candidate sink installation")
		}
		runtime.Gosched()
	}
}

func newRunLifecycleExecutorOccurrence(
	t *testing.T,
	process *worklifetime.Process,
) *worklifetime.RuntimeOccurrence {
	return newRunLifecycleExecutorOccurrenceForBundle(t, process, runLifecycleCandidateParityBundleHash)
}

func newRunLifecycleExecutorOccurrenceForBundle(
	t *testing.T,
	process *worklifetime.Process,
	bundleHash string,
) *worklifetime.RuntimeOccurrence {
	t.Helper()
	occurrence, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: uuid.NewString(),
		BundleHash:        bundleHash,
	})
	if err != nil {
		t.Fatalf("create candidate runtime occurrence: %v", err)
	}
	return occurrence
}

func newRunLifecycleParityExecutor(
	t *testing.T,
	store runtimerunlifecycle.CandidateStore,
	occurrence *worklifetime.RuntimeOccurrence,
) *runtimerunlifecycle.Executor {
	return newRunLifecycleParityExecutorWithCatalog(t, store, occurrence, runtimerunlifecycle.TerminalCatalog{})
}

func newRunLifecycleParityExecutorWithCatalog(
	t *testing.T,
	store runtimerunlifecycle.CandidateStore,
	occurrence *worklifetime.RuntimeOccurrence,
	catalog runtimerunlifecycle.TerminalCatalog,
) *runtimerunlifecycle.Executor {
	return newRunLifecycleParityExecutorForScope(t, store, occurrence, runLifecycleCandidateParityBundleHash, catalog)
}

func newRunLifecycleParityExecutorForScope(
	t *testing.T,
	store runtimerunlifecycle.CandidateStore,
	occurrence *worklifetime.RuntimeOccurrence,
	bundleHash string,
	catalog runtimerunlifecycle.TerminalCatalog,
) *runtimerunlifecycle.Executor {
	t.Helper()
	executor, err := runtimerunlifecycle.NewExecutor(
		store,
		runtimerunlifecycle.CandidateScope{BundleHash: bundleHash},
		catalog,
		occurrence,
		runtimerunlifecycle.ExecutorOptions{Clock: runLifecycleExecutorImmediateClock{}},
	)
	if err != nil {
		t.Fatalf("create candidate executor: %v", err)
	}
	return executor
}

func awaitRunLifecycleState(
	t *testing.T,
	store runLifecycleCandidateParityStore,
	runID string,
	want runtimerunlifecycle.State,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot, err := store.LoadRunLifecycleSnapshot(context.Background(), runID)
		if err == nil && snapshot.Status == string(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run state = %#v/%v, want %s", snapshot, err, want)
		}
		runtime.Gosched()
	}
}

func awaitRunLifecycleExecutorCandidates(
	t *testing.T,
	executor *runtimerunlifecycle.Executor,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for executor.ActiveCandidates() != want {
		if time.Now().After(deadline) {
			t.Fatalf("active lifecycle candidates = %d, want %d", executor.ActiveCandidates(), want)
		}
		runtime.Gosched()
	}
}

func retireRunLifecycleExecutorOccurrence(t *testing.T, occurrence *worklifetime.RuntimeOccurrence) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := occurrence.RetireAndWait(ctx); err != nil {
		t.Fatalf("retire candidate runtime occurrence: %v", err)
	}
}

func retireRunLifecycleProcess(t *testing.T, process *worklifetime.Process) {
	t.Helper()
	process.Retire()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := process.Join(ctx); err != nil {
		t.Fatalf("join candidate process owner: %v", err)
	}
}

func awaitRunLifecycleSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func awaitRunLifecycleCandidate(
	t *testing.T,
	ch <-chan runtimerunlifecycle.Candidate,
	label string,
) runtimerunlifecycle.Candidate {
	t.Helper()
	select {
	case candidate := <-ch:
		return candidate
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		return runtimerunlifecycle.Candidate{}
	}
}

func awaitRunLifecycleError(t *testing.T, ch <-chan error, label string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		return nil
	}
}
