package serveapp

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type receiverCompletionResult struct {
	candidate runlifecycle.Candidate
	result    runlifecycle.CompletionResult
	err       error
}

// Observe the real candidate owner and executor; do not replace their decisions
// or stop completion work to manufacture an exclusive diagnostic writer.
type receiverCompletionBoundary struct {
	runlifecycle.CandidateOwner
	sourceOnce sync.Once
	mu         sync.Mutex
	executor   *runlifecycle.Executor
	pending    int
	executing  int
	version    uint64
	changed    chan struct{}
	results    []receiverCompletionResult
}

func installReceiverCompletionBoundary(t *testing.T) *receiverCompletionBoundary {
	t.Helper()
	boundary := &receiverCompletionBoundary{changed: make(chan struct{})}
	previous := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		persistence := previous(owner)
		boundary.sourceOnce.Do(func() { boundary.CandidateOwner = persistence.deps.RunLifecycleCandidates })
		persistence.deps.RunLifecycleCandidates = boundary
		return persistence
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
	return boundary
}

func (b *receiverCompletionBoundary) notifyLocked() {
	b.version++
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *receiverCompletionBoundary) RegisterCompletionCandidateSink(ctx context.Context, scope runlifecycle.CandidateScope, sink runlifecycle.CandidateSink) (runlifecycle.CandidateRegistration, error) {
	executor, ok := sink.(*runlifecycle.Executor)
	if !ok {
		return nil, fmt.Errorf("receiver completion boundary requires the live executor, got %T", sink)
	}
	b.mu.Lock()
	b.executor = executor
	b.mu.Unlock()
	return b.CandidateOwner.RegisterCompletionCandidateSink(ctx, scope, receiverCompletionSink{boundary: b, sink: sink})
}

type receiverCompletionSink struct {
	boundary *receiverCompletionBoundary
	sink     runlifecycle.CandidateSink
}

func (s receiverCompletionSink) ReserveCompletionCandidate(ctx context.Context) (runlifecycle.CandidateAdmission, error) {
	s.boundary.mu.Lock()
	s.boundary.pending++
	s.boundary.notifyLocked()
	s.boundary.mu.Unlock()
	admission, err := s.sink.ReserveCompletionCandidate(ctx)
	if err != nil {
		s.boundary.finishReservation()
		return nil, err
	}
	return &receiverCompletionAdmission{CandidateAdmission: admission, boundary: s.boundary}, nil
}

type receiverCompletionAdmission struct {
	runlifecycle.CandidateAdmission
	boundary *receiverCompletionBoundary
	once     sync.Once
}

func (a *receiverCompletionAdmission) Submit(candidate runlifecycle.Candidate) error {
	err := a.CandidateAdmission.Submit(candidate)
	a.once.Do(a.boundary.finishReservation)
	return err
}

func (a *receiverCompletionAdmission) Cancel() error {
	err := a.CandidateAdmission.Cancel()
	a.once.Do(a.boundary.finishReservation)
	return err
}

func (b *receiverCompletionBoundary) finishReservation() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending--
	b.notifyLocked()
}

func (b *receiverCompletionBoundary) ExecuteCompletionCandidate(ctx context.Context, candidate runlifecycle.Candidate, catalog runlifecycle.FinalCatalog) (runlifecycle.CompletionResult, error) {
	b.mu.Lock()
	b.executing++
	b.notifyLocked()
	b.mu.Unlock()
	result, err := b.CandidateOwner.ExecuteCompletionCandidate(ctx, candidate, catalog)
	b.mu.Lock()
	b.results = append(b.results, receiverCompletionResult{candidate: candidate, result: result, err: err})
	b.executing--
	b.notifyLocked()
	b.mu.Unlock()
	return result, err
}

func (b *receiverCompletionBoundary) wait(ctx context.Context, bundleHash string) ([]receiverCompletionResult, error) {
	// The predicate is exact owner accounting, not an arbitrary quiet interval.
	// The executor removes its chain just after the observed native return.
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		b.mu.Lock()
		version, idle, changed, executor := b.version, b.pending == 0 && b.executing == 0, b.changed, b.executor
		b.mu.Unlock()
		if idle && executor != nil && executor.ActiveCandidates() == 0 {
			page, err := b.ListCompletionCandidates(ctx, runlifecycle.CandidateScope{BundleHash: bundleHash}, runlifecycle.CandidateCursor{}, 128)
			if err != nil {
				return nil, err
			}
			b.mu.Lock()
			settled := version == b.version && b.pending == 0 && b.executing == 0 && page.Exhausted && len(page.Candidates) == 0 && executor.ActiveCandidates() == 0
			results := append([]receiverCompletionResult(nil), b.results...)
			b.mu.Unlock()
			if settled {
				for _, receipt := range results {
					if receipt.err != nil || !receipt.result.Committed || receipt.result.Validate() != nil || receipt.result.Outcome == runlifecycle.OutcomeRetryCurrent || receipt.result.Outcome == runlifecycle.OutcomeRearmAt {
						return nil, fmt.Errorf("completion boundary contains unsettled result: candidate=%+v result=%+v err=%v", receipt.candidate, receipt.result, receipt.err)
					}
				}
				return results, nil
			}
		}
		select {
		case <-changed:
		case <-tick.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func requireReceiverCompletionBoundary(t *testing.T, ctx context.Context, boundary *receiverCompletionBoundary, bundleHash, seedRun string) []receiverCompletionResult {
	t.Helper()
	results, err := boundary.wait(ctx, bundleHash)
	if err != nil {
		t.Fatalf("join receiver completion writers before conservation snapshot: %v", err)
	}
	for _, receipt := range results {
		seed := receipt.candidate.RunID == seedRun
		t.Logf("completion writer settled before held-origin snapshot: seed=%t run=%s revision=%d due=%s outcome=%s committed=%t", seed, receipt.candidate.RunID, receipt.candidate.Revision, receipt.candidate.DueAt.Format(time.RFC3339Nano), receipt.result.Outcome, receipt.result.Committed)
	}
	return results
}
