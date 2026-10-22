package runlifecycle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func TestExecutorPreparedHandoffDoesNotExecuteBeforeStart(t *testing.T) {
	var executions atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, FinalCatalog) (CompletionResult, error) {
			executions.Add(1)
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})
	if err := executor.SubmitCompletionCandidate(context.Background(), executorTestCandidate(1)); err != nil {
		t.Fatal(err)
	}
	if executor.ActiveCandidates() != 1 || executor.Ready() {
		t.Fatal("prepared handoff lost durable representation or released execution")
	}
	if err := executor.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := executor.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	retireRuntimeOccurrence(t, occurrence)
	if executions.Load() != 0 || executor.ActiveCandidates() != 0 {
		t.Fatal("prepared or rejected activation executed completion work")
	}
}

func TestExecutorSelectedScopeAndBoundAuthority(t *testing.T) {
	candidate := executorTestCandidate(1)
	candidate.SelectedForkRunID = candidate.RunID
	executed := make(chan Candidate, 1)
	store := &executorTestStore{
		list: func(_ context.Context, scope CandidateScope, _ CandidateCursor, _ int) (CandidatePage, error) {
			if !scope.MatchesCandidate(candidate) {
				t.Error("startup scan lost exact selected scope")
			}
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(ctx context.Context, got Candidate, _ FinalCatalog) (CompletionResult, error) {
			authority, found := effects.AuthorityFromContext(ctx)
			if !found || authority.SelectedFork.ForkRunID != candidate.RunID || authority.SelectedFork.ExecutionID != "22222222-2222-4222-8222-222222222222" {
				t.Error("selected executor borrowed handoff caller authority")
			}
			executed <- got
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	executor, occurrence := newSelectedExecutorTestSubject(t, store, candidate)
	for _, change := range []func(*Candidate){
		func(c *Candidate) { c.SelectedForkRunID = "" },
		func(c *Candidate) { c.RunID = "33333333-3333-4333-8333-333333333333"; c.SelectedForkRunID = c.RunID },
		func(c *Candidate) { c.BundleHash = "foreign-bundle" },
	} {
		other := candidate
		change(&other)
		if err := executor.SubmitCompletionCandidate(context.Background(), other); err == nil {
			t.Fatal("selected sink accepted ordinary, sibling or foreign candidate")
		}
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	if err := executor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := receiveValue(t, executed, "selected candidate"); !got.SameIdentity(candidate) {
		t.Fatalf("selected execution changed identity: %+v", got)
	}
	if err := executor.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := executor.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := occurrence.RetireAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestExecutorStaleAuthorityDoesNotRetry(t *testing.T) {
	candidate := executorTestCandidate(1)
	candidate.SelectedForkRunID = candidate.RunID
	var attempts atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Candidates: []Candidate{candidate}, Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, FinalCatalog) (CompletionResult, error) {
			attempts.Add(1)
			return CompletionResult{}, ErrCompletionAuthority
		},
	}
	executor, occurrence := newSelectedExecutorTestSubject(t, store, candidate)
	if err := executor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for executor.ActiveCandidates() != 0 {
		select {
		case <-deadline:
			t.Fatal("stale selected authority retained an executable retry chain")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := executor.Retire(context.Background()); !errors.Is(err, ErrCompletionAuthority) {
		t.Fatalf("stale authority diagnostic lost: %v", err)
	}
	if err := executor.Wait(context.Background()); !CompletionJoinSucceeded(err) || !errors.Is(err, ErrCompletionAuthority) {
		t.Fatalf("joined stale authority diagnostic lost: %v", err)
	}
	if err := occurrence.RetireAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("stale selected authority retried %d times", attempts.Load())
	}
}

func newSelectedExecutorTestSubject(t *testing.T, store CandidateStore, candidate Candidate) (*Executor, *worklifetime.SelectedForkOccurrence) {
	t.Helper()
	process := worklifetime.NewProcess()
	identity := worklifetime.SelectedForkIdentity{ExecutionID: "22222222-2222-4222-8222-222222222222", RunID: candidate.RunID, Generation: 1}
	occurrence, err := process.NewSelectedFork(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	source, err := correlation.NewSourceArtifactFact(candidate.BundleHash)
	if err != nil {
		t.Fatal(err)
	}
	authority := effects.Authority{Kind: effects.AuthoritySelectedContractFork, ID: identity.ExecutionID,
		ExecutionOwner: "completion-test", LeaseExpiresAt: time.Now().Add(time.Hour), FenceGeneration: 1, ExecutionMode: effects.ExecutionModeLive,
		SelectedFork: effects.SelectedContractForkAuthority{ExecutionID: identity.ExecutionID, ForkRunID: identity.RunID, Generation: 1,
			AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config"}}
	ctx := effects.WithAuthority(correlation.WithSourceArtifactFact(context.Background(), source), authority)
	executor, err := NewExecutor(store, candidate.Scope(), FinalCatalog{}, occurrence, ExecutorOptions{ExecutionContext: ctx})
	if err != nil {
		t.Fatal(err)
	}
	return executor, occurrence
}
