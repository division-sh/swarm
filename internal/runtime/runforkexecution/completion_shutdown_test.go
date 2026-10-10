package runforkexecution

import (
	"context"
	"errors"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type lateCompletionStore struct {
	started, release chan struct{}
	cleanupErr       error
	executions       atomic.Int32
}

func (*lateCompletionStore) ListCompletionCandidates(context.Context, runlifecycle.CandidateScope, runlifecycle.CandidateCursor, int) (runlifecycle.CandidatePage, error) {
	return runlifecycle.CandidatePage{Exhausted: true}, nil
}

func (s *lateCompletionStore) ExecuteCompletionCandidate(context.Context, runlifecycle.Candidate, runlifecycle.FinalCatalog) (runlifecycle.CompletionResult, error) {
	s.executions.Add(1)
	close(s.started)
	<-s.release
	return runlifecycle.CompletionResult{Committed: true, Outcome: runlifecycle.OutcomeAwaitMutation}, s.cleanupErr
}

type completionShutdownRegistration struct{ releases atomic.Int32 }

func (r *completionShutdownRegistration) Release() { r.releases.Add(1) }

func TestSelectedShutdownPreservesLateCompletionDiagnostic(t *testing.T) {
	primary := errors.New("acknowledged completion cleanup failed after retirement")
	store := &lateCompletionStore{started: make(chan struct{}), release: make(chan struct{}), cleanupErr: primary}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(store.release) })
	candidate := runlifecycle.Candidate{
		RunID: "11111111-1111-4111-8111-111111111111", SelectedForkRunID: "11111111-1111-4111-8111-111111111111",
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), Revision: 1,
		DueAt: runlifecycle.CanonicalTimestamp(time.Now().Add(-time.Second)),
	}
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
	authority := effects.Authority{
		Kind: effects.AuthoritySelectedContractFork, ID: identity.ExecutionID,
		ExecutionOwner: "completion-shutdown-test", LeaseExpiresAt: time.Now().Add(time.Hour), FenceGeneration: 1, ExecutionMode: effects.ExecutionModeLive,
		SelectedFork: effects.SelectedContractForkAuthority{ExecutionID: identity.ExecutionID, ForkRunID: identity.RunID, Generation: 1,
			AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config"},
	}
	ctx := effects.WithAuthority(correlation.WithSourceArtifactFact(context.Background(), source), authority)
	executor, err := runlifecycle.NewExecutor(store, candidate.Scope(), runlifecycle.FinalCatalog{}, occurrence, runlifecycle.ExecutorOptions{ExecutionContext: ctx})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := occurrence.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	registration := &completionShutdownRegistration{}
	diagnostics := &selectedForkCommitDiagnostics{}
	var cleanups atomic.Int32
	runtime := &selectedContractAgentRuntime{
		completionExecutor: executor, completionRegistration: registration, completionDiagnostics: diagnostics,
		executionLease: lease, cleanup: func() { cleanups.Add(1) },
	}
	if err := executor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	awaitCompletionShutdownSignal(t, store.started)
	finished := make(chan error, 1)
	go func() { finished <- runtime.Shutdown() }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for executor.Ready() {
		select {
		case <-deadline.C:
			t.Fatal("shutdown did not close completion admission")
		default:
			goruntime.Gosched()
		}
	}
	if registration.releases.Load() != 0 || cleanups.Load() != 0 {
		t.Fatal("cleanup released custody before accepted completion joined")
	}
	releaseOnce.Do(func() { close(store.release) })
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("committed diagnostic incorrectly retained shutdown resources: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown failed to join completion")
	}
	if !errors.Is(diagnostics.err(), primary) || len(diagnostics.errors) != 1 {
		t.Fatalf("late diagnostic lost or duplicated: %v", diagnostics.err())
	}
	if runtime.completionExecutor != nil || runtime.completionRegistration != nil || runtime.executionLease != nil || registration.releases.Load() != 1 || cleanups.Load() != 1 || store.executions.Load() != 1 {
		t.Fatal("completion shutdown retained custody, replayed the commit or failed exact release")
	}
	if err := runtime.Shutdown(); err != nil || registration.releases.Load() != 1 || cleanups.Load() != 1 || len(diagnostics.errors) != 1 {
		t.Fatalf("joined shutdown was not idempotent: %v", err)
	}
	if err := occurrence.RetireAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if process.ActiveCount() != 0 {
		t.Fatalf("completion retained %d process leases", process.ActiveCount())
	}
}

func awaitCompletionShutdownSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for completion shutdown interleaving")
	}
}
