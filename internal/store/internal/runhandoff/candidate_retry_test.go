package runhandoff

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func retryCandidate() runlifecycle.CandidateRequestResult {
	return runlifecycle.CandidateRequestResult{Disposition: runlifecycle.CandidateRequested,
		Candidate: runlifecycle.Candidate{RunID: "11111111-1111-4111-8111-111111111111",
			BundleHash: handoffTestBundleHash, Revision: 1,
			DueAt: time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)}}
}

func TestCandidateHandoffResetAttemptPreservesOccurrenceAndOnlyCommitsCurrent(t *testing.T) {
	process := worklifetime.NewProcess()
	occurrence, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: "retry-runtime", BundleHash: handoffTestBundleHash})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := occurrence.RetireAndWait(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ctx := worklifetime.WithOccurrence(context.Background(), occurrence)
	handoff, err := ReserveCandidateHandoff(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer handoff.Rollback()
	lease := handoff.lease
	coordinator := NewCandidateCoordinator()
	sink := &recordingCandidateSink{}
	registration, err := coordinator.Register(ctx, runlifecycle.CandidateScope{BundleHash: handoffTestBundleHash}, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Release()
	if err := handoff.ResetAttempt(); err != nil {
		t.Fatal(err)
	}
	first := retryCandidate()
	if err := handoff.Prepare(coordinator, first); err != nil {
		t.Fatal(err)
	}
	if err := occurrence.Fence(); err != nil {
		t.Fatal(err)
	}
	if err := handoff.ResetAttempt(); err != nil {
		t.Fatal(err)
	}
	if handoff.lease != lease || occurrence.ActiveCount() != 1 || sink.cancelled != 1 || len(sink.submitted) != 0 {
		t.Fatalf("reset lost lifetime or published rollback: active=%d cancelled=%d submitted=%v", occurrence.ActiveCount(), sink.cancelled, sink.submitted)
	}
	// Reusing the exact identity must reserve again after rollback; changed due
	// remains distinct within the same successful attempt.
	if err := handoff.Prepare(coordinator, first); err != nil {
		t.Fatal(err)
	}
	if err := handoff.Prepare(coordinator, first); err != nil {
		t.Fatal(err)
	}
	current := first
	current.Candidate.DueAt = current.Candidate.DueAt.Add(time.Microsecond)
	if err := handoff.Prepare(coordinator, current); err != nil {
		t.Fatal(err)
	}
	if err := handoff.Commit(); err != nil {
		t.Fatal(err)
	}
	handoff.Rollback()
	if sink.reserved != 3 || sink.cancelled != 1 || len(sink.submitted) != 2 || occurrence.ActiveCount() != 0 ||
		!sink.submitted[0].SameIdentity(first.Candidate) || !sink.submitted[1].SameIdentity(current.Candidate) {
		t.Fatalf("attempt settlement: sink=%+v active=%d", sink, occurrence.ActiveCount())
	}
	if err := handoff.ResetAttempt(); err == nil {
		t.Fatal("settled handoff admitted a reset")
	}
}

func TestCandidateHandoffResetAttemptReleasesPreRegistrationBarrier(t *testing.T) {
	handoff, err := ReserveCandidateHandoff(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer handoff.Rollback()
	coordinator := NewCandidateCoordinator()
	if err := handoff.Prepare(coordinator, retryCandidate()); err != nil {
		t.Fatal(err)
	}
	barrier := coordinator.entries[handoffTestBundleHash].pendingZero
	if err := handoff.ResetAttempt(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-barrier:
	default:
		t.Fatal("rolled-back preregistration barrier remained held")
	}
	if err := handoff.ResetAttempt(); err != nil {
		t.Fatal(err)
	}
	sink := &recordingCandidateSink{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	registration, err := coordinator.Register(ctx, runlifecycle.CandidateScope{BundleHash: handoffTestBundleHash}, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Release()
	if err := handoff.Prepare(coordinator, retryCandidate()); err != nil {
		t.Fatal(err)
	}
	handoff.Rollback()
	if sink.reserved != 1 || sink.cancelled != 1 || len(sink.submitted) != 0 {
		t.Fatalf("rollback after reset: %+v", sink)
	}
	if err := handoff.ResetAttempt(); err == nil {
		t.Fatal("rolled-back handoff admitted a reset")
	}
}

type retryCancelFailure struct {
	err     error
	cancels int
	submits int
}

func TestCandidateHandoffResetAttemptThenNoCandidateCommitsNothing(t *testing.T) {
	handoff, err := ReserveCandidateHandoff(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer handoff.Rollback()
	coordinator := NewCandidateCoordinator()
	sink := &recordingCandidateSink{}
	registration, err := coordinator.Register(context.Background(), runlifecycle.CandidateScope{BundleHash: handoffTestBundleHash}, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Release()
	if err := handoff.Prepare(coordinator, retryCandidate()); err != nil {
		t.Fatal(err)
	}
	if err := handoff.ResetAttempt(); err != nil {
		t.Fatal(err)
	}
	if err := handoff.Commit(); err != nil {
		t.Fatal(err)
	}
	if sink.reserved != 1 || sink.cancelled != 1 || len(sink.submitted) != 0 {
		t.Fatalf("no-candidate retry revived previous attempt: %+v", sink)
	}
}

func (a *retryCancelFailure) Submit(runlifecycle.Candidate) error { a.submits++; return nil }
func (a *retryCancelFailure) Cancel() error                       { a.cancels++; return a.err }

func TestCandidateHandoffResetAttemptReturnsCancellationFailure(t *testing.T) {
	failure := errors.New("candidate cancellation failure")
	handoff, err := ReserveCandidateHandoff(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer handoff.Rollback()
	coordinator := NewCandidateCoordinator()
	if err := handoff.Prepare(coordinator, retryCandidate()); err != nil {
		t.Fatal(err)
	}
	barrier := coordinator.entries[handoffTestBundleHash].pendingZero
	admission := &retryCancelFailure{err: failure}
	handoff.handoffs = append(handoff.handoffs, candidateHandoff{admission: admission})
	if err := handoff.ResetAttempt(); !errors.Is(err, failure) {
		t.Fatalf("lost cancellation error: %v", err)
	}
	select {
	case <-barrier:
	default:
		t.Fatal("cancellation error skipped remaining barrier cleanup")
	}
	if len(handoff.handoffs) != 1 || handoff.handoffs[0].admission != admission || len(handoff.barriers) != 0 || len(handoff.identities) != 0 || handoff.settled {
		t.Fatalf("stale attempt state or lost outer reservation: %+v", handoff)
	}
	if err := handoff.Commit(); !errors.Is(err, failure) || admission.submits != 0 {
		t.Fatalf("failed cancellation allowed stale submission: %v, %+v", err, admission)
	}
	if err := handoff.Prepare(coordinator, retryCandidate()); !errors.Is(err, failure) {
		t.Fatalf("failed cancellation allowed a new admission: %v", err)
	}
	handoff.Rollback()
	if admission.cancels != 2 {
		t.Fatalf("outer cleanup lost failed admission: %+v", admission)
	}
}
