package runhandoff

import (
	"context"
	"sync"
	"testing"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

const handoffTestBundleHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type recordingCandidateSink struct {
	mu        sync.Mutex
	reserved  int
	submitted []runtimerunlifecycle.Candidate
	cancelled int
}

func (s *recordingCandidateSink) ReserveCompletionCandidate(context.Context) (runtimerunlifecycle.CandidateAdmission, error) {
	s.mu.Lock()
	s.reserved++
	s.mu.Unlock()
	return &recordingCandidateAdmission{sink: s}, nil
}

type recordingCandidateAdmission struct {
	sink *recordingCandidateSink
}

func (a *recordingCandidateAdmission) Submit(candidate runtimerunlifecycle.Candidate) error {
	a.sink.mu.Lock()
	defer a.sink.mu.Unlock()
	a.sink.submitted = append(a.sink.submitted, candidate)
	return nil
}

func (a *recordingCandidateAdmission) Cancel() error {
	a.sink.mu.Lock()
	defer a.sink.mu.Unlock()
	a.sink.cancelled++
	return nil
}

func TestCandidateHandoffCoalescesOnlyExactIdentityWithinTransaction(t *testing.T) {
	coordinator := NewCandidateCoordinator()
	sink := &recordingCandidateSink{}
	registration, err := coordinator.Register(
		context.Background(),
		runtimerunlifecycle.CandidateScope{BundleHash: handoffTestBundleHash},
		sink,
	)
	if err != nil {
		t.Fatalf("register candidate sink: %v", err)
	}
	defer registration.Release()

	handoff, err := ReserveCandidateHandoff(context.Background())
	if err != nil {
		t.Fatalf("reserve candidate handoff: %v", err)
	}
	defer handoff.Rollback()
	candidate := runtimerunlifecycle.Candidate{
		RunID:      "11111111-1111-4111-8111-111111111111",
		BundleHash: handoffTestBundleHash,
		Revision:   1,
		DueAt:      time.Date(2026, 8, 28, 2, 0, 0, 0, time.UTC),
	}
	for _, disposition := range []runtimerunlifecycle.CandidateRequestDisposition{
		runtimerunlifecycle.CandidateRequested,
		runtimerunlifecycle.CandidateAlreadyCurrent,
	} {
		if err := handoff.Prepare(coordinator, runtimerunlifecycle.CandidateRequestResult{
			Disposition: disposition,
			Candidate:   candidate,
		}); err != nil {
			t.Fatalf("prepare exact candidate %s: %v", disposition, err)
		}
	}
	changedDue := candidate
	changedDue.DueAt = changedDue.DueAt.Add(time.Microsecond)
	if err := handoff.Prepare(coordinator, runtimerunlifecycle.CandidateRequestResult{
		Disposition: runtimerunlifecycle.CandidateAlreadyCurrent,
		Candidate:   changedDue,
	}); err != nil {
		t.Fatalf("prepare distinct exact candidate: %v", err)
	}
	if err := handoff.Commit(); err != nil {
		t.Fatalf("commit candidate handoff: %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.reserved != 2 || len(sink.submitted) != 2 {
		t.Fatalf("candidate admissions = reserved:%d submitted:%d, want 2/2", sink.reserved, len(sink.submitted))
	}
	if !sink.submitted[0].SameIdentity(candidate) || !sink.submitted[1].SameIdentity(changedDue) {
		t.Fatalf("submitted candidates = %#v", sink.submitted)
	}
}
