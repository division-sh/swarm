package runlifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func TestCandidateAdmissionCancelSettlesRealLeaseExactlyOnce(t *testing.T) {
	for _, settledLease := range []bool{false, true} {
		name := "fresh"
		if settledLease {
			name = "already_settled_lease"
		}
		t.Run(name, func(t *testing.T) {
			executor, occurrence := newExecutorTestSubject(t, &executorTestStore{}, ExecutorOptions{})
			defer retireExecutorTestSubject(t, executor, occurrence)
			reserved, err := executor.ReserveCompletionCandidate(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			admission := reserved.(*candidateAdmission)
			pendingZero := executor.pendingZero
			if occurrence.ActiveCount() != 1 || executor.pending != 1 {
				t.Fatalf("reservation accounting active=%d pending=%d", occurrence.ActiveCount(), executor.pending)
			}
			if settledLease {
				// Hostile private-owner misuse: the only fallible Done path for
				// this valid lease has already released its occurrence token.
				if err := admission.lease.Done(); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 2; i++ {
				err := admission.Cancel()
				if settledLease && !errors.Is(err, worklifetime.ErrAlreadySettled) || !settledLease && err != nil {
					t.Fatalf("cancel %d: %v", i, err)
				}
				if occurrence.ActiveCount() != 0 || executor.pending != 0 {
					t.Fatalf("cancel leaked admission active=%d pending=%d", occurrence.ActiveCount(), executor.pending)
				}
			}
			select {
			case <-pendingZero:
			default:
				t.Fatal("canceled admission still blocks retirement")
			}
		})
	}
}

func TestCandidateAdmissionCancelAfterRejectedSubmitRetainsErrorWithoutLease(t *testing.T) {
	executor, occurrence := newExecutorTestSubject(t, &executorTestStore{}, ExecutorOptions{})
	defer retireExecutorTestSubject(t, executor, occurrence)
	admission, err := executor.ReserveCompletionCandidate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	submitErr := admission.Submit(Candidate{})
	if submitErr == nil {
		t.Fatal("invalid candidate admitted")
	}
	if err := admission.Cancel(); err != submitErr {
		t.Fatalf("cancel lost already-settled submission error: %v / %v", err, submitErr)
	}
	if occurrence.ActiveCount() != 0 || executor.pending != 0 || executor.ActiveCandidates() != 0 {
		t.Fatalf("rejected submission leaked work: active=%d pending=%d", occurrence.ActiveCount(), executor.pending)
	}
}
