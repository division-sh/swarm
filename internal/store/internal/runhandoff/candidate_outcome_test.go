package runhandoff

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type outcomeSink struct {
	submits, cancels int
	err              error
}

func (s *outcomeSink) ReserveCompletionCandidate(context.Context) (runlifecycle.CandidateAdmission, error) {
	return s, nil
}
func (s *outcomeSink) Submit(runlifecycle.Candidate) error { s.submits++; return s.err }
func (s *outcomeSink) Cancel() error                       { s.cancels++; return nil }

func TestCandidateHandoffOutcomeRetainsResultAndIndependentErrors(t *testing.T) {
	for _, committed := range []bool{false, true} {
		for _, failHandoff := range []bool{false, true} {
			t.Run(fmt.Sprintf("committed=%v/handoff_failure=%v", committed, failHandoff), func(t *testing.T) {
				primary, handoffErr := errors.New("transaction owner error"), errors.New("handoff error")
				sink := &outcomeSink{}
				if failHandoff {
					sink.err = handoffErr
				}
				coordinator := NewCandidateCoordinator()
				registration, err := coordinator.Register(context.Background(), runlifecycle.CandidateScope{BundleHash: handoffTestBundleHash}, sink)
				if err != nil {
					t.Fatal(err)
				}
				defer registration.Release()
				calls := 0
				result, err := WithCandidateHandoffOutcomeResult(context.Background(), func(handoff *CandidateHandoff) (int, bool, error) {
					calls++
					candidate := runlifecycle.Candidate{RunID: "11111111-1111-4111-8111-111111111111", BundleHash: handoffTestBundleHash, Revision: 1, DueAt: time.Now().UTC().Truncate(time.Microsecond)}
					if err := handoff.Prepare(coordinator, runlifecycle.CandidateRequestResult{Disposition: runlifecycle.CandidateRequested, Candidate: candidate}); err != nil {
						t.Fatal(err)
					}
					return 42, committed, primary
				})
				if !errors.Is(err, primary) || errors.Is(err, handoffErr) != (committed && failHandoff) {
					t.Fatalf("lost independent error: %v", err)
				}
				if calls != 1 {
					t.Fatalf("callback replayed %d times", calls)
				}
				if committed {
					if result != 42 || sink.submits != 1 || sink.cancels != 0 {
						t.Fatalf("lost committed result or handoff: %d %+v", result, sink)
					}
				} else if result != 0 || sink.submits != 0 || sink.cancels != 1 {
					t.Fatalf("unacknowledged result escaped: %d %+v", result, sink)
				}
			})
		}
	}
}
