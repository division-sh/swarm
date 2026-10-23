package runtimepersistence

import (
	"context"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type completionHandoffEvidenceProbeSink struct {
	submit func(runtimerunlifecycle.Candidate) error
}

func (s *completionHandoffEvidenceProbeSink) ReserveCompletionCandidate(context.Context) (runtimerunlifecycle.CandidateAdmission, error) {
	return s, nil
}
func (s *completionHandoffEvidenceProbeSink) Submit(c runtimerunlifecycle.Candidate) error {
	return s.submit(c)
}
func (*completionHandoffEvidenceProbeSink) Cancel() error { return nil }
