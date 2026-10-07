package pipeline

import (
	"context"
	"errors"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestIssue2564TimerInterceptionOutcomePreservesDisposition(t *testing.T) {
	contention := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "pipeline", "timer_test", nil)
	canceled := errors.Join(errWorkflowTimerUncommittedInterruption, context.Canceled)
	deadline := errors.Join(errWorkflowTimerUncommittedInterruption, context.DeadlineExceeded)
	for _, test := range []struct {
		name       string
		recognized bool
		advanced   bool
		err        error
		committed  bool
		retry      bool
		code       string
		class      runtimefailures.Class
	}{
		{name: "ordinary_event"},
		{name: "ordinary_contention", err: contention},
		{name: "ordinary_interruption", err: canceled},
		{name: "stale_timer", recognized: true},
		{name: "committed_timer", recognized: true, advanced: true, committed: true},
		{name: "committed_contention", recognized: true, advanced: true, err: contention, committed: true},
		{name: "committed_cancellation", recognized: true, advanced: true, err: canceled, committed: true},
		{name: "uncommitted_contention", recognized: true, err: contention, retry: true, code: "workflow_engine_state_revision_conflict", class: runtimefailures.ClassLifecycleConflict},
		{name: "contention_with_cleanup", recognized: true, err: errors.Join(contention, errors.New("independent cleanup failure"))},
		{name: "uncommitted_cancellation", recognized: true, err: canceled, retry: true, code: "workflow_timer_transition_interrupted", class: runtimefailures.ClassDependencyUnavailable},
		{name: "uncommitted_deadline", recognized: true, err: deadline, retry: true, code: "workflow_timer_transition_interrupted", class: runtimefailures.ClassTimeout},
		{name: "unclassified_failure", recognized: true, err: errors.New("ordinary failure")},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome, retry := workflowTimerInterceptionOutcome(test.recognized, test.advanced, test.err)
			if outcome.Committed != test.committed || retry != test.retry || outcome.ContinueDispatch() == retry {
				t.Fatalf("committed=%t retry=%t continue=%t", outcome.Committed, retry, outcome.ContinueDispatch())
			}
			if _, durable := outcome.Disposition(); durable {
				t.Fatal("timer interception invented a durable settlement")
			}
			release, released := outcome.RetryRelease()
			if released != retry {
				t.Fatalf("retry release=%t want=%t", released, retry)
			}
			if retry {
				failure := release.Failure()
				if release.ReasonCode() != test.code || failure == nil || failure.Detail.Code != test.code || failure.Class != test.class {
					t.Fatalf("retry=%s failure=%+v", release.ReasonCode(), failure)
				}
			}
		})
	}
}
