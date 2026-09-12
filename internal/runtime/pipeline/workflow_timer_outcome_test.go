package pipeline

import (
	"context"
	"testing"
)

type timerResultControlOwner struct {
	result CommittedWorkflowTimerOccurrence
	calls  int
}

func (o *timerResultControlOwner) CommitWorkflowTimerOccurrence(context.Context, WorkflowTimerOccurrenceCommand) (CommittedWorkflowTimerOccurrence, error) {
	o.calls++
	return o.result, nil
}

func TestWorkflowTimerMissingOrInvalidCommitResultDoesNotDispatch(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		for _, phase := range []string{"zero_nil_error", "invalid_acknowledged"} {
			t.Run(backend.name+"/"+phase, func(t *testing.T) {
				store, ctx := backend.open(t)
				bus := &recordingPipelineBus{}
				pc, _, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)
				defer pc.StopWorkflowTimerLifecycle(context.Background())
				owner := &timerResultControlOwner{}
				want := WorkflowTimerFireRetry
				if phase == "invalid_acknowledged" {
					owner.result.Outcome, want = WorkflowTimerOccurrenceCommitted, WorkflowTimerFireTerminal
				}
				pc.workflowTimers.storeOwner.timerOccurrences = owner
				planner := &outcomeEnginePlanner{recordingPipelineBus: bus}
				pc.workflowTimers.publication = planner
				// This control calls Fire directly: suppress asynchronous recovery
				// so one malformed owner response cannot race fixture teardown.
				pc.workflowTimers.workOwner = nil
				pc.workflowTimers.scheduler = nil
				wakeup, err := newWorkflowTimerWakeup(activation)
				if err != nil {
					t.Fatal(err)
				}
				outcome, recurrence, err := pc.workflowTimers.fireWakeup(ctx, wakeup)
				if err == nil || outcome != want || recurrence || owner.calls != 1 || planner.releases != 1 || planner.finalizes != 0 || bus.publishedCount() != 0 {
					t.Fatalf("outcome=%s recurrence=%t err=%v calls=%d releases=%d finalizes=%d published=%d", outcome, recurrence, err, owner.calls, planner.releases, planner.finalizes, bus.publishedCount())
				}
			})
		}
	}
}
