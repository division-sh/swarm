package genericschedule

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

func TestLifecycleMissingOrInvalidCommitResultReleasesPlan(t *testing.T) {
	for _, phase := range []string{"zero_nil_error", "invalid_acknowledged"} {
		t.Run(phase, func(t *testing.T) {
			activation, occurrence, wakeup := lifecyclePreparedOccurrence(t)
			result := CommitResult{}
			want := CommitRetry
			if phase == "invalid_acknowledged" {
				result.Outcome, want = CommitCommitted, CommitCommitted
			}
			store := &lifecycleProofStore{activation: activation, prepared: PreparedOccurrence{Outcome: PrepareReady, Activation: activation, Occurrence: occurrence}, commit: func(CommitCommand) (CommitResult, error) { return result, nil }}
			planner := &lifecycleProofPlanner{}
			dispatcher := &lifecycleProofDispatcher{}
			lifecycle, err := NewLifecycle(store, &lifecycleProofScheduler{}, planner, dispatcher, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			defer stopLifecycleProof(t, lifecycle)
			got, err := lifecycle.fire(context.Background(), wakeup)
			if err == nil || got.Outcome != want || planner.releases != 1 || planner.finalizes != 0 || dispatcher.calls != 0 || store.commitCalls != 1 {
				t.Fatalf("result=%+v err=%v release=%d finalize=%d dispatch=%d commits=%d", got, err, planner.releases, planner.finalizes, dispatcher.calls, store.commitCalls)
			}
		})
	}
}
