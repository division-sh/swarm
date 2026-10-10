package pipeline_test

import (
	"context"
	"testing"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestWorkflowInstanceStoreMutate_IgnoresSchedulerOwnedTimerRows(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreMutate_IgnoresSchedulerOwnedTimerRowsForTest(t, func(t *testing.T, runID string) pipeline.WorkflowSchedulerTimerNativeFixtureForTest {
				selected, _, reopen := openTimerReplayNativeStore(t, backend)
				native := workflowMutationNativeFixtureFromSelected(t, selected, reopen, runID, 3, 1)
				return pipeline.WorkflowSchedulerTimerNativeFixtureForTest{
					WorkflowProjectionNativeFixtureForTest: native.WorkflowProjectionNativeFixtureForTest,
					AdmitSchedule:                          selected.(runtimegenericschedule.Store).AdmitGenericScheduleOutcome,
					CountSchedulerTimers: func(ctx context.Context, entity, path string) (int64, error) {
						return storetest.CountWorkflowSchedulerTimers(ctx, selected, entity, path)
					},
				}
			})
		})
	}
}
