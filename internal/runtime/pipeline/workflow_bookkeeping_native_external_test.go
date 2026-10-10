package pipeline_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestWorkflowEngineCompleteCarrierPreservesBookkeepingOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowEngineCompleteCarrierPreservesBookkeepingForTest(t, func(t *testing.T, runID string) pipeline.WorkflowBookkeepingNativeFixtureForTest {
				selected, _, reopen := openTimerReplayNativeStore(t, backend)
				native := workflowMutationNativeFixtureFromSelected(t, selected, reopen, runID, 3, 1)
				return pipeline.WorkflowBookkeepingNativeFixtureForTest{
					WorkflowProjectionNativeFixtureForTest: native.WorkflowProjectionNativeFixtureForTest,
					SetPlatformBookkeeping: func(ctx context.Context, run, path string) (int64, error) {
						return storetest.SetWorkflowProjectionPlatformBookkeeping(ctx, selected, run, path)
					},
				}
			})
		})
	}
}
