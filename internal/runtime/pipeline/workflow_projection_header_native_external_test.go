package pipeline_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func workflowProjectionHeaderNativeFixture(t *testing.T, backend, runID string) pipeline.WorkflowProjectionHeaderNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	native := workflowProjectionNativeFixtureWithWrites(t, selected, reopen, runID, 3)
	return pipeline.WorkflowProjectionHeaderNativeFixtureForTest{
		WorkflowProjectionNativeFixtureForTest: native,
		ObsoleteFieldRows: func(ctx context.Context, run string) (int64, error) {
			return storetest.SetWorkflowProjectionObsoleteFieldRows(ctx, selected, run)
		},
		List: selected.ListWorkflowInstances,
	}
}
func TestWorkflowInstanceFixtureListUsesConstructedHeadersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceFixtureListUsesConstructedHeadersForTest(t, func(t *testing.T, runID string) pipeline.WorkflowProjectionHeaderNativeFixtureForTest {
				return workflowProjectionHeaderNativeFixture(t, backend, runID)
			})
		})
	}
}
