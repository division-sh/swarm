package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func workflowProjectionHeaderNativeFixture(t *testing.T, backend, runID string) pipeline.WorkflowProjectionHeaderNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	native := workflowProjectionNativeFixtureWithWrites(t, selected, reopen, runID, 2)
	return pipeline.WorkflowProjectionHeaderNativeFixtureForTest{
		WorkflowProjectionNativeFixtureForTest: native,
		List:                                   selected.ListWorkflowInstances,
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
