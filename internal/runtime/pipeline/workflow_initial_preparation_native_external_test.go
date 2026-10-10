package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestWorkflowInitialLifecyclePreparationRejectsUnownedEmissions(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInitialLifecyclePreparationRejectsUnownedEmissionsForTest(t, func(t *testing.T, runID string) pipeline.WorkflowProjectionNativeFixtureForTest {
				selected, _, reopen := openTimerReplayNativeStore(t, backend)
				return workflowProjectionNativeFixtureWithWrites(t, selected, reopen, runID, 0)
			})
		})
	}
}
