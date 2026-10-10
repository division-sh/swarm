package pipeline_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func workflowProjectionStorageNativeFixture(t *testing.T, backend, runID string) pipeline.WorkflowProjectionStorageNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	return pipeline.WorkflowProjectionStorageNativeFixtureForTest{
		WorkflowProjectionNativeFixtureForTest: workflowProjectionNativeFixtureFromSelected(t, selected, reopen, runID),
		ReadControl: func(ctx context.Context, run, entity string) (pipeline.WorkflowControlProjectionStorageForTest, error) {
			out, err := storetest.ReadWorkflowControlProjectionStorage(ctx, selected, run, entity)
			return pipeline.WorkflowControlProjectionStorageForTest(out), err
		},
		ReadDuplicate: func(ctx context.Context, run, entity string) (pipeline.WorkflowDuplicateProjectionStorageForTest, error) {
			out, err := storetest.ReadWorkflowDuplicateProjectionStorage(ctx, selected, run, entity)
			return pipeline.WorkflowDuplicateProjectionStorageForTest(out), err
		},
	}
}

func TestWorkflowInstanceStoreProjection_DoesNotExposeControlStatusAsEntityField(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreProjection_DoesNotExposeControlStatusAsEntityFieldForTest(t, func(t *testing.T, runID string) pipeline.WorkflowProjectionStorageNativeFixtureForTest {
				return workflowProjectionStorageNativeFixture(t, backend, runID)
			})
		})
	}
}

func TestWorkflowInstanceStoreCreateRejectsDuplicateWithoutMutatingProjection(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreCreateRejectsDuplicateWithoutMutatingProjectionForTest(t, func(t *testing.T, runID string) pipeline.WorkflowProjectionStorageNativeFixtureForTest {
				return workflowProjectionStorageNativeFixture(t, backend, runID)
			})
		})
	}
}
