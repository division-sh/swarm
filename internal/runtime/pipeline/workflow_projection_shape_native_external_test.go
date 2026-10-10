package pipeline_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func workflowProjectionShapeNativeFixture(t *testing.T, backend, runID string) pipeline.WorkflowProjectionShapeNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	native := workflowProjectionNativeFixtureWithWrites(t, selected, reopen, runID, 2)
	return pipeline.WorkflowProjectionShapeNativeFixtureForTest{
		WorkflowProjectionNativeFixtureForTest: native,
		FieldsArray: func(ctx context.Context, run, key string) (int64, error) {
			return storetest.SetWorkflowProjectionFieldsArray(ctx, selected, run, key)
		},
		NumericGate: func(ctx context.Context, run, key string) (int64, error) {
			return storetest.SetWorkflowProjectionNumericGate(ctx, selected, run, key)
		},
		AccumulatorArray: func(ctx context.Context, run, key string) (int64, error) {
			return storetest.SetWorkflowProjectionAccumulatorArray(ctx, selected, run, key)
		},
		MalformedTransitionHistory: func(ctx context.Context, run, key string) (int64, error) {
			return storetest.SetWorkflowProjectionMalformedTransitionHistory(ctx, selected, run, key)
		},
		ConflictingInstanceID: func(ctx context.Context, run, key string) (int64, error) {
			return storetest.SetWorkflowProjectionConflictingInstanceID(ctx, selected, run, key)
		},
		SlashOnlyFlowPath: func(ctx context.Context, run, key string) (int64, error) {
			return storetest.SetWorkflowProjectionSlashOnlyFlowPath(ctx, selected, run, key)
		},
	}
}

func TestWorkflowInstanceStoreProjection_RejectsMalformedPersistedShapes(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreProjection_RejectsMalformedPersistedShapesForTest(t, func(t *testing.T, runID string) pipeline.WorkflowProjectionShapeNativeFixtureForTest {
				return workflowProjectionShapeNativeFixture(t, backend, runID)
			})
		})
	}
}
