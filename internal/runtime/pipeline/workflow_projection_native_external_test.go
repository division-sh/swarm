package pipeline_test

import (
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func workflowProjectionNativeFixture(t *testing.T, backend, runID string) pipeline.WorkflowProjectionNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	return workflowProjectionNativeFixtureFromSelected(t, selected, reopen, runID)
}

func workflowProjectionNativeFixtureFromSelected(t *testing.T, selected timerReplaySelectedStore, reopen func() timerReplaySelectedStore, runID string) pipeline.WorkflowProjectionNativeFixtureForTest {
	return workflowProjectionNativeFixtureWithWrites(t, selected, reopen, runID, 1)
}

func workflowProjectionNativeFixtureWithWrites(t *testing.T, selected timerReplaySelectedStore, reopen func() timerReplaySelectedStore, runID string, writes uint64) pipeline.WorkflowProjectionNativeFixtureForTest {
	t.Helper()
	native := workflowActivityNativeFixtureFromSelected(t, selected, reopen)
	ctx := withLiveGateExecution(runtimecorrelation.WithRunID(native.Context, runID))
	if err := native.RequireRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	t.Cleanup(func() {
		counts := probe.Snapshot()
		if counts.Total.WriteCommits != writes || counts.ByOperation[storetest.TransactionWorkflowMutation].WriteCommits != 0 || counts.Active != 0 {
			t.Errorf("projection escaped original selected construction owner: %+v", counts)
		}
	})
	return pipeline.WorkflowProjectionNativeFixtureForTest{
		Persistence: native.Persistence, Context: ctx, Construct: native.Construct,
	}
}

func TestWorkflowInstanceStoreProjection_RoundTripPreservesCanonicalState(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreProjection_RoundTripPreservesCanonicalStateForTest(t, func(t *testing.T, runID string) pipeline.WorkflowProjectionNativeFixtureForTest {
				return workflowProjectionNativeFixture(t, backend, runID)
			})
		})
	}
}

func TestSQLiteWorkflowInstanceStore_MutateERollsBackCallbackFailure(t *testing.T) {
	pipeline.VerifySQLiteWorkflowInstanceStore_MutateERollsBackCallbackFailureForTest(t, func(t *testing.T, runID string) pipeline.WorkflowProjectionNativeFixtureForTest {
		return workflowProjectionNativeFixture(t, "sqlite", runID)
	})
}

func TestWorkflowInstanceStoreProjection_StaticRowsPersistCanonicalFlowPathOnRoundTrip(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreProjection_StaticRowsPersistCanonicalFlowPathOnRoundTripForTest(t, func(t *testing.T, runID string) pipeline.WorkflowProjectionNativeFixtureForTest {
				return workflowProjectionNativeFixture(t, backend, runID)
			})
		})
	}
}

func TestPipelineEngineStateRepoLoadStateRejectsMalformedPersistedCarrier(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineEngineStateRepoLoadStateRejectsMalformedPersistedCarrierForTest(t, func(t *testing.T, runID string) pipeline.WorkflowProjectionNativeFixtureForTest {
				return workflowProjectionNativeFixture(t, backend, runID)
			})
		})
	}
}
