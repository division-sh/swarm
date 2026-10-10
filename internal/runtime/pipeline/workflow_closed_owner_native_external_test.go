package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func workflowClosedOwnerNativeFixture(t *testing.T, backend string) pipeline.WorkflowClosedOwnerNativeFixtureForTest {
	t.Helper()
	selected, closeOwner, _ := openTimerReplayNativeStore(t, backend)
	native := workflowActivityNativeFixtureFromSelected(t, selected, nil)
	ctx := withLiveGateExecution(correlation.WithRunID(native.Context, "77777777-7777-7777-7777-777777777777"))
	if err := native.RequireRun(ctx, correlation.RunIDFromContext(ctx)); err != nil {
		t.Fatal(err)
	}
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	t.Cleanup(func() {
		counts := probe.Snapshot()
		if counts.Total.WriteCommits != 0 || counts.Active != 0 {
			t.Errorf("closed-owner proof wrote or escaped joined ownership: %+v", counts)
		}
	})
	return pipeline.WorkflowClosedOwnerNativeFixtureForTest{
		Persistence: native.Persistence, Context: ctx, Close: closeOwner,
	}
}

func TestUpdateEntityState_ReturnsWorkflowStoreMutationError(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyUpdateEntityState_ReturnsWorkflowStoreMutationErrorForTest(t, func(t *testing.T) pipeline.WorkflowClosedOwnerNativeFixtureForTest {
				return workflowClosedOwnerNativeFixture(t, backend)
			})
		})
	}
}

func TestAccumulatorAppend_ReturnsWorkflowStoreMutationError(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyAccumulatorAppend_ReturnsWorkflowStoreMutationErrorForTest(t, func(t *testing.T) pipeline.WorkflowClosedOwnerNativeFixtureForTest {
				return workflowClosedOwnerNativeFixture(t, backend)
			})
		})
	}
}
