package pipeline_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func workflowLookupNativeFixture(t *testing.T, backend, runID string) pipeline.WorkflowLookupNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	native := workflowActivityNativeFixtureFromSelected(t, selected, reopen)
	ctx := withLiveGateExecution(correlation.WithRunID(native.Context, runID))
	if err := native.RequireRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	t.Cleanup(func() {
		counts := probe.Snapshot()
		if counts.Total.WriteCommits != 0 || counts.Total.ReadCommits != 4 || counts.Active != 0 {
			t.Errorf("lookup miss escaped original owner: %+v", counts)
		}
	})
	return pipeline.WorkflowLookupNativeFixtureForTest{
		WorkflowProjectionNativeFixtureForTest: pipeline.WorkflowProjectionNativeFixtureForTest{Persistence: native.Persistence, Context: ctx, Construct: native.Construct},
		CountHeaders:                           func(ctx context.Context) (int64, error) { return storetest.CountWorkflowInstanceHeaders(ctx, selected) },
	}
}
func TestWorkflowInstanceLookupMissIsTypedAndExactOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceLookupMissIsTypedAndExactForTest(t, func(t *testing.T, runID string) pipeline.WorkflowLookupNativeFixtureForTest {
				return workflowLookupNativeFixture(t, backend, runID)
			})
		})
	}
}
