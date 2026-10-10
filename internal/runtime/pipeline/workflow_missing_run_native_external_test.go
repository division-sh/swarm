package pipeline_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestWorkflowInstanceStore_RequiresRunContext(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStore_RequiresRunContextForTest(t, func(t *testing.T) pipeline.WorkflowActivityNativeFixtureForTest {
				selected, _, reopen := openTimerReplayNativeStore(t, backend)
				native := workflowActivityNativeFixtureFromSelected(t, selected, reopen)
				native.Context = withLiveGateExecution(native.Context)
				probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
				t.Cleanup(func() {
					if counts := probe.Snapshot(); counts.Total.WriteCommits != 0 || counts.Active != 0 {
						t.Errorf("missing run wrote through native coordinator: %+v", counts)
					}
					count, err := storetest.CountWorkflowInstanceHeaders(context.Background(), selected)
					if err != nil || count != 0 {
						t.Errorf("missing run admitted a workflow header: %d %v", count, err)
					}
				})
				return native
			})
		})
	}
}
