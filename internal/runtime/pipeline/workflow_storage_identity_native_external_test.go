package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func workflowStorageIdentityNativeFixture(t *testing.T, backend string, wantCommits uint64) pipeline.WorkflowActivityNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	fixture := workflowActivityNativeFixtureFromSelected(t, selected, reopen)
	fixture.Context = withLiveGateExecution(fixture.Context)
	// The first run also persists the canonical source artifact once.
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	t.Cleanup(func() {
		counts := probe.Snapshot()
		if counts.Total.WriteCommits != wantCommits || counts.ByOperation[storetest.TransactionWorkflowMutation].WriteCommits != 0 || counts.Active != 0 {
			t.Errorf("storage identity escaped original selected construction: %+v, want commits=%d", counts, wantCommits)
		}
	})
	return fixture
}

func TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleedForTest(t, func(t *testing.T) pipeline.WorkflowActivityNativeFixtureForTest {
				return workflowStorageIdentityNativeFixture(t, backend, 5)
			})
		})
	}
}

func TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStoresForTest(t, func(t *testing.T) pipeline.WorkflowActivityNativeFixtureForTest {
				return workflowStorageIdentityNativeFixture(t, backend, 4)
			})
		})
	}
}

func TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifySQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadataForTest(t, func(t *testing.T) pipeline.WorkflowActivityNativeFixtureForTest {
				return workflowStorageIdentityNativeFixture(t, backend, 3)
			})
		})
	}
}
