package cataloge2e

import (
	"context"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCatalogSourceRunLifecycleUsesSelectedOwnerBothStores(t *testing.T) {
	fixture := catalogRuntimeFixture(t, "catalog.runtime.event_loop", "test-chain-depth-limit")
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, fixture.Root, backend, true)
			h.shutdown()
			var selected runtimebus.RunLifecycleReadPersistence = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			h.db = nil
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			assertSourceRunLifecycle(t, selected, catalogRuntimeRunID, "running", false)
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("lifecycle observation wrote or retained selected work: %+v", counts)
			}
			if snapshot, err := selected.LoadRunLifecycleSnapshot(testAuthorActivityContext(context.Background()), uuid.NewString()); err == nil || snapshot.RunID != "" {
				t.Fatalf("missing run borrowed lifecycle evidence: %+v %v", snapshot, err)
			}
		})
	}
}
