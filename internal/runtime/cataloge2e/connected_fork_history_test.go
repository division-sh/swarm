package cataloge2e

import (
	"context"

	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestConnectedForkCompletedDynamicHistoryRefusalBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := selectedForkReadinessCatalogFixture(t, 0, "node")
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, "worker-flow/worker-001", "worker.inspect.requested")
			ctx := catalogRunContext(h, catalogRuntimeRunID)
			frontier := publishSelectedForkReadinessFrontier(t, ctx, h, "worker.inspect")
			observed, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			var ancestorID string
			for _, event := range observed {
				if event.EventID == frontier {
					ancestorID = event.SourceEventID
				}
			}
			completedAncestor := false
			for _, event := range observed {
				if event.EventID != ancestorID || event.EventName != "worker.inspect" || len(event.Deliveries) != 1 {
					continue
				}
				delivery := event.Deliveries[0]
				completedAncestor = delivery.Status == "delivered" && delivery.Terminal && !delivery.Route.ConnectClaim.Empty() && delivery.Target.FlowInstance == "worker-flow/worker-001"
			}
			if !completedAncestor {
				t.Fatalf("frontier %s has no completed connection-authorized dynamic ancestor %s", frontier, ancestorID)
			}
			var source interface {
				storetest.RootedDataCatalogStore
				forkexecution.SourceArtifactSelectedContractSourceStore
			} = h.pg
			if h.sqlite != nil {
				source = h.sqlite
			}
			loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, source)
			installCatalogSelectedSourceTopology(t, ctx, h, loaded)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			// Join the source writers, but retain the process capability needed by
			// the real selected-contract operation. Harness cleanup retires it.
			if err := h.rt.Shutdown(); err != nil {
				t.Fatal(err)
			}
			before := snapshotCatalogApplication(t, h)
			for attempt := 0; attempt < 2; attempt++ {
				result, err := forkexecution.ExecuteSelectedContractRunFork(ctx, forkexecution.SelectedContractExecutionRequest{
					SourceRunID: catalogRuntimeRunID, At: frontier, AllowSourceFreeze: true,
					Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
				})
				storetest.RequireRunForkReplayResumeBlocker(t, err, runfork.RunForkBlockerSelectedContractRouteRecoveryUnproven, runfork.RunForkHistoricalReplayFactRoutes)
				if err.Error() != "selected-contract route resolution requires complete static and dynamic topology proof" {
					t.Fatalf("different route-history manifestation: %v", err)
				}
				if result.Materialization.ForkRunID != "" || result.Activation.Activated || result.ExecutedEventCount != 0 || len(result.ForkEvents) != 0 || result.ForkLocalRuntimeContainer != nil {
					t.Fatalf("refusal returned fork work: %+v", result)
				}
				if after := snapshotCatalogApplication(t, h); !reflect.DeepEqual(before, after) {
					for table, rows := range after {
						if !reflect.DeepEqual(before[table], rows) {
							t.Errorf("attempt %d changed %s: before=%v after=%v", attempt, table, before[table], rows)
						}
					}
					t.Fatal("refused connected-history fork changed application state")
				}
			}
		})
	}
}

// Every application table, column and row participates. Only database-internal
// objects are excluded; the caller must join background writers first.
func snapshotCatalogApplication(t *testing.T, h *runtimeHarness) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), reader)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
