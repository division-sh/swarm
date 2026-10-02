package cataloge2e

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestConnectedForkFutureSuccessArtifactsPreserved(t *testing.T) {
	for name, want := range map[string]string{
		"connected_fork_readiness_success_test.go.txt": "d7c22f96f5c59896ee8c9ac18257975086f8da603bcbd4ca40b9639383fc8392",
		"connected_fork_readiness_fixture.go.txt":      "16e427c354f33b812e5eba9a86602433ece5d756b75c08b354a166762e6e75f4",
	} {
		raw, err := os.ReadFile("testdata/future_capabilities/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != want {
			t.Fatalf("#642 original success oracle %s changed: %s, want %s", name, got, want)
		}
	}
}

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
				storetest.DurableDataCatalogStore
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
				storetest.RequireRunForkReplayResumeBlocker(t, err, runfork.RunForkBlockerFlowRouteHistoryUnproven, runfork.RunForkReplayResumeFactRouteHistory)
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
func snapshotCatalogApplication(t *testing.T, h *runtimeHarness) map[string][]string {
	t.Helper()
	opts := &sql.TxOptions{ReadOnly: true}
	query := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	if h.backend == catalogBackendPostgres {
		opts.Isolation = sql.LevelRepeatableRead
		query = `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name`
	}
	tx, err := h.db.BeginTx(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rows, err := tx.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	out := map[string][]string{}
	for _, table := range tables {
		rows, err := tx.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `"`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(columns)
		if err != nil {
			t.Fatal(err)
		}
		out[table+"/columns"] = []string{string(encoded)}
		out[table] = []string{}
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			for i, value := range values {
				if raw, ok := value.([]byte); ok {
					values[i] = string(raw)
				}
			}
			encoded, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			out[table] = append(out[table], string(encoded))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		sort.Strings(out[table])
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return out
}
