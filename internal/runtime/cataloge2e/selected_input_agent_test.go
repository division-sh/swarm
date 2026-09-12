package cataloge2e

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestSelectedForkPendingRootAgentInputBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := catalogRuntimeFixture(t, "catalog.runtime.selected_contract_fork", "test-selected-contract-fork-execution").Root
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			h.seedInitialState(pipeline.FlowInstanceEntityID(catalogRuntimeRunID))
			ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
			if _, err := runScopedCatalogStore(t, h).PauseRunControl(ctx, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID, Reason: "pending root agent input", ControlledBy: "cataloge2e"}); err != nil {
				t.Fatal(err)
			}
			if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "task.ready", Payload: map[string]any{}}, 10*time.Second, true); err != nil {
				t.Fatal(err)
			}
			var point string
			if err := h.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='task.ready'`, catalogRuntimeRunID).Scan(&point); err != nil {
				t.Fatal(err)
			}
			assertPending := func() {
				t.Helper()
				var count int
				if err := h.db.QueryRowContext(ctx, `SELECT count(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2 AND subscriber_type='agent' AND subscriber_id='test-agent' AND status='pending'`, catalogRuntimeRunID, point).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("source pending deliveries=%d want=1", count)
				}
			}
			assertPending()
			var artifact interface {
				storetest.DurableDataCatalogStore
				runforkexecution.SourceArtifactSelectedContractSourceStore
			} = h.pg
			if h.sqlite != nil {
				artifact = h.sqlite
			}
			loader, selection, _ := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, artifact)
			cfg := testRuntimeConfig()
			cfg.LLM.Backend = "anthropic"
			result, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: point, AllowSourceFreeze: true,
				Owner: selectedContractExecutionOwnerForCatalogHarness(t, h), SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg),
			})
			if err != nil {
				t.Fatal(err)
			}
			if !result.Activation.Activated || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
				t.Fatalf("root agent input execution: %+v", result)
			}
			child := result.Materialization.ForkRunID
			if child == "" || child == catalogRuntimeRunID {
				t.Fatal("missing exact selected child")
			}
			var count int
			if err := h.db.QueryRowContext(ctx, `SELECT count(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND subscriber_id='test-agent' AND status=$2`, child, string(deliverylifecycle.StatusDelivered)).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("selected root agent deliveries=%d want=1", count)
			}
			if err := h.db.QueryRowContext(ctx, `SELECT count(*) FROM run_fork_selected_contract_executions WHERE source_run_id=$1 AND fork_run_id=$2 AND source_event_id=$3 AND fork_event_id=$4`, catalogRuntimeRunID, child, point, result.ForkEvents[0].ForkEventID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("exact selected input lineage rows=%d want=1", count)
			}
			assertPending()
			if err := h.db.QueryRowContext(ctx, `SELECT count(*) FROM event_deliveries WHERE run_id=$1 AND status='delivered'`, catalogRuntimeRunID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("selected execution delivered source work")
			}
		})
	}
}
