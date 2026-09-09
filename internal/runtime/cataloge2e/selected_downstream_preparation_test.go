package cataloge2e

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestSelectedDownstreamPreparationFailsBeforeMaterializationBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyReceiverMaterializationWithAgent(t, "renamed-observer")
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
			selected := runScopedCatalogStore(t, h)
			if _, err := selected.PauseRunControl(ctx, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID, Reason: "preparation before materialization", ControlledBy: "cataloge2e"}); err != nil {
				t.Fatal(err)
			}
			if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "start.seeded", Payload: map[string]any{"token": "preparation"}}, 20*time.Second, true); err != nil {
				t.Fatal(err)
			}
			var inputID string
			if err := h.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='start.seeded'`, catalogRuntimeRunID).Scan(&inputID); err != nil {
				t.Fatal(err)
			}
			var sourceStore interface {
				storetest.DurableDataCatalogStore
				forkexecution.SourceArtifactSelectedContractSourceStore
			} = h.pg
			var lifecycle forkexecution.SelectedContractForkLifecycle = h.pg
			if h.sqlite != nil {
				sourceStore, lifecycle = h.sqlite, h.sqlite
			}
			loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, sourceStore)
			installCatalogSelectedSourceTopology(t, ctx, h, loaded)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h, lifecycle)
			counts := func() map[string]int {
				t.Helper()
				out := make(map[string]int)
				for _, table := range []string{"runs", "events", "event_deliveries", "agents", "entity_state", "run_fork_selected_contract_bindings", "run_fork_selected_contract_runtime_executions", "runtime_generation_grants"} {
					var count int
					if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
						t.Fatal(err)
					}
					out[table] = count
				}
				return out
			}
			before := counts()
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			options.Config, options.AgentFactory = nil, nil
			result, err := forkexecution.ExecuteSelectedContractRunFork(ctx, forkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: inputID, AllowSourceFreeze: true,
				Owner: owner, SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: options,
			})
			if err == nil || !strings.Contains(err.Error(), "missing selected-fork agent factory/runtime configuration") {
				t.Fatalf("missing downstream configuration must fail at preparation: %v", err)
			}
			if result.Materialization.ForkRunID != "" || result.ExecutedEventCount != 0 {
				t.Fatalf("preparation failure created/executed a fork: %+v", result)
			}
			if after := counts(); !reflect.DeepEqual(before, after) {
				t.Fatalf("preparation failure mutated domain: before=%v after=%v", before, after)
			}
			options = selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			options.Config.LLM.Backend = "anthropic"
			prepared, err := owner.Prepare(ctx, forkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: inputID, AllowSourceFreeze: true,
				Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
			})
			if err != nil {
				t.Fatalf("complete downstream preparation: %v", err)
			}
			defer func() {
				if err := prepared.Close(); err != nil {
					t.Error(err)
				}
			}()
			request, err := prepared.MaterializationRequest()
			if err != nil || len(request.Preparation.Actors) != 1 {
				t.Fatalf("missing prepared downstream actor: %v, %v", request.Preparation.Actors, err)
			}
			initial, err := request.RecipientPlanning.SelectedAgentPlans()
			if err != nil || len(initial) != 0 {
				t.Fatalf("node-only frontier acquired initial agent dispatch: %v, %v", initial, err)
			}
			for _, change := range []string{"empty_census", "missing_census", "duplicate_actor", "configuration", "same_name_foreign_owner", "unknown_template_instance"} {
				t.Run(change, func(t *testing.T) {
					bad := request
					bad.Preparation.Actors = append([]runfork.SelectedForkPreparedActor(nil), request.Preparation.Actors...)
					switch change {
					case "empty_census":
						bad.Preparation.Actors = []runfork.SelectedForkPreparedActor{}
					case "missing_census":
						bad.Preparation.Actors = nil
					case "duplicate_actor":
						bad.Preparation.Actors = append(bad.Preparation.Actors, bad.Preparation.Actors[0])
					case "configuration":
						bad.Preparation.Actors[0].ConfigurationRevision = strings.Repeat("d", 64)
					case "same_name_foreign_owner":
						bad.Preparation.Actors[0].Plan.Name.Owner += "-foreign"
					case "unknown_template_instance":
						route, err := flowidentity.StoredRoute("consumer", "unknown", "consumer/unknown").AgentIdentityRoute()
						if err != nil {
							t.Fatal(err)
						}
						bad.Preparation.Actors[0].Plan.Route = route
					}
					if _, err := lifecycle.MaterializeRunForkForSelectedContractExecution(ctx, bad); err == nil {
						t.Fatalf("named materialization admitted %s", change)
					}
					if after := counts(); !reflect.DeepEqual(before, after) {
						t.Fatalf("rejected census mutated domain: before=%v after=%v", before, after)
					}
				})
			}
			materialized, err := lifecycle.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil || materialized.ForkRunID == "" {
				t.Fatalf("exact prepared census cannot materialize: %+v, %v", materialized, err)
			}
		})
	}
}
