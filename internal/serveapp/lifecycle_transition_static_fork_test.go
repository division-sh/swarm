package serveapp

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// Static business execution is supported independently of selected-fork deferred
// gate/timer controls. Keep the original lifecycle-fork future assertions too.
func TestServedCompiledTransitionStaticForkEvidenceOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleSelectedCarriers(t, false))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "static-fork-seed",
			})
			scopes := []string{"", "child/", "sibling/", "child/nested/"}
			for _, scope := range scopes {
				requireLifecycleFlowEntity(t, rt, seed.RunID, scope, "waiting")
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "static-fork-pause"})
			frontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.first", "run_id": seed.RunID, "payload": map[string]any{"choice": "alpha"}, "idempotency_key": "static-fork-frontier",
			})
			before := lifecycleStoredSnapshot(t, rt, seed.RunID)
			params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier.EventID, "allow_source_freeze": true, "idempotency_key": "static-fork"}
			var fork, duplicate apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			if fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ExecutedEventCount != 1 {
				t.Fatalf("invalid static fork result: %+v", fork)
			}
			var childEvent string
			if err := rt.DB.QueryRow(`SELECT fork_event_id FROM run_fork_selected_contract_executions WHERE fork_run_id=$1 AND source_event_id=$2`, fork.ForkRunID, frontier.EventID).Scan(&childEvent); err != nil {
				t.Fatal(err)
			}
			for _, scope := range scopes {
				entityID := requireLifecycleFlowEntity(t, rt, fork.ForkRunID, scope, "done")
				history := readLifecycleTransitionHistory(t, rt, fork.ForkRunID, entityID)
				if len(history) != 2 {
					t.Fatalf("scope %q history: %+v", scope, history)
				}
				flow := strings.TrimSuffix(scope, "/")
				if flow == "" {
					flow = "."
				}
				selected := history[0].Evidence.RuleSelection()
				if history[0].From != "waiting" || history[0].To != "active" || history[0].TriggerEventID != childEvent ||
					history[0].Evidence.FlowID() != flow || selected.Ref().Flow().String() != flow || selected.DisplayLabel() != "alpha" ||
					selected.Ref().SemanticPath() != `nodes["controller"].handlers["work.first"].rules[0]` ||
					!reflect.DeepEqual(history[0].GuardsEvaluated, []string{"first_choice", "first_nonempty"}) || history[1].From != "active" || history[1].To != "done" {
					t.Fatalf("scope %q lost exact child transition cause: %+v", scope, history)
				}
				var entity operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": fork.ForkRunID, "entity_id": entityID}, &entity)
				if entity.Fields["result"] != scope+"first/alpha" {
					t.Fatalf("scope %q wrong public result: %+v", scope, entity)
				}
				requireLifecycleEventCount(t, rt, fork.ForkRunID, scope+"work.completed", 1)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
			childBefore := lifecycleStoredSnapshot(t, rt, fork.ForkRunID)
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &duplicate)
			if !reflect.DeepEqual(fork, duplicate) || lifecycleStoredSnapshot(t, rt, seed.RunID) != before || lifecycleStoredSnapshot(t, rt, fork.ForkRunID) != childBefore {
				t.Fatal("duplicate fork changed child evidence or source state")
			}
		})
	}
}
