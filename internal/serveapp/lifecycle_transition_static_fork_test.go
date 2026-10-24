package serveapp

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// Static business execution is supported independently of selected-fork deferred
// gate/timer controls. Keep the original lifecycle-fork future assertions too.
func TestServedCompiledTransitionStaticForkEvidenceOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			publication := newExactOriginPublicationProbe(t)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleSelectedCarriers(t, false))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "static-fork-seed",
			})
			scopes := []string{"", "child/", "sibling/", "child/nested/"}
			for _, scope := range scopes {
				requireLifecycleFlowEntity(t, rt, seed.RunID, scope, "waiting")
			}
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "static-fork-pause"})
			frontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.first", "run_id": seed.RunID, "payload": map[string]any{"choice": "alpha"}, "idempotency_key": "static-fork-frontier",
			})
			waitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, frontier.EventID)
			if err := waitExactOriginPublications(t.Context(), publication, seed.EventID, frontier.EventID); err != nil {
				t.Fatal(err)
			}
			before := lifecycleStoredSnapshot(t, rt, seed.RunID)
			params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier.EventID, "allow_source_freeze": true, "idempotency_key": "static-fork"}
			fork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, params)
			if fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ExecutedEventCount != 0 {
				t.Fatalf("invalid static fork result: %+v", fork)
			}
			childEvent := waitServedSelectedForkFrontier(t, rt.ReceiverStateReader, rt.ReadRunDeliveries, fork.ForkRunID, frontier.EventID)
			var storedChildEvent string
			if err := rt.DB.QueryRow(`SELECT fork_event_id FROM run_fork_selected_contract_executions WHERE fork_run_id=$1 AND source_event_id=$2`, fork.ForkRunID, frontier.EventID).Scan(&storedChildEvent); err != nil {
				t.Fatal(err)
			}
			if storedChildEvent != childEvent {
				t.Fatalf("stored child frontier=%q want %q", storedChildEvent, childEvent)
			}
			for _, scope := range scopes {
				entityID := requireLifecycleFlowEntity(t, rt, fork.ForkRunID, scope, "done")
				history := readLifecycleTransitionHistory(t, rt.ReceiverStateReader, fork.ForkRunID, entityID)
				if len(history) != 1 || history[0].From != "active" || history[0].To != "done" {
					t.Fatalf("scope %q history: %+v", scope, history)
				}
				flow := strings.TrimSuffix(scope, "/")
				if flow == "" {
					flow = "."
				}
				first := readLifecycleTransitionAtCut(t, rt.ReceiverStateReader, fork.ForkRunID, entityID, history[0].TriggerEventID)
				selected := first.Evidence.RuleSelection()
				if first.From != "waiting" || first.To != "active" || first.TriggerEventID != childEvent ||
					first.Evidence.FlowID() != flow || selected.Ref().Flow().String() != flow || selected.DisplayLabel() != "rules[0]: when payload.choice == 'alpha'" ||
					selected.Ref().SemanticPath() != `nodes["controller"].handlers["work.first"].rules[0]` ||
					!reflect.DeepEqual(first.GuardsEvaluated, []string{"first_choice", "first_nonempty"}) {
					t.Fatalf("scope %q lost exact child transition cause: %+v", scope, history)
				}
				var entity operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, rt.Endpoint, "state.get", map[string]any{"run_id": fork.ForkRunID, "entity_id": entityID}, &entity)
				if entity.Fields["result"] != scope+"first/alpha" {
					t.Fatalf("scope %q wrong public result: %+v", scope, entity)
				}
				requireLifecycleEventCount(t, rt, fork.ForkRunID, scope+"work.completed", 1)
			}
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, fork.ForkRunID)
			childBefore := lifecycleStoredSnapshot(t, rt, fork.ForkRunID)
			duplicate := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, params)
			if !reflect.DeepEqual(fork, duplicate) || lifecycleStoredSnapshot(t, rt, seed.RunID) != before || lifecycleStoredSnapshot(t, rt, fork.ForkRunID) != childBefore {
				t.Fatal("duplicate fork changed child evidence or source state")
			}
		})
	}
}
