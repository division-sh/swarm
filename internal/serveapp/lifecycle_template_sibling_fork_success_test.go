package serveapp

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// Keep the original T17 success oracle active. Construction is read through
// the canonical header reader rather than reproducing the old fixture's join.
func TestServedCompiledTransitionTemplateSiblingForkOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			publication := newExactOriginPublicationProbe(t)
			rt := startLifecycleTemplateRuntime(t, backend, canonicalrouting.CopyLifecycleNestedTemplates(t))
			runID, sourceEvent, siblings := prepareLifecycleTemplateSiblings(t, rt)
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, runID)
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": runID, "idempotency_key": "template-pause"})
			frontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.observed", "run_id": runID, "source_event_id": sourceEvent, "payload": map[string]any{"seed": true}, "idempotency_key": "template-frontier"})
			waitServedEventPublishedLog(t, rt.Endpoint, runID, frontier.EventID)
			origins := []string{frontier.EventID}
			for _, sibling := range siblings {
				if len(sibling.publications) != 5 {
					t.Fatalf("sibling %s lost an acknowledged source publication: %v", sibling.side, sibling.publications)
				}
				origins = append(origins, sibling.publications...)
			}
			if err := waitExactOriginPublications(t.Context(), publication, origins...); err != nil {
				t.Fatal(err)
			}
			before := lifecycleStoredSnapshot(t, rt, runID)
			params := map[string]any{"source_run_id": runID, "fork_event_id": frontier.EventID, "allow_source_freeze": true, "idempotency_key": "template-fork"}
			var fork, duplicate apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &duplicate)
			if fork.ForkRunID == "" || fork.ForkRunID == runID || fork.ForkRunID != duplicate.ForkRunID || fork.ExecutedEventCount != 1 {
				t.Fatalf("template fork=%#v duplicate=%#v", fork, duplicate)
			}
			instances, err := rt.ReceiverStateReader.ListWorkflowInstances(t.Context(), fork.ForkRunID)
			if err != nil {
				t.Fatal(err)
			}
			var children []lifecycleTemplateSibling
			for _, parent := range siblings {
				child, matches := parent, 0
				for _, instance := range instances {
					if instance.WorkflowName == parent.flow+"/sink" && instance.CurrentState == "review" {
						child.gateEntity, child.gateInstance = instance.EntityID, instance.InstanceID
						matches++
					}
				}
				if matches != 1 {
					t.Fatalf("child lacks its unique logical gate owner %s: matches=%d instances=%+v", parent.flow+"/sink", matches, instances)
				}
				child.gate = readLifecycleTemplateGate(t, rt, fork.ForkRunID, child.gateEntity, child.flow+"/sink")
				if child.gate.CardID == parent.gate.CardID || child.gate.ActivationID == parent.gate.ActivationID || child.gateEntity == parent.gateEntity || child.gateInstance == parent.gateInstance || child.gate.RoutesJSON != parent.gate.RoutesJSON || child.gate.BundleHash != parent.gate.BundleHash {
					t.Fatalf("fork did not remint ownership while preserving exact frozen source: parent=%#v child=%#v", parent, child)
				}
				child.decision = lifecycleDecisionParamsForCard(t, rt, child.gate.CardID, "approve")
				child.decision["idempotency_key"] = child.side + "-fork-decide"
				t.Logf("TEMPLATE_FORK source_run=%s fork_run=%s side=%s parent_card=%s child_card=%s parent_activation=%s child_activation=%s parent_instance=%s child_instance=%s routes=%s", runID, fork.ForkRunID, child.side, parent.gate.CardID, child.gate.CardID, parent.gate.ActivationID, child.gate.ActivationID, parent.gateInstance, child.gateInstance, child.gate.RoutesJSON)
				children = append(children, child)
			}
			if children[0].gate.CardID == children[1].gate.CardID || children[0].gate.ActivationID == children[1].gate.ActivationID || children[0].gateInstance == children[1].gateInstance {
				t.Fatal("fork sibling gates aliased")
			}
			completeLifecycleTemplateSiblings(t, rt, fork.ForkRunID, children)
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, fork.ForkRunID)
			if lifecycleStoredSnapshot(t, rt, runID) != before {
				t.Fatal("fork sibling verdict changed parent state/history")
			}
			for _, parent := range siblings {
				requireLifecycleEventCount(t, rt, runID, parent.flow+"/sink/work.completed", 0)
				pending := readLifecycleTemplateGate(t, rt, runID, parent.gateEntity, parent.flow+"/sink")
				if !reflect.DeepEqual(pending, parent.gate) {
					t.Fatalf("fork mutated parent frozen gate: before=%#v after=%#v", parent.gate, pending)
				}
				params := lifecycleDecisionParamsForCard(t, rt, parent.gate.CardID, "approve")
				if params["observed_content_hash"] != parent.decision["observed_content_hash"] {
					t.Fatal("fork changed parent card contents")
				}
			}
		})
	}
}
