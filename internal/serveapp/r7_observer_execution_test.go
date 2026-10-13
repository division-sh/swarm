package serveapp

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestR7ServedObserverOrdersExecutionAndRestartBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, observerFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/observer_first=%t", backend, observerFirst), func(t *testing.T) {
				root := canonicalrouting.CopyExecutableObserverDependencies(t)
				requireA2PortfolioVerification(t, root)
				source := semanticview.Wrap(loadWorkflowValidationBundleAt(t, root))
				proof := startWorkspaceGatewayProofRuntime(t, backend, root, "", nil, "127.0.0.1:0")
				var runID string
				publish := func(event, key string, payload map[string]any) servedEventPublishRPCResult {
					t.Helper()
					params := map[string]any{"event_name": event, "idempotency_key": key, "payload": payload}
					if runID == "" {
						params["bundle_hash"] = proof.BundleHash
					} else {
						params["run_id"] = runID
					}
					result := requireServedEventPublishRPCResult(t, proof.Endpoint, params)
					if runID != "" && result.RunID != runID {
						t.Fatalf("observer publication changed its admitted run: %+v", result)
					}
					runID = result.RunID
					waitWorkspaceProofPipelineHandoff(t, proof, runID)
					return result
				}
				if observerFirst {
					publish("observer.create", "observer", map[string]any{"item_id": "one"})
				}
				for _, flow := range []string{"producer", "other"} {
					for _, key := range []string{"old-a", "old-b"} {
						publish(flow+".create", flow+"-"+key, map[string]any{"item_id": key})
					}
				}
				if !observerFirst {
					publish("observer.create", "observer", map[string]any{"item_id": "one"})
				}
				original := r7ObserverNativeInventory(t, proof, source, runID)
				if len(original) != 6 {
					t.Fatalf("original observer inventory=%d, want root plus five receivers", len(original))
				}
				observer := r7ObserverReceiptForKey(t, original, "observer", "one")
				fire := func(flow, key, marker string) {
					t.Helper()
					producer := r7ObserverReceiptForKey(t, original, flow, key)
					trigger := publish(flow+".fire", marker, map[string]any{"item_id": key, "observer_id": "one", "marker": marker})
					local := "work.ready"
					if flow == "other" {
						local = "other.ready"
					}
					incoming := r7ObserverPublicEmission(t, proof.Endpoint, runID, trigger.EventID, producer.Identity.InstancePath+"/"+local)
					r7RequireObserverDelivery(t, incoming, "observer", "observe", observer.Identity.InstancePath, observer.Identity.EntityID)
					outcome := r7ObserverPublicEmission(t, proof.Endpoint, runID, incoming.EventID, observer.Identity.InstancePath+"/observation.logged")
					if !reflect.DeepEqual(incoming.Payload, map[string]any{"item_id": "one", "marker": marker}) || !reflect.DeepEqual(outcome.Payload, incoming.Payload) {
						t.Fatalf("real observer handler lost its admitted input: incoming=%+v outcome=%+v", incoming, outcome)
					}
					r7RequireObserverDelivery(t, outcome, ".", "observations", runID, runID)
					if got := r7ObserverNativeInventory(t, proof, source, runID); !reflect.DeepEqual(got, original) {
						t.Fatal("handler execution changed an original construction receipt or created a receiver")
					}
				}
				for _, flow := range []string{"producer", "other"} {
					for _, key := range []string{"old-a", "old-b"} {
						fire(flow, key, "before-"+flow+"-"+key)
					}
				}
				publish("producer.create", "new-producer", map[string]any{"item_id": "new"})
				withNew := r7ObserverNativeInventory(t, proof, source, runID)
				if len(withNew) != 7 {
					t.Fatalf("late producer inventory=%d, want seven", len(withNew))
				}
				for path, receipt := range original {
					if !reflect.DeepEqual(withNew[path], receipt) {
						t.Fatalf("late producer replaced original receipt %s", path)
					}
				}
				original = withNew
				fire("producer", "new", "late-producer")
				proof = proof.Restart()
				if got := r7ObserverNativeInventory(t, proof, source, runID); !reflect.DeepEqual(got, original) {
					t.Fatal("joined shutdown/restart reminted original construction")
				}
				for _, flow := range []string{"producer", "other"} {
					for _, key := range []string{"old-a", "old-b"} {
						fire(flow, key, "after-"+flow+"-"+key)
					}
				}
				fire("producer", "new", "after-late-producer")
				publish("observer.stop", "stop-observer", map[string]any{"item_id": "one"})
				reader := requireReceiverProofStateReader(t, proof.Events)
				_, completed := requireReceiverTargetState(t, reader, runID, "observer", observer.Identity.InstancePath, observer.Identity.EntityID)
				if completed.CurrentState != "done" || completed.Status != "active" || !completed.TerminatedAt.IsZero() {
					t.Fatalf("final-stage entry synthesized observer retirement: %+v", completed)
				}
				ctx := correlation.WithRunID(workspaceProofAuthorActivityContext(t, proof), runID)
				if err := proof.Runtime.Manager.DeactivateFlowInstanceModel(ctx, pipeline.FlowInstanceDeactivationRequest{
					ContractBundle: source, Instance: observer.Identity, FinalState: "done",
				}); err != nil {
					t.Fatalf("retire exact observer through its joined lifecycle owner: %v", err)
				}
				proof = proof.Restart()
				reader = requireReceiverProofStateReader(t, proof.Events)
				_, stopped := requireReceiverTargetState(t, reader, runID, "observer", observer.Identity.InstancePath, observer.Identity.EntityID)
				if stopped.CurrentState != "done" || stopped.Status != "terminated" || stopped.TerminatedAt.IsZero() {
					t.Fatalf("restart lost the exact retired observer: %+v", stopped)
				}
				if got := r7ObserverNativeInventory(t, proof, source, runID); !reflect.DeepEqual(got, original) {
					t.Fatal("retirement or restart rewrote immutable construction or erased occupancy")
				}
			})
		}
	}
}

func r7ObserverNativeInventory(t *testing.T, proof servedWorkspaceProofRuntime, source semanticview.Source, runID string) map[string]pipeline.FlowConstructionPublicationEvidence {
	t.Helper()
	index, ok := proof.Events.(pipeline.FlowInstanceIndexReader)
	if !ok {
		t.Fatal("observer proof requires the native instance index")
	}
	scope, err := pipeline.NewFlowInstanceLookupScope(source, proof.Runtime.Options.SourceArtifactFact, runID, []string{".", "producer", "other", "observer"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	observations, err := index.ListFlowInstances(correlation.WithRunID(context.Background(), runID), scope)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]pipeline.FlowConstructionPublicationEvidence, len(observations))
	for _, observation := range observations {
		receipt, native, err := observation.NativeConstruction()
		if err != nil || !native || !observation.Valid() || receipt.Identity != observation.Identity() {
			t.Fatalf("observer inventory lost exact native receipt: %+v %v", observation.Identity(), err)
		}
		if _, duplicate := result[receipt.Identity.InstancePath]; duplicate {
			t.Fatal("observer inventory repeated an exact construction coordinate")
		}
		result[receipt.Identity.InstancePath] = receipt
	}
	return result
}

func r7ObserverReceiptForKey(t *testing.T, inventory map[string]pipeline.FlowConstructionPublicationEvidence, flow, key string) pipeline.FlowConstructionPublicationEvidence {
	t.Helper()
	var found []pipeline.FlowConstructionPublicationEvidence
	for _, receipt := range inventory {
		if receipt.Identity.TemplateID == flow && receipt.Fields["item_id"] == key {
			found = append(found, receipt)
		}
	}
	if len(found) != 1 {
		t.Fatalf("native receiver %s/%s has %d original receipts", flow, key, len(found))
	}
	return found[0]
}

func r7ObserverPublicEmission(t *testing.T, endpoint, runID, sourceID, name string) operatorread.OperatorEventFull {
	t.Helper()
	var matches []operatorread.OperatorEventFull
	cursor := ""
	for {
		var page operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 100, "cursor": cursor}, &page)
		for _, event := range page.Events {
			if event.EventName == name && event.SourceEventID == sourceID {
				matches = append(matches, event)
			}
		}
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor {
			t.Fatal("public observer event readback failed to advance its cursor")
		}
		cursor = page.NextCursor
	}
	if len(matches) != 1 {
		t.Fatalf("exact observer emission %s from %s: got%d", name, sourceID, len(matches))
	}
	return matches[0]
}

func r7RequireObserverDelivery(t *testing.T, event operatorread.OperatorEventFull, flow, node, path, entity string) {
	t.Helper()
	if len(event.Deliveries) != 1 || event.NoDelivery != nil || len(event.DeadLetters) != 0 {
		t.Fatalf("observer event lacks one successful delivery: %+v", event)
	}
	delivery := event.Deliveries[0]
	if delivery.SubscriberID != identitytest.FlowNode(t, flow, node).Key() || delivery.Target.FlowID != flow || delivery.Target.FlowInstance != path || delivery.Target.EntityID != entity || delivery.Status != "delivered" || delivery.Failure != nil || len(delivery.DeadLetters) != 0 {
		t.Fatalf("observer handler did not execute on the exact receiver: %+v", delivery)
	}
}
