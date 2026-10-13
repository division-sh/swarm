package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestIssue642FutureKeyedProviderReusesConstructedInstanceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			observed := issue642ObserveKeyedProviderPreparation(t)
			_, start := issue2564ServeHarness(t, backend, canonicalrouting.CopySelectedFutureReusedProviderInput(t), true)
			process, rt := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("reused-provider shutdown=%d\n%s", code, process.outputString())
				}
			})
			response := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "event.publish", map[string]any{
				"event_name": "start.requested", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"work_id": "one"}, "idempotency_key": "future-provider-reuse-source",
			}, 30*time.Second)
			if response.Error != nil {
				t.Fatalf("reused-provider source admission: %+v", response.Error)
			}
			var seed servedEventPublishRPCResult
			if err := json.Unmarshal(response.Result, &seed); err != nil {
				t.Fatal(err)
			}
			issue642WaitKeyedProviderCompletion(t, rt, seed.RunID)
			rt.waitDeliveries(t, seed.RunID)
			issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
			issue642RequireReusedProviderResult(t, rt, seed.RunID)
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if rt.Runtime == nil || rt.Runtime.Options.ProcessWorkOwner == nil {
				t.Fatal("reused-provider proof lost its process work owner")
			}
			baseline := rt.Runtime.Options.ProcessWorkOwner.ActiveCount()
			fork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, map[string]any{
				"source_run_id": seed.RunID, "at_start": true, "allow_source_freeze": true,
				"idempotency_key": "future-provider-reuse-start-cut",
			})
			terminalDeadline := time.Now().Add(servedProofPollDeadline)
			if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID ||
				fork.ForkPointKind != string(runfork.RunForkPointRunStart) || fork.ForkEventID != "" {
				t.Fatalf("reused-provider fork lost its exclusive start cut: %+v", fork)
			}
			select {
			case proof := <-observed:
				if proof.MaterializationRequired || !proof.MaterializationSupported || len(proof.ConfiguredAgentPlans) != 0 || len(proof.AgentRecipientPlans) != 0 {
					t.Fatalf("start cut invented an existing worker: %+v", proof)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("reused-provider activation did not return preparation evidence")
			}
			issue642WaitKeyedProviderCompletion(t, rt, fork.ForkRunID)
			rt.waitDeliveries(t, fork.ForkRunID)
			issue642RequireReusedProviderResult(t, rt, fork.ForkRunID)
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("reused-provider child changed its source domain: %v", err)
			}
			issue642WaitFutureProviderRetirement(t, rt, fork.ForkRunID, baseline, terminalDeadline)
		})
	}
}

func issue642RequireReusedProviderResult(t *testing.T, rt issue2564ServedFixture, runID string) {
	t.Helper()
	assigned := rt.events(t, runID, "work.assigned")
	if len(assigned) != 2 {
		t.Fatalf("reuse requires two distinct assignments: %+v", assigned)
	}
	bySequence := make(map[float64]operatorread.OperatorEventFull, 2)
	for _, event := range assigned {
		sequence, ok := event.Payload["sequence"].(float64)
		if !ok || (sequence != 1 && sequence != 2) || bySequence[sequence].EventID != "" ||
			event.RunID != runID || event.Payload["work_id"] != "one" || len(event.Deliveries) != 1 || len(event.DeadLetters) != 0 {
			t.Fatalf("assignment lost its exact occurrence: %+v", event)
		}
		delivery := event.Deliveries[0]
		if delivery.Target.FlowID != "work" || delivery.Target.FlowInstance == "" || delivery.Target.EntityID == "" ||
			!delivery.Terminal || delivery.Status != "delivered" || delivery.Failure != nil {
			t.Fatalf("assignment did not settle to its constructed worker: %+v", delivery)
		}
		bySequence[sequence] = event
	}
	first, second := bySequence[1], bySequence[2]
	firstDelivery, secondDelivery := first.Deliveries[0], second.Deliveries[0]
	target := firstDelivery.Target
	if first.EventID == second.EventID || firstDelivery.DeliveryID == secondDelivery.DeliveryID ||
		target.FlowInstance != secondDelivery.Target.FlowInstance || target.EntityID != secondDelivery.Target.EntityID ||
		firstDelivery.Route.AgentIdentity != secondDelivery.Route.AgentIdentity {
		t.Fatalf("same authored key did not reuse one exact worker: first=%+v second=%+v", firstDelivery, secondDelivery)
	}
	progress := rt.events(t, runID, target.FlowInstance+"/work.progress")
	finished := rt.events(t, runID, target.FlowInstance+"/work.finished")
	if len(progress) != 1 || len(finished) != 1 {
		t.Fatalf("two provider turns need exactly one progress and one final output: progress=%+v finished=%+v", progress, finished)
	}
	for _, output := range []operatorread.OperatorEventFull{progress[0], finished[0]} {
		if output.ProducerType != events.EventProducerAgent || output.RunID != runID || output.Payload["work_id"] != "one" ||
			output.Source != finished[0].Source || len(output.DeadLetters) != 0 {
			t.Fatalf("provider output lost its shared actor or business identity: %+v", output)
		}
		for _, delivery := range output.Deliveries {
			if !delivery.Terminal || delivery.Status != "delivered" || delivery.Failure != nil {
				t.Fatalf("provider output did not settle: %+v", delivery)
			}
		}
	}
	if len(progress[0].Deliveries) != 1 || len(finished[0].Deliveries) != 2 || finished[0].Payload["result"] != "finished" ||
		second.SourceEventID != progress[0].EventID {
		t.Fatalf("reuse must follow the actual returned progress and reach both final consumers: second=%+v progress=%+v finished=%+v", second, progress, finished)
	}
	reader, ok := rt.persistence.deps.EventStore.(receiverProofStateReader)
	if !ok {
		t.Fatalf("reuse requires the native receiver read owner: %T", rt.persistence.deps.EventStore)
	}
	_, worker := requireSingleReceiverTargetState(t, reader, runID, "work")
	if worker.EntityID != target.EntityID || worker.StorageRef != target.FlowInstance || worker.CurrentState != "done" {
		t.Fatalf("reuse created another receiver or lost final state: %+v", worker)
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(target.FlowID, flowidentity.LogicalInstanceID(target.FlowInstance), target.FlowInstance))
	if err != nil {
		t.Fatal(err)
	}
	construction, err := storetest.ReadReceiverConstructionPublication(t.Context(), rt.persistence.deps.EventStore, owner, target.EntityID)
	if err != nil || construction.CreatingInput.EventID != first.EventID || construction.Fields["work_id"] != "one" {
		t.Fatalf("second assignment replaced the immutable first construction: %+v err=%v", construction, err)
	}
	entity, err := rt.selected.LoadOperatorEntity(t.Context(), target.EntityID, runID)
	if err != nil || entity.Fields["work_id"] != "one" || entity.Fields["result"] != "finished" {
		t.Fatalf("reused worker lost the final business result: %+v err=%v", entity, err)
	}
	root, err := rt.selected.LoadOperatorEntity(t.Context(), runID, runID)
	if err != nil || root.Entity.CurrentState != "done" || root.Fields["counter"] != float64(1) {
		t.Fatalf("reused provider must finish its root exactly once: %+v err=%v", root, err)
	}
	agentReader, ok := rt.persistence.deps.EventStore.(operatorread.AgentReader)
	if !ok {
		t.Fatalf("reuse requires the native agent identity reader: %T", rt.persistence.deps.EventStore)
	}
	identity, err := agentReader.ResolveOperatorAgentIdentity(t.Context(), runID, finished[0].Source, target.FlowInstance)
	if err != nil || identity.RunID != runID || identity.FlowInstance() != target.FlowInstance || identity != firstDelivery.Route.AgentIdentity {
		t.Fatalf("reuse lost exact actor identity: %+v err=%v", identity, err)
	}
	turns, err := storetest.ReadManagedAgentTurnStorage(t.Context(), rt.persistence.deps.EventStore, runID, finished[0].Source)
	if err != nil || len(turns) != 2 {
		t.Fatalf("one reused actor must retain two real provider turns: %+v err=%v", turns, err)
	}
	triggers := map[string]bool{first.EventID: false, second.EventID: false}
	for _, turn := range turns {
		seen, exists := triggers[turn.TriggerEventID]
		if !exists || seen || len(turn.RequestPayload) == 0 || len(turn.ResponsePayload) == 0 {
			t.Fatalf("provider turn is missing or duplicates an assignment: %+v", turn)
		}
		triggers[turn.TriggerEventID] = true
	}
	t.Logf("future_provider_reuse run=%s instance=%s entity=%s assignments=2 turns=2 construction_input=%s", runID, target.FlowInstance, target.EntityID, construction.CreatingInput.EventID)
}
