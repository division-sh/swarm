package serveapp

import (
	"fmt"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedCompiledTransitionNestedCarrierCollisionOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleNestedCascade(t))
			var runID string
			var decisions []map[string]any
			var gateEntities []string
			for _, side := range []string{"left", "right"} {
				prefix := "outer/" + side + "/"
				seedParams := map[string]any{"event_name": prefix + "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": side + "-seed"}
				if runID != "" {
					seedParams["run_id"] = runID
					delete(seedParams, "bundle_hash")
				}
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, seedParams)
				if runID != "" && seed.RunID != runID {
					t.Fatal("sibling created a separate run")
				}
				runID = seed.RunID
				entityID := requireLifecycleFlowEntity(t, rt, runID, prefix, "waiting")
				publish := func(event, key string, payload map[string]any) {
					requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": prefix + event, "run_id": runID, "source_event_id": seed.EventID, "payload": payload, "idempotency_key": side + "-" + key})
				}
				publish("loop.start", "start", map[string]any{"seed": true})
				requireLifecycleFlowEntity(t, rt, runID, prefix, "drafting")
				for attempt := 1; attempt <= 2; attempt++ {
					loop := readLifecycleLoop(t, rt, runID, entityID)
					payload := map[string]any{"revision_id": loop.RevisionID}
					publish("loop.admit", fmt.Sprintf("admit-%d", attempt), payload)
					requireLifecycleFlowEntity(t, rt, runID, prefix, "review")
					publish("loop.repeat", fmt.Sprintf("repeat-%d", attempt), payload)
					state := "drafting"
					if attempt == 2 {
						state = "escaped"
					}
					requireLifecycleFlowEntity(t, rt, runID, prefix, state)
				}
				closed := readLifecycleLoop(t, rt, runID, entityID)
				gateEntity := requireLifecycleFlowEntity(t, rt, runID, prefix+"sink/", "review")
				gateEntities = append(gateEntities, gateEntity)
				var entity operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": gateEntity}, &entity)
				if entity.Fields["revision_id"] != closed.RevisionID {
					t.Fatalf("sibling gate consumed wrong revision: %#v loop=%#v", entity, closed)
				}
				history := readLifecycleTransitionHistory(t, rt, runID, entityID)
				if len(history) != 5 {
					t.Fatalf("nested loop history=%#v", history)
				}
				compiled, ok := history[4].Evidence.Compiled()
				if !ok || compiled.FlowID() != "outer/"+side || compiled.Edge().Source != "loop.escape" || compiled.Edge().LoopID != "revision" {
					t.Fatalf("nested escape borrowed sibling cause=%#v", compiled)
				}
				var cards struct {
					Items []struct {
						DecisionCard struct {
							CardID string `json:"card_id"`
						} `json:"decision_card"`
					} `json:"items"`
				}
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", map[string]any{"run_id": runID, "entity_id": gateEntity, "status": "pending"}, &cards)
				if len(cards.Items) != 1 {
					t.Fatalf("nested gate cards=%#v", cards)
				}
				decision := lifecycleDecisionParamsForCard(t, rt, cards.Items[0].DecisionCard.CardID, "approve")
				decision["idempotency_key"] = side + "-decide"
				decisions = append(decisions, decision)
			}
			if decisions[0]["card_id"] == decisions[1]["card_id"] {
				t.Fatal("nested sibling cards aliased")
			}
			var workers sync.WaitGroup
			start := make(chan struct{})
			for _, params := range decisions {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					var result map[string]any
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &result)
				}()
			}
			close(start)
			workers.Wait()
			if t.Failed() {
				return
			}
			for index, side := range []string{"left", "right"} {
				prefix := "outer/" + side + "/"
				requireLifecycleFlowEntity(t, rt, runID, prefix+"sink/", "approved")
				final := requireLifecycleFlowEntity(t, rt, runID, prefix+"sink/final/", "done")
				var entity operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": final}, &entity)
				if entity.Fields["result"] != side {
					t.Fatalf("sibling verdict reached wrong third flow=%#v", entity)
				}
				history := readLifecycleTransitionHistory(t, rt, runID, gateEntities[index])
				if len(history) != 2 {
					t.Fatalf("nested gate history=%#v", history)
				}
				compiled, ok := history[1].Evidence.Compiled()
				if !ok || compiled.FlowID() != prefix+"sink" || compiled.Edge().DecisionID != "review_decision" || compiled.Edge().Verdict != "approve" {
					t.Fatalf("nested gate borrowed sibling cause=%#v", compiled)
				}
				requireLifecycleEventCount(t, rt, runID, prefix+"loop.escaped", 1)
				requireLifecycleEventCount(t, rt, runID, prefix+"sink/work.completed", 1)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, runID)
			var entities int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, runID).Scan(&entities); err != nil {
				t.Fatal(err)
			}
			if entities != 6 {
				t.Fatalf("nested source/gate/final recipients=%d", entities)
			}
			before := lifecycleStoredSnapshot(t, rt, runID)
			for _, params := range decisions {
				var result map[string]any
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &result)
			}
			if lifecycleStoredSnapshot(t, rt, runID) != before {
				t.Fatal("duplicate sibling verdict changed history or consumer state")
			}
		})
	}
}
