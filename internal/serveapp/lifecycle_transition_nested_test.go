package serveapp

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedCompiledTransitionNestedCarrierCollisionOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			probe := lifecycletest.New(t)
			configureOwnedMockLifecycleProbe(t, probe)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleNestedCascade(t))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "left.work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "tree-seed"})
			runID := seed.RunID
			wantStages := map[string]string{
				runID: "pending", "outer": "pending",
				"outer/left": "waiting", "outer/left/sink": "waiting", "outer/left/sink/final": "waiting",
				"outer/right": "waiting", "outer/right/sink": "waiting", "outer/right/sink/final": "waiting",
			}
			rows, err := rt.DB.Query(`SELECT f.instance_path,f.current_state,r.phase FROM flow_instances f
				JOIN flow_instance_runtime_readiness r ON r.run_id=f.run_id AND r.instance_path=f.instance_path
				WHERE f.run_id=$1 ORDER BY f.instance_path`, runID)
			if err != nil {
				t.Fatal(err)
			}
			gotStages := map[string]string{}
			for rows.Next() {
				var path, stage, phase string
				if err := rows.Scan(&path, &stage, &phase); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				if phase != "ready" {
					rows.Close()
					t.Fatalf("initial tree %s attachment phase=%s", path, phase)
				}
				gotStages[path] = stage
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotStages, wantStages) {
				t.Fatalf("one constructor did not create the complete initial tree: got=%v want=%v", gotStages, wantStages)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, runID)
			beforeRedundantSeed := settledStaticRunSnapshot(t, rt, probe, seed)
			refusal := requireServedJSONRPCError(t, rt.Endpoint, "event.publish", map[string]any{"event_name": "right.work.requested", "run_id": runID, "payload": map[string]any{"seed": true}, "idempotency_key": "redundant-tree-seed"})
			details, ok := refusal.Data["details"].(map[string]any)
			if !ok || refusal.Data["code"] != "EVENT_NOT_DECLARED" || details["reason"] != "declared_event_has_no_selected_run_recipient" {
				t.Fatalf("handler-free redundant seed refusal=%#v", refusal)
			}
			if !reflect.DeepEqual(repeatedStaticRunSnapshot(t, rt.DB, runID), beforeRedundantSeed) {
				t.Fatal("redundant tree seed changed canonical construction or delivery evidence")
			}
			var decisions []map[string]any
			var gateEntities []string
			for _, side := range []string{"left", "right"} {
				prefix := "outer/" + side + "/"
				entityID := requireLifecycleFlowEntity(t, rt, runID, prefix, "waiting")
				publish := func(event, key string, payload map[string]any) {
					requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": side + "." + event, "run_id": runID, "source_event_id": seed.EventID, "payload": payload, "idempotency_key": side + "-" + key})
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
