package serveapp

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedCompiledTransitionForkIsolationOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, gate := range []bool{false, true} {
			name := "open_loop"
			if gate {
				name = "frozen_gate"
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleForkSource(t, gate))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-seed"})
				stage := "waiting"
				if gate {
					stage = "review"
				}
				entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", stage)
				var parentCard map[string]any
				if gate {
					parentCard = lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
				} else {
					requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "loop.start", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-loop-start"})
					stage = "drafting"
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, stage)
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "fork-pause"})
				frontierEvent := "work.observed"
				frontierPayload := map[string]any{"seed": true}
				if !gate {
					frontierEvent = "loop.admit"
					frontierPayload = map[string]any{"revision_id": readLifecycleLoop(t, rt, seed.RunID, entityID).RevisionID}
				}
				frontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": frontierEvent, "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": frontierPayload, "idempotency_key": "fork-frontier"})
				before := lifecycleStoredSnapshot(t, rt, seed.RunID)
				params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier.EventID, "confirm_source_freeze": true, "idempotency_key": "lifecycle-fork"}
				var fork, duplicate apiv1.RunForkExecutionResult
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &duplicate)
				if fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ExecutedEventCount != 1 || fork.ForkRunID != duplicate.ForkRunID {
					t.Fatalf("fork=%#v duplicate=%#v", fork, duplicate)
				}
				forkEntity := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, fork.ForkRunID, "", "review")
				if gate {
					childCard := lifecycleGateDecisionParams(t, rt, fork.ForkRunID, "approve")
					if childCard["card_id"] == parentCard["card_id"] {
						t.Fatal("fork retained parent card identity")
					}
					var decision map[string]any
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", childCard, &decision)
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, fork.ForkRunID, forkEntity, "done")
					requireLifecycleEventCount(t, rt, fork.ForkRunID, "work.completed", 1)
					requireLifecycleEventCount(t, rt, seed.RunID, "work.completed", 0)
					var entity operatorread.OperatorEntityFull
					requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": fork.ForkRunID, "entity_id": forkEntity}, &entity)
					if entity.Fields["result"] != "approved" {
						t.Fatalf("fork gate consumer=%#v", entity)
					}
					history := readLifecycleTransitionHistory(t, rt, fork.ForkRunID, forkEntity)
					if len(history) < 2 {
						t.Fatalf("fork gate history=%#v", history)
					}
					compiled, ok := history[len(history)-2].Evidence.Compiled()
					if !ok || compiled.FlowID() != "." || compiled.Edge().Source != "gate" || compiled.Edge().DecisionID != "review_decision" || compiled.Edge().Verdict != "approve" {
						t.Fatalf("fork gate source=%#v", compiled)
					}
					stillPending := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
					if stillPending["card_id"] != parentCard["card_id"] || stillPending["observed_content_hash"] != parentCard["observed_content_hash"] {
						t.Fatal("fork mutated parent pending card")
					}
				} else {
					parent := readLifecycleLoop(t, rt, seed.RunID, entityID)
					child := readLifecycleLoop(t, rt, fork.ForkRunID, forkEntity)
					if parent.RevisionID == child.RevisionID || child.Attempt != parent.Attempt || child.MaxAttempts != parent.MaxAttempts {
						t.Fatalf("loop fork=%#v parent=%#v", child, parent)
					}
					parentActivation := readLifecycleStoredLoop(t, rt, seed.RunID, entityID)
					childActivation := readLifecycleStoredLoop(t, rt, fork.ForkRunID, forkEntity)
					if parentActivation.ActivationID == childActivation.ActivationID || childActivation.RevisionID != child.RevisionID {
						t.Fatal("fork loop activation did not remint consistently")
					}
					var sourceEvent string
					if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='loop.admit'`, fork.ForkRunID).Scan(&sourceEvent); err != nil {
						t.Fatal(err)
					}
					closedEvent := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "loop.close", "run_id": fork.ForkRunID, "source_event_id": sourceEvent, "payload": map[string]any{"revision_id": child.RevisionID}, "idempotency_key": "fork-close"})
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, fork.ForkRunID, forkEntity, "done")
					history := readLifecycleTransitionHistory(t, rt, fork.ForkRunID, forkEntity)
					if len(history) == 0 || history[len(history)-1].From != "review" || history[len(history)-1].To != "done" || history[len(history)-1].Evidence.FlowID() != "." {
						t.Fatalf("fork cause=%#v", history)
					}
					compiled, ok := history[len(history)-1].Evidence.Compiled()
					if !ok || compiled.Edge().LoopID != "revision" || string(compiled.Edge().LoopOperation) != "close" || history[len(history)-1].TriggerEventID != closedEvent.EventID {
						t.Fatalf("fork close cause=%#v", compiled)
					}
					closed := readLifecycleLoop(t, rt, fork.ForkRunID, forkEntity)
					if closed.Status != loopruntime.StatusClosed || closed.RevisionID != child.RevisionID {
						t.Fatalf("fork public closed loop=%#v", closed)
					}
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
				if lifecycleStoredSnapshot(t, rt, seed.RunID) != before {
					t.Fatal("fork execution mutated parent history/state")
				}
			})
		}
	}
}

func readLifecycleStoredLoop(t *testing.T, rt servedControlProofRuntime, runID, entityID string) loopruntime.Activation {
	t.Helper()
	var raw string
	if err := rt.DB.QueryRow(`SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var bucket map[string]map[string]any
	if err := json.Unmarshal([]byte(raw), &bucket); err != nil {
		t.Fatal(err)
	}
	loop, found, err := loopruntime.Load(bucket, ".", "revision")
	if err != nil || !found {
		t.Fatalf("loop missing: %v %s", err, raw)
	}
	return loop
}
