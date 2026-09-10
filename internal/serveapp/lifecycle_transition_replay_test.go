package serveapp

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedCompiledLoopTransitionReplayOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopRepeatEmits))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "replay-seed"})
			entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "waiting")
			params := func(event, key string, payload map[string]any) map[string]any {
				return map[string]any{"event_name": event, "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": payload, "idempotency_key": key}
			}
			requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.start", "start", map[string]any{"seed": true}))
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "drafting")
			first := readLifecycleLoop(t, rt, seed.RunID, entityID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.admit", "admit-1", map[string]any{"revision_id": first.RevisionID}))
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "review")
			repeated := requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.repeat", "repeat-1", map[string]any{"revision_id": first.RevisionID}))
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "drafting")
			requireLifecycleFlowEntity(t, rt, seed.RunID, "ordinary/", "observed")
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			before := lifecycleStoredSnapshot(t, rt, seed.RunID)
			// The public replay endpoint requires agent delivery history. Do not
			// manufacture that history for this real node-only execution.
			replay := requireServedJSONRPCError(t, rt.Endpoint, "event.replay", map[string]any{"event_id": repeated.EventID, "idempotency_key": "repeat-replay"})
			if replay.Data["code"] != "EVENT_REPLAY_NO_DELIVERY_HISTORY" {
				t.Fatalf("node-only replay=%#v", replay)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			if lifecycleStoredSnapshot(t, rt, seed.RunID) != before {
				t.Fatal("refused node-only replay mutated the next attempt")
			}
			var replayFailures int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' AND status='delivered'`, repeated.EventID).Scan(&replayFailures); err != nil {
				t.Fatal(err)
			}
			if replayFailures != 1 {
				t.Fatalf("accepted ordinary repeat node deliveries=%d", replayFailures)
			}
			current := readLifecycleLoop(t, rt, seed.RunID, entityID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.admit", "admit-2", map[string]any{"revision_id": current.RevisionID}))
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "review")
			var replies [2]servedJSONRPCEnvelope
			start := make(chan struct{})
			var workers sync.WaitGroup
			for index, event := range []string{"loop.repeat", "loop.close"} {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					replies[index] = requestServedJSONRPC(t, rt.Endpoint, "event.publish", params(event, "race-"+event, map[string]any{"revision_id": current.RevisionID}))
				}()
			}
			close(start)
			workers.Wait()
			if t.Failed() {
				return
			}
			accepted := map[string]bool{}
			for _, reply := range replies {
				if reply.Error != nil {
					if reply.Error.Data["code"] != "RUN_ALREADY_TERMINAL" {
						t.Fatalf("competing public admission=%#v", reply.Error)
					}
					continue
				}
				var result servedEventPublishRPCResult
				if err := json.Unmarshal(reply.Result, &result); err != nil {
					t.Fatal(err)
				}
				accepted[result.EventID] = true
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			closed := readLifecycleLoop(t, rt, seed.RunID, entityID)
			if closed.Status != loopruntime.StatusClosed || closed.Attempt != 2 || closed.RevisionID != current.RevisionID {
				t.Fatalf("race loop=%#v", closed)
			}
			history := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
			if len(history) != 5 || !accepted[history[4].TriggerEventID] {
				t.Fatalf("race committed extra/missing cause=%#v", history)
			}
			compiled, ok := history[4].Evidence.Compiled()
			if !ok || compiled.FlowID() != "." || compiled.Edge().LoopID != "revision" {
				t.Fatalf("race selected owner=%#v", compiled)
			}
			escapeCount := 0
			if closed.CloseReason == "escaped" {
				escapeCount = 1
				if history[4].To != "escaped" || compiled.Edge().Source != "loop.escape" {
					t.Fatal("escape winner lost exact cause")
				}
				requireLifecycleFlowEntity(t, rt, seed.RunID, "sink/", "done")
			} else {
				if history[4].To != "done" || string(compiled.Edge().LoopOperation) != "close" {
					t.Fatalf("close winner lost exact cause=%#v", compiled)
				}
			}
			requireLifecycleEventCount(t, rt, seed.RunID, "ordinary.repeated", 1)
			requireLifecycleEventCount(t, rt, seed.RunID, "loop.escaped", escapeCount)
			settled := lifecycleStoredSnapshot(t, rt, seed.RunID)
			duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.repeat", "repeat-1", map[string]any{"revision_id": first.RevisionID}))
			if duplicate.EventID != repeated.EventID || lifecycleStoredSnapshot(t, rt, seed.RunID) != settled {
				t.Fatal("closed replay reminted work or changed evidence")
			}
			t.Logf("cap/close winner=%s accepted_public_contenders=%d", closed.CloseReason, len(accepted))
		})
	}
}
