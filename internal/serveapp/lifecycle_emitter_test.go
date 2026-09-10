package serveapp

import (
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedLifecycleEmitterLoopEscapeJourney(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopConnected))
			defer func() {
				if !t.Failed() {
					return
				}
				rows, err := rt.DB.Query(`SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE status = 'dead_letter'`)
				if err != nil {
					t.Logf("failure readback: %v", err)
					return
				}
				defer rows.Close()
				for rows.Next() {
					var failure string
					if err := rows.Scan(&failure); err != nil {
						t.Log(err)
						continue
					}
					t.Logf("terminal delivery failure: %s", failure)
				}
			}()
			started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "loop-create",
			})
			entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, "", "waiting")
			publish := func(event, key string, payload map[string]any) servedEventPublishRPCResult {
				return requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": event, "run_id": started.RunID, "source_event_id": started.EventID, "payload": payload, "idempotency_key": key,
				})
			}
			publish("loop.start", "loop-start", map[string]any{"seed": true})
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, entityID, "drafting")
			first := readLifecycleLoop(t, rt, started.RunID, entityID)
			if first.Attempt != 1 || first.MaxAttempts != 2 || first.Status != loopruntime.StatusOpen {
				t.Fatalf("first loop=%#v", first)
			}
			for attempt := 1; attempt <= 2; attempt++ {
				current := readLifecycleLoop(t, rt, started.RunID, entityID)
				if current.Attempt != attempt {
					t.Fatalf("attempt=%#v", current)
				}
				payload := map[string]any{"revision_id": current.RevisionID}
				publish("loop.admit", fmt.Sprintf("admit-%d", attempt), payload)
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, entityID, "review")
				publish("loop.repeat", fmt.Sprintf("repeat-%d", attempt), payload)
				state := "drafting"
				if attempt == 2 {
					state = "escaped"
				}
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, entityID, state)
			}
			closed := readLifecycleLoop(t, rt, started.RunID, entityID)
			if closed.Status != loopruntime.StatusClosed || closed.CloseReason != "escaped" || closed.Attempt != 2 || closed.RevisionID == first.RevisionID {
				t.Fatalf("closed loop=%#v", closed)
			}
			receipt := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, "", "done")
			if receipt == entityID {
				t.Fatal("connected consumer reused producer entity")
			}
			var received operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": started.RunID, "entity_id": receipt}, &received)
			if received.Fields["revision_id"] != closed.RevisionID {
				t.Fatalf("escape result=%#v, loop=%#v", received, closed)
			}
			requireLifecycleEventCount(t, rt, started.RunID, "loop.escaped", 1)
		})
	}
}

func readLifecycleLoop(t *testing.T, rt servedControlProofRuntime, runID, entityID string) loopruntime.PublicActivation {
	t.Helper()
	var result operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entityID}, &result)
	if len(result.Loops) != 1 {
		t.Fatalf("expected one public loop: %#v", result)
	}
	return result.Loops[0]
}

func TestServedLifecycleEmitterGateJourney(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			for _, tc := range []struct {
				name, prefix, verdict, result string
				variant                       canonicalrouting.LifecycleEmitterVariant
			}{
				{"root", "", "approve", "approved", canonicalrouting.LifecycleGateLocal},
				{"shared_verdict", "", "reject", "rejected", canonicalrouting.LifecycleGateSharedEvent},
				{"nested", "outer/inner/", "approve", "approved", canonicalrouting.LifecycleGateNested},
			} {
				t.Run(tc.name, func(t *testing.T) {
					root := canonicalrouting.CopyLifecycleEmitter(t, tc.variant)
					rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
					started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
						"event_name": tc.prefix + "work.requested", "payload": map[string]any{"seed": true}, "idempotency_key": "gate-start", "bundle_hash": rt.BundleHash,
					})
					entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, "", "review")
					cardID := waitLifecycleGateCard(t, rt, started.RunID)
					var detail struct {
						DecisionCard struct {
							ContentHash string `json:"card_content_hash"`
						} `json:"decision_card"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": cardID}, &detail)
					if detail.DecisionCard.ContentHash == "" {
						t.Fatal("missing frozen card hash")
					}
					params := map[string]any{"card_id": cardID, "verdict": tc.verdict, "observed_content_hash": detail.DecisionCard.ContentHash, "idempotency_key": "gate-decide"}
					var decided map[string]any
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &decided)
					if decided["status"] != "decided" {
						t.Fatalf("decision = %#v", decided)
					}
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, entityID, "done")
					requireServedEntityReadback(t, rt.Endpoint, started.RunID, entityID, "done")
					var replay map[string]any
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &replay)
					requireLifecycleEventCount(t, rt, started.RunID, tc.prefix+"work.completed", 1)
					var fields string
					if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id = $1 AND entity_id = $2`, started.RunID, entityID).Scan(&fields); err != nil {
						t.Fatal(err)
					}
					var entity operatorread.OperatorEntityFull
					requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"entity_id": entityID, "run_id": started.RunID}, &entity)
					if entity.Fields["result"] != tc.result {
						t.Fatalf("public fields=%#v, stored=%s", entity.Fields, fields)
					}
				})
			}
		})
	}
}

func waitLifecycleGateCard(t *testing.T, rt servedControlProofRuntime, runID string) string {
	t.Helper()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		var result struct {
			Items []struct {
				DecisionCard struct {
					CardID string `json:"card_id"`
				} `json:"decision_card"`
			} `json:"items"`
		}
		requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", map[string]any{"status": "pending", "run_id": runID}, &result)
		if len(result.Items) == 1 {
			if result.Items[0].DecisionCard.CardID == "" {
				t.Fatalf("missing card identity: %#v", result.Items)
			}
			return result.Items[0].DecisionCard.CardID
		}
		if len(result.Items) > 1 {
			t.Fatalf("unexpected multiple gate cards: %#v", result.Items)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("gate card not visible: %s", servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID))
	return ""
}

func requireLifecycleEventCount(t *testing.T, rt servedControlProofRuntime, runID, event string, want int) {
	t.Helper()
	var count int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id = $1 AND event_name = $2`, runID, event).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatal(fmt.Sprintf("%s count=%d, want %d", event, count, want))
	}
}
