package serveapp

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestPausedAcceptedTimerAndDecisionPublicBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, family := range []string{"timer", "decision"} {
			t.Run(string(backend)+"/"+family, func(t *testing.T) {
				root := canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateLocal)
				if family == "timer" {
					root = canonicalrouting.CopyLifecycleEmitterCompetingExit(t, true)
				}
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
				created := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "work.requested", "bundle_hash": rt.BundleHash,
					"payload": map[string]any{"seed": true}, "idempotency_key": "paused-accepted-" + family,
				})
				if !created.NewRunCreated || created.RunID == "" || created.EventID == "" {
					t.Fatalf("create review run = %+v", created)
				}
				t.Cleanup(func() {
					if t.Failed() {
						t.Log(servedEventPublishDebugSummary(t, rt.DB, rt.Backend, created.RunID))
					}
				})
				entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, created.RunID, "", "review")
				requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": created.RunID, "idempotency_key": "pause-" + family})
				requireServedRunStatus(t, rt.Endpoint, created.RunID, "paused")
				var eventID string
				if family == "decision" {
					var listed struct {
						Items []struct {
							Kind string `json:"kind"`
							Card struct {
								ID   string `json:"card_id"`
								Hash string `json:"card_content_hash"`
							} `json:"decision_card"`
						} `json:"items"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", map[string]any{"run_id": created.RunID, "status": "pending", "limit": 200}, &listed)
					if len(listed.Items) != 1 || listed.Items[0].Kind != decisioncard.KindDecisionCard || listed.Items[0].Card.ID == "" {
						t.Fatalf("review card = %+v", listed)
					}
					var detail struct {
						Card struct {
							Hash string `json:"card_content_hash"`
						} `json:"decision_card"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": listed.Items[0].Card.ID}, &detail)
					params := map[string]any{"card_id": listed.Items[0].Card.ID, "verdict": "approve", "observed_content_hash": detail.Card.Hash, "idempotency_key": "paused-verdict"}
					var outcome struct {
						Status  string `json:"status"`
						EventID string `json:"decision_event_id"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &outcome)
					if outcome.Status != decisioncard.StatusDecided || outcome.EventID == "" {
						t.Fatalf("paused accepted verdict = %+v", outcome)
					}
					eventID = outcome.EventID
					var replay struct {
						EventID  string `json:"decision_event_id"`
						Replayed bool   `json:"idempotency_replayed"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &replay)
					if !replay.Replayed || replay.EventID != eventID {
						t.Fatalf("paused verdict replay = %+v", replay)
					}
				} else {
					deadline := time.Now().Add(10 * time.Second)
					query := `SELECT event_id FROM events WHERE run_id=$1 AND event_name='platform.stage_timer'`
					for {
						if err := rt.DB.QueryRow(query, created.RunID).Scan(&eventID); err == nil && eventID != "" {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("accepted timer occurrence was not persisted while paused")
						}
						time.Sleep(10 * time.Millisecond)
					}
					var active, fired int
					if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM timers WHERE run_id=$1 AND status='active'`, created.RunID).Scan(&active); err != nil {
						t.Fatal(err)
					}
					if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM timers WHERE run_id=$1 AND status='fired'`, created.RunID).Scan(&fired); err != nil || active != 0 || fired != 1 {
						t.Fatalf("accepted timer activation active=%d fired=%d err=%v", active, fired, err)
					}
				}
				if got := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, created.RunID, entityID, "review"); got != entityID {
					t.Fatalf("paused outcome changed entity identity: %s", got)
				}
				requireServedRunStatus(t, rt.Endpoint, created.RunID, "paused")
				var count int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, created.RunID, eventID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("accepted occurrence cardinality = %d, %v", count, err)
				}
				requireServedOKJSONRPC(t, rt.Endpoint, "run.continue", map[string]any{"run_id": created.RunID, "idempotency_key": "continue-" + family})
				state := "done"
				if family == "timer" {
					state = "cancelled"
				}
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, created.RunID, entityID, state)
				requireServedRunStatus(t, rt.Endpoint, created.RunID, "completed")
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, created.RunID, eventID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("continue duplicated accepted occurrence = %d, %v", count, err)
				}
			})
		}
	}
}
