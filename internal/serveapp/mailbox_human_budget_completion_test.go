package serveapp

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestMailboxHumanBudgetAndCrossCredentialDraftBothStores(t *testing.T) {
	requireMailboxCompletionFaultFunction(t)
	const secondToken = "budget-proof-second-credential"
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt, owner, restart := newRetainedMailboxCompletionRuntimeConfigured(t, backend, canonicalrouting.CopyMailboxCompletionMatrix(t), "budget:\n  human_tasks:\n    max_tasks_per_week: 1\n", apiv1.DefaultLoopbackAPIToken, secondToken)
			type retained struct {
				params map[string]any
				result map[string]any
				runID  string
			}
			var completed []retained
			for _, outcome := range []string{"approve", "budget_forced_defer", "reject_with_draft"} {
				f := mailboxCompletionFixtureInRuntime(t, rt, owner)
				card := mailboxCompletionAnchorCard(t, f, decisioncard.AnchorKindHumanTask)
				params := map[string]any{"card_id": card.CardID, "verdict": "approve", "observed_content_hash": card.CardContentHash, "idempotency_key": uuid.NewString()}
				if outcome == "reject_with_draft" {
					var draft map[string]any
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.begin_input", map[string]any{"card_id": card.CardID, "verdict": "reject", "observed_content_hash": card.CardContentHash, "idempotency_key": uuid.NewString()}, &draft)
					params["verdict"], params["fields"], params["input_draft_id"] = "reject", map[string]any{"reason": "not needed"}, draft["input_draft_id"]
				}
				if outcome == "budget_forced_defer" {
					before := mailboxCompletionRunEffects(t, rt, f.base.RunID)
					reached, remove := installMailboxCompletionFaultWitness(t, rt, params["idempotency_key"].(string))
					response, status, err := mailboxTransportRequest(f.ctx, rt.Endpoint, "http", secondToken, "mailbox.decide", params)
					if err != nil || status != http.StatusOK || response.Error == nil {
						t.Fatalf("forced-deferral INSERT fault not surfaced: status=%d err=%v rpc=%+v", status, err, response.Error)
					}
					reached()
					remove()
					if after := mailboxCompletionRunEffects(t, rt, f.base.RunID); !reflect.DeepEqual(before, after) {
						t.Fatalf("failed response INSERT changed snapshot: %s", mailboxEffectsDifference(before, after))
					}
				}
				response, status, err := mailboxTransportRequest(f.ctx, rt.Endpoint, "http", secondToken, "mailbox.decide", params)
				if err != nil || status != http.StatusOK || response.Error != nil {
					t.Fatalf("human outcome %s: status=%d err=%v rpc=%+v", outcome, status, err, response.Error)
				}
				var result map[string]any
				if err := json.Unmarshal(response.Result, &result); err != nil {
					t.Fatal(err)
				}
				if result["idempotency_replayed"] != false {
					t.Fatalf("fresh outcome marked replay: %v", result)
				}
				delete(result, "idempotency_replayed")
				var state, cause string
				var requeues int
				if err := rt.DB.QueryRow(`SELECT state,COALESCE(defer_cause,''),requeue_count FROM human_task_continuations WHERE card_id=$1`, card.CardID).Scan(&state, &cause, &requeues); err != nil {
					t.Fatal(err)
				}
				if outcome == "budget_forced_defer" {
					if result["status"] != "pending" || result["verdict"] != nil || result["decision_event_id"] != nil || state != "pending" || cause != "weekly_budget_exhausted" || requeues != 1 {
						t.Fatalf("requested approve did not project committed deferral: %v state=%s cause=%s requeues=%d", result, state, cause, requeues)
					}
				} else if result["verdict"] != params["verdict"] {
					t.Fatalf("committed verdict changed: %v", result)
				}
				if outcome == "reject_with_draft" {
					var draftStatus, principal, actual string
					if err := rt.DB.QueryRow(`SELECT status,principal_id FROM decision_card_input_drafts WHERE input_draft_id=$1`, params["input_draft_id"]).Scan(&draftStatus, &actual); err != nil {
						t.Fatal(err)
					}
					if err := rt.DB.QueryRow(`SELECT principal_id FROM operator_principals`).Scan(&principal); err != nil {
						t.Fatal(err)
					}
					if draftStatus != "consumed" || principal != actual {
						t.Fatalf("credential B did not consume credential A's principal draft: %s %s", draftStatus, actual)
					}
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, f.base.RunID)
				completed = append(completed, retained{params: params, result: result, runID: f.base.RunID})
			}
			rt, _ = restart()
			for _, row := range completed {
				before := mailboxCompletionRunEffects(t, rt, row.runID)
				response, _, err := mailboxTransportRequest(context.Background(), rt.Endpoint, "http", secondToken, "mailbox.decide", row.params)
				if err != nil || response.Error != nil {
					t.Fatalf("post-restart outcome replay: err=%v rpc=%+v", err, response.Error)
				}
				var replay map[string]any
				if err := json.Unmarshal(response.Result, &replay); err != nil {
					t.Fatal(err)
				}
				if replay["idempotency_replayed"] != true {
					t.Fatal("restart caused fresh human mutation")
				}
				delete(replay, "idempotency_replayed")
				if !reflect.DeepEqual(row.result, replay) {
					t.Fatalf("restart changed actual outcome: %v / %v", row.result, replay)
				}
				if after := mailboxCompletionRunEffects(t, rt, row.runID); !reflect.DeepEqual(before, after) {
					t.Fatal("restart replay changed human state")
				}
			}
		})
	}
}
