package serveapp

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestMailboxMutationAdmissionRollbackBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt, owner := newMailboxCompletionRuntime(t, backend)
			for _, kind := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect} {
				t.Run(string(kind), func(t *testing.T) {
					f := mailboxCompletionFixtureInRuntime(t, rt, owner)
					card := mailboxCompletionAnchorCard(t, f, kind)
					foreign := mailboxCompletionFixtureInRuntime(t, rt, owner)
					var foreignDraft map[string]any
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.begin_input", map[string]any{"card_id": foreign.base.CardID, "verdict": "reject", "observed_content_hash": foreign.base.CardContentHash}, &foreignDraft)
					notice := mailboxCompletionNotice(t, f)
					before := mailboxCompletionRunEffects(t, rt, card.RunID)
					beforeForeign := mailboxCompletionRunEffects(t, rt, foreign.base.RunID)
					for _, bad := range []struct {
						name, method string
						params       map[string]any
					}{
						{"invalid_verdict", "mailbox.decide", map[string]any{"card_id": card.CardID, "verdict": "not-authored", "observed_content_hash": card.CardContentHash}},
						{"stale_hash", "mailbox.decide", map[string]any{"card_id": card.CardID, "verdict": "approve", "observed_content_hash": "stale-content"}},
						{"invalid_input_verdict", "mailbox.begin_input", map[string]any{"card_id": card.CardID, "verdict": "not-authored", "observed_content_hash": card.CardContentHash}},
						{"stale_input_hash", "mailbox.begin_input", map[string]any{"card_id": card.CardID, "verdict": "reject", "observed_content_hash": "stale-content"}},
						{"foreign_input_cancel", "mailbox.cancel_input", map[string]any{"card_id": card.CardID, "input_draft_id": foreignDraft["input_draft_id"]}},
						{"unknown_input_cancel", "mailbox.cancel_input", map[string]any{"card_id": card.CardID, "input_draft_id": uuid.NewString()}},
						{"foreign_input_decide", "mailbox.decide", map[string]any{"card_id": card.CardID, "verdict": "approve", "observed_content_hash": card.CardContentHash, "input_draft_id": foreignDraft["input_draft_id"]}},
						{"notice_decide", "mailbox.decide", map[string]any{"card_id": notice, "verdict": "approve", "observed_content_hash": card.CardContentHash}},
						{"notice_defer", "mailbox.defer", map[string]any{"card_id": notice, "until": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)}},
						{"notice_begin", "mailbox.begin_input", map[string]any{"card_id": notice, "verdict": "reject", "observed_content_hash": card.CardContentHash}},
						{"notice_cancel", "mailbox.cancel_input", map[string]any{"card_id": notice, "input_draft_id": foreignDraft["input_draft_id"]}},
						{"card_acknowledge", "mailbox.acknowledge", map[string]any{"mailbox_id": card.CardID}},
						{"unknown_notice", "mailbox.acknowledge", map[string]any{"mailbox_id": uuid.NewString()}},
					} {
						t.Run(bad.name, func(t *testing.T) {
							key := uuid.NewString()
							bad.params["idempotency_key"] = key
							if result := requestServedJSONRPC(t, rt.Endpoint, bad.method, bad.params); result.Error == nil {
								t.Fatalf("invalid mutation succeeded: %s", result.Result)
							}
							var count int
							if err := rt.DB.QueryRow(`SELECT count(*) FROM api_idempotency WHERE idempotency_key=$1`, key).Scan(&count); err != nil || count != 0 {
								t.Fatalf("refusal stored success: count=%d err=%v", count, err)
							}
						})
					}
					if after := mailboxCompletionRunEffects(t, rt, card.RunID); !reflect.DeepEqual(before, after) {
						t.Fatalf("invalid mutation changed domain: %s", mailboxEffectsDifference(before, after))
					}
					if after := mailboxCompletionRunEffects(t, rt, foreign.base.RunID); !reflect.DeepEqual(beforeForeign, after) {
						t.Fatalf("invalid mutation changed foreign draft: %s", mailboxEffectsDifference(beforeForeign, after))
					}
					var notified bool
					if err := rt.DB.QueryRow(`SELECT notified FROM mailbox WHERE item_id=$1`, notice).Scan(&notified); err != nil || notified {
						t.Fatalf("invalid mutation changed notice: notified=%t err=%v", notified, err)
					}
					// One key across four different methods must not alias completions.
					key, verdict := uuid.NewString(), "reject"
					if kind == decisioncard.AnchorKindProposedEffect {
						verdict = "revise"
					}
					var draft, result map[string]any
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.begin_input", map[string]any{"card_id": card.CardID, "verdict": verdict, "observed_content_hash": card.CardContentHash, "idempotency_key": key}, &draft)
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.cancel_input", map[string]any{"card_id": card.CardID, "input_draft_id": draft["input_draft_id"], "idempotency_key": key}, &result)
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.defer", map[string]any{"card_id": card.CardID, "until": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), "idempotency_key": key}, &result)
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", map[string]any{"card_id": card.CardID, "verdict": "approve", "observed_content_hash": card.CardContentHash, "idempotency_key": key}, &result)
					var count int
					if err := rt.DB.QueryRow(`SELECT count(DISTINCT method) FROM api_idempotency WHERE idempotency_key=$1 AND resource_id=$2`, key, card.CardID).Scan(&count); err != nil || count != 4 {
						t.Fatalf("method identities aliased: count=%d err=%v", count, err)
					}
				})
			}
		})
	}
}
