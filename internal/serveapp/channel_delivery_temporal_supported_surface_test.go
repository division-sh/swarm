package serveapp

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelDeliveryHumanTemporalReceiptPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, transition := range []string{"defer_restart", "expire_restart", "stop_supersedes"} {
			t.Run(string(backend)+"/"+transition, func(t *testing.T) {
				h, db, bundleHash := startChannelAnchorJourney(t, backend, "temporal-token")
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": bundleHash,
					"payload": map[string]any{"seed": true}, "idempotency_key": "temporal-seed",
				})
				gate := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate)
				gateMessage := waitChannelAnchorReceipt(t, db, gate)
				deadline := time.Now().UTC().Add(time.Hour)
				if transition == "expire_restart" {
					deadline = time.Now().UTC().Add(10 * time.Second)
				}
				requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "observer.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
					"payload":         map[string]any{"seed": true, "deadline_at": deadline.Format(time.RFC3339Nano)},
					"idempotency_key": "temporal-human",
				})
				card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindHumanTask)
				message := waitChannelAnchorReceipt(t, db, card)
				token, found := telegramCallbackToken(h.provider.Delivery(message-1), "Approve")
				if !found {
					t.Fatal("actual human card lacks its initial action")
				}
				var snapshot, renderID, frozenInput, frozenHash string
				if err := db.QueryRow(`SELECT CAST(c.snapshot AS TEXT),p.current_render_id,CAST(r.render_input AS TEXT),r.render_hash
					FROM decision_cards c JOIN channel_delivery_plans p ON p.source_id=c.card_id
					JOIN channel_delivery_renders r ON r.render_id=p.current_render_id WHERE c.card_id=$1`, card).Scan(&snapshot, &renderID, &frozenInput, &frozenHash); err != nil {
					t.Fatal(err)
				}
				fragment, terminal := "Deferred until: ", false
				var response map[string]any
				switch transition {
				case "defer_restart":
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.defer", map[string]any{
						"card_id": card, "until": time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339Nano),
						"idempotency_key": "temporal-defer",
					}, &response)
					waitChannelReceiptText(t, h, message, fragment, terminal)
					h.stop(t)
					h.start(t)
				case "expire_restart":
					h.stop(t)
					if remaining := time.Until(deadline); remaining > 0 {
						time.Sleep(remaining)
					}
					h.start(t)
					fragment, terminal = "The decision expired. No action was taken.", true
				case "stop_supersedes":
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.stop", map[string]any{
						"run_id": seed.RunID, "idempotency_key": "temporal-stop",
					}, &response)
					fragment, terminal = "The flow moved on. No action was taken.", true
					waitChannelReceiptText(t, h, gateMessage, fragment, terminal)
				}
				waitChannelReceiptText(t, h, message, fragment, terminal)
				var afterSnapshot, afterInput, afterHash string
				if err := db.QueryRow(`SELECT CAST(snapshot AS TEXT) FROM decision_cards WHERE card_id=$1`, card).Scan(&afterSnapshot); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT CAST(render_input AS TEXT),render_hash FROM channel_delivery_renders WHERE render_id=$1`, renderID).Scan(&afterInput, &afterHash); err != nil {
					t.Fatal(err)
				}
				if afterSnapshot != snapshot || afterInput != frozenInput || afterHash != frozenHash {
					t.Fatal("temporal transition or retained restart overwrote historical frozen meaning")
				}
				if terminal {
					callback, signing, _ := h.provider.Registration()
					if names := postChannelTelegramUpdate(t, callback, signing, map[string]any{
						"update_id": 839000 + message, "callback_query": map[string]any{
							"id": "temporal-stale-" + card, "from": map[string]any{"id": 7000},
							"message": map[string]any{"message_id": message, "chat": map[string]any{"id": 1001, "type": "private"}},
							"data":    token,
						},
					}); len(names) != 0 {
						t.Fatalf("stale human action escaped into business events: %v", names)
					}
					waitChannelRejectedCallback(t, db, token)
					var decisions int
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&decisions); err != nil || decisions != 0 {
						t.Fatalf("terminal receipt regained verdict authority: decisions=%d err=%v", decisions, err)
					}
				}
			})
		}
	}
}

func waitChannelReceiptText(t *testing.T, h *channelOnboardingE2EHarness, message int, fragment string, terminal bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		for _, edit := range h.provider.Edits() {
			if fmt.Sprint(edit["message_id"]) != fmt.Sprint(message) || !strings.Contains(fmt.Sprint(edit["text"]), fragment) {
				continue
			}
			if terminal {
				markup, _ := edit["reply_markup"].(map[string]any)
				rows, _ := markup["inline_keyboard"].([]any)
				if len(rows) != 0 {
					t.Fatalf("terminal receipt retains controls: %v", edit)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("actual receipt %d did not render %q: %v\n%s", message, fragment, h.provider.Edits(), h.process.outputString())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
