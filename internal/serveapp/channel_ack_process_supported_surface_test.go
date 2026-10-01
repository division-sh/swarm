package serveapp

import (
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelActionAcknowledgmentAbruptProcessDeathPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, cut := range []string{"launched", "settled"} {
			t.Run(string(backend)+"/"+cut, func(t *testing.T) {
				h, db, hash := startChannelAnchorJourney(t, backend, "ack-crash-token", false)
				waitChannelDeliverySendsSettled(t, db)
				h.stop(t)
				t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
				first := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
				h.endpoint = first.endpoint(t)
				waitChannelDeliverySendsSettled(t, db)
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true},
					"idempotency_key": "ack-crash-seed",
				})
				card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate)
				messageID := waitChannelAnchorReceipt(t, db, card)
				token, ok := telegramCallbackToken(h.provider.Delivery(messageID-1), "approve")
				if !ok {
					t.Fatal("real gate has no callback control")
				}
				var arrived <-chan struct{}
				var release func()
				if cut == "launched" {
					arrived, release = h.provider.PauseNextCallbackResponse()
					t.Cleanup(release)
				}
				callback, signing, _ := h.provider.Registration()
				postChannelSourceControl(t, callback, signing, card, token, messageID)
				if arrived != nil {
					select {
					case <-arrived:
					case <-time.After(20 * time.Second):
						t.Fatal("callback did not reach actual provider response barrier")
					}
				} else {
					waitChannelAnchorDecision(t, db, card)
				}
				operation := waitChannelAcknowledgmentAttempt(t, db, cut)
				before := len(h.provider.Acknowledgments())
				if err := first.kill(); err != nil {
					t.Fatal(err)
				}
				if first.waitError() == nil {
					t.Fatal("OS death was reported as graceful shutdown")
				}
				if release != nil {
					release()
				}
				second := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
				h.endpoint = second.endpoint(t)
				want := "settled"
				if cut == "launched" {
					want = "outcome_uncertain"
				}
				if after := waitChannelAcknowledgmentAttempt(t, db, want); after != operation {
					t.Fatal("restart replaced original acknowledgment operation")
				}
				waitChannelAnchorDecision(t, db, card)
				if len(h.provider.Acknowledgments()) != before {
					t.Fatal("restart replayed uncertain or settled acknowledgment")
				}
				var decisions int
				if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&decisions); err != nil || decisions != 1 {
					t.Fatalf("ACK was confused with business completion: decisions=%d err=%v", decisions, err)
				}
				if err := second.stop(); err != nil {
					t.Fatalf("recovered process did not stop: %v\n%s", err, second.output.String())
				}
			})
		}
	}
}

func waitChannelAcknowledgmentAttempt(t *testing.T, db *sql.DB, want string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var operation, state string
		err := db.QueryRow(`SELECT CAST(o.operation_id AS TEXT),a.state FROM runtime_external_effect_operations o
			JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id WHERE o.authority_kind='channel_action_ack'`).Scan(&operation, &state)
		if err == nil && state == want {
			return operation
		}
		if err != nil && err != sql.ErrNoRows || time.Now().After(deadline) {
			t.Fatalf("ACK attempt state=%s want=%s err=%v", state, want, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
