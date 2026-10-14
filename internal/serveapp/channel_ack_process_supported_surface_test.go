package serveapp

import (
	"context"
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
				waitSelectedChannelDeliverySendsSettled(t, channelSettlementStorageObservation(t, h))
				h.stop(t)
				t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
				first := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
				h.endpoint = first.endpoint(t)
				intentIdentity := captureChannelIntentIdentity(t, h)
				intentReader := openChannelAnchorObservation(t, h)
				waitSelectedChannelDeliverySendsSettled(t, channelSettlementStorageObservation(t, h))
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": "ack-crash-seed"})
				card := waitChannelAnchorCard(t, channelAnchorStorageObservation(t, h), seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
				messageID := waitChannelAnchorReceipt(t, channelAnchorStorageObservation(t, h), card)
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
				_, intentQuery := postChannelSourceControlObserved(t, callback, signing, intentIdentity, card, token, messageID)
				if arrived != nil {
					select {
					case <-arrived:
						var status string
						var decisions int
						if err := func() error {
							observed, err := channelSettlementStorageObservation(t, h).ReadChannelAnchorDecisionStorage(context.Background(), card)
							status = observed.Status
							return err
						}(); err != nil || status != "pending" {
							t.Fatalf("ACK barrier is not before business mutation: %s %v", status, err)
						}
						result, found, err := intentReader.ObserveChannelIntent(context.Background(), intentQuery)
						if err != nil || !found || result.State != "pending" || result.Disposition != "" {
							t.Fatalf("verified exact action %+v missing at pre-mutation barrier: %+v found=%t %v", intentQuery, result, found, err)
						}
						{
							var err error
							decisions, err = channelAnchorStorageObservation(t, h).CountRunNamedEventStorage(context.Background(), seed.RunID, "mailbox.card_decided")
							if err != nil || decisions != 0 {
								t.Fatalf("pre-mutation cut already emitted a decision: %d %v", decisions, err)
							}
						}
					case <-time.After(20 * time.Second):
						t.Fatal("callback did not reach actual provider response barrier")
					}
				} else {
					waitChannelAnchorDecision(t, channelAnchorStorageObservation(t, h), card)
				}
				operation := waitChannelAcknowledgmentAttempt(t, db, cut)
				before := len(h.provider.Acknowledgments())
				if err := intentReader.Close(); err != nil {
					t.Fatal(err)
				}
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
				intentReader = openChannelAnchorObservation(t, h)
				want := "settled"
				if cut == "launched" {
					want = "outcome_uncertain"
				}
				if after := waitChannelAcknowledgmentAttempt(t, db, want); after != operation {
					t.Fatal("restart replaced original acknowledgment operation")
				}
				waitChannelAnchorDecision(t, channelAnchorStorageObservation(t, h), card)
				result, found, err := intentReader.ObserveChannelIntent(context.Background(), intentQuery)
				if err != nil || !found || result.State != "settled" || result.Disposition != "applied" {
					t.Fatalf("restart did not atomically complete exact action %+v: %+v found=%t %v", intentQuery, result, found, err)
				}
				if len(h.provider.Acknowledgments()) != before {
					t.Fatal("restart replayed uncertain or settled acknowledgment")
				}
				var decisions int
				{
					var err error
					decisions, err = channelAnchorStorageObservation(t, h).CountRunNamedEventStorage(context.Background(), seed.RunID, "mailbox.card_decided")
					if err != nil || decisions != 1 {
						t.Fatalf("ACK was confused with business completion: decisions=%d err=%v", decisions, err)
					}
				}
				if err := intentReader.Close(); err != nil {
					t.Fatal(err)
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
