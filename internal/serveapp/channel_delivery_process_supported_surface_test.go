package serveapp

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelDeliveryAbruptProcessDeathPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, cut := range []string{"launched", "settled"} {
			t.Run(string(backend)+"/"+cut, func(t *testing.T) {
				h, db, hash := startChannelAnchorJourney(t, backend, "delivery-crash-token", false)
				waitChannelDeliverySendsSettled(t, db)
				h.stop(t)
				t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
				first := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
				h.endpoint = first.endpoint(t)
				var standingRun string
				if err := db.QueryRow(`SELECT current_run_id FROM standing_services WHERE current_run_id IS NOT NULL`).Scan(&standingRun); err != nil {
					t.Fatal(err)
				}
				standingCard := waitChannelAnchorCard(t, db, standingRun, decisioncard.AnchorKindStageGate)
				waitChannelAnchorReceipt(t, db, standingCard)
				waitChannelDeliverySendsSettled(t, db)
				var arrived <-chan struct{}
				var release func()
				if cut == "launched" {
					arrived, release = h.provider.PauseNextDeliveryResponse()
					t.Cleanup(release)
				}
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": hash,
					"payload": map[string]any{"seed": true}, "idempotency_key": "delivery-crash-seed",
				})
				card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate)
				if arrived != nil {
					select {
					case <-arrived:
					case <-time.After(20 * time.Second):
						t.Fatalf("delivery never reached the actual provider response barrier\n%s", first.output.String())
					}
					if _, ok := telegramCallbackToken(h.provider.Delivery(channelDeliveryProviderMessageCount(h.provider)-1), "approve"); !ok {
						t.Fatalf("provider barrier selected a different source: %#v", h.provider.Delivery(channelDeliveryProviderMessageCount(h.provider)-1))
					}
				}
				state := "settled"
				if cut == "launched" {
					state = "launched"
				}
				operation, render := waitChannelDeliveryProcessAttempt(t, db, backend, card, state)
				var input, renderHash string
				if err := db.QueryRow(`SELECT CAST(render_input AS TEXT),render_hash FROM channel_delivery_renders WHERE render_id=$1`, render).Scan(&input, &renderHash); err != nil {
					t.Fatal(err)
				}
				before := channelDeliveryProviderMessageCount(h.provider)
				messageID := before
				token, ok := telegramCallbackToken(h.provider.Delivery(messageID-1), "approve")
				if !ok {
					t.Fatal("real gate delivery lacks its frozen approve control")
				}
				if err := first.kill(); err != nil {
					t.Fatal(err)
				}
				if first.waitError() == nil {
					t.Fatal("abrupt process death reported a graceful exit")
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
				waitChannelDeliveryProcessAttempt(t, db, backend, card, want)
				var actualOperation, afterInput, afterHash, planState, receiptState, current string
				if err := db.QueryRow(`SELECT CAST(r.effect_operation_id AS TEXT),p.state,r.state,COALESCE(CAST(p.current_receipt_operation_id AS TEXT),'')
					FROM channel_delivery_plans p JOIN channel_delivery_receipts r ON r.delivery_id=p.delivery_id AND r.effect_operation_id=$2
					WHERE p.source_id=$1`, card, operation).Scan(&actualOperation, &planState, &receiptState, &current); err != nil {
					var retainedState, retainedCurrent string
					var receipts int
					readErr := db.QueryRow(`SELECT p.state,COALESCE(CAST(p.current_receipt_operation_id AS TEXT),''),
						(SELECT COUNT(*) FROM channel_delivery_receipts WHERE effect_operation_id=$2)
						FROM channel_delivery_plans p WHERE p.source_id=$1`, card, operation).Scan(&retainedState, &retainedCurrent, &receipts)
					t.Fatalf("exact recovered receipt missing: cut=%s operation=%s want_attempt=%s plan=%s current=%s receipts=%d read=%v join=%v\n%s",
						cut, operation, want, retainedState, retainedCurrent, receipts, readErr, err, second.output.String())
				}
				if err := db.QueryRow(`SELECT CAST(render_input AS TEXT),render_hash FROM channel_delivery_renders WHERE render_id=$1`, render).Scan(&afterInput, &afterHash); err != nil {
					t.Fatal(err)
				}
				wantReceipt := "sent"
				wantCurrent := operation
				if cut == "launched" {
					wantReceipt = "uncertain"
					wantCurrent = ""
				}
				if actualOperation != operation || current != wantCurrent || planState != wantReceipt || receiptState != wantReceipt || afterInput != input || afterHash != renderHash {
					t.Fatalf("restart changed exact frozen delivery/receipt: op=%s want=%s plan=%s receipt=%s", actualOperation, operation, planState, receiptState)
				}
				if count := channelDeliveryProviderMessageCount(h.provider); count != before {
					t.Fatalf("startup replayed a %s delivery: before=%d after=%d", cut, before, count)
				}
				callback, signing, _ := h.provider.Registration()
				postChannelTelegramUpdate(t, callback, signing, map[string]any{
					"update_id": 940000 + messageID, "callback_query": map[string]any{
						"id": "crash-control-" + card, "from": map[string]any{"id": 7000},
						"message": map[string]any{"message_id": messageID, "chat": map[string]any{"id": 1001, "type": "private"}},
						"data":    token,
					},
				})
				if cut == "launched" {
					waitChannelRejectedCallback(t, db, token)
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("uncertain historical control acquired decision authority: count=%d %v", count, err)
					}
				} else {
					waitChannelAnchorDecision(t, db, card)
				}
				if err := second.stop(); err != nil {
					t.Fatalf("recovered process did not stop: %v\n%s", err, second.output.String())
				}
			})
		}
	}
}

func waitChannelDeliveryProcessAttempt(t *testing.T, db *sql.DB, backend servedparity.Backend, card, state string) (string, string) {
	t.Helper()
	evidence := "json_extract(o.authority_evidence,'$.delivery_id')=p.delivery_id"
	if backend == servedparity.BackendExplicitPostgres {
		evidence = "o.authority_evidence->>'delivery_id'=CAST(p.delivery_id AS TEXT)"
	}
	query := `SELECT CAST(o.operation_id AS TEXT),p.current_render_id,a.state
		FROM channel_delivery_plans p JOIN runtime_external_effect_operations o ON ` + evidence + `
		JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		WHERE p.source_id=$1 AND o.authority_kind='channel_delivery'`
	deadline := time.Now().Add(20 * time.Second)
	for {
		var operation, render, actual string
		err := db.QueryRow(query, card).Scan(&operation, &render, &actual)
		if err != nil && err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if err == nil && actual == state {
			return operation, render
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery card=%s attempt=%s, want=%s: %v", card, actual, state, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestChannelDeliverySettlementWaitIncludesPlannedWork(t *testing.T) {
	for _, state := range []string{"planned", "rendered"} {
		t.Run(state, func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "wait.db"))
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE channel_delivery_plans (state TEXT NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO channel_delivery_plans (state) VALUES (?)`, state); err != nil {
				t.Fatal(err)
			}
			started, done := make(chan struct{}), make(chan struct{})
			go func() {
				close(started)
				defer close(done)
				waitChannelDeliverySendsSettled(t, db)
			}()
			<-started
			select {
			case <-done:
				t.Fatalf("settlement wait returned while a %s delivery still exists", state)
			case <-time.After(200 * time.Millisecond):
			}
			if _, err := db.Exec(`UPDATE channel_delivery_plans SET state='sent'`); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("settlement wait did not release after the delivery settled")
			}
		})
	}
}

func waitChannelDeliverySendsSettled(t *testing.T, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM channel_delivery_plans WHERE state IN ('planned','rendered')`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("initial public deliveries never settled")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func channelDeliveryProviderMessageCount(provider *channelOnboardingTelegramProvider) int {
	for index := 0; ; index++ {
		if provider.Delivery(index) == nil {
			return index
		}
	}
}
