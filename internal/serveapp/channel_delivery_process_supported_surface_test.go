package serveapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestChannelDeliveryAbruptProcessDeathPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, cut := range []string{"launched", "settled"} {
			t.Run(string(backend)+"/"+cut, func(t *testing.T) {
				h, db, hash := startChannelAnchorJourney(t, backend, "delivery-crash-token", false)
				waitChannelDeliverySendsSettled(t, db, backend)
				h.stop(t)
				t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
				first := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
				h.endpoint = first.endpoint(t)
				var standingRun string
				if err := db.QueryRow(`SELECT current_run_id FROM standing_services WHERE current_run_id IS NOT NULL`).Scan(&standingRun); err != nil {
					t.Fatal(err)
				}
				standingCard := waitChannelAnchorCard(t, db, standingRun, decisioncard.AnchorKindStageGate, "telegram-ingress")
				waitChannelAnchorReceipt(t, db, standingCard)
				waitChannelDeliverySendsSettled(t, db, backend)
				var arrived <-chan int
				var release func()
				seedAdmitted := make(chan struct{})
				var seedRunID string
				if cut == "launched" {
					arrived, release = h.provider.PauseDeliveryResponseMatching(func(delivery map[string]any) bool {
						token, ok := telegramCallbackToken(delivery, "approve")
						if !ok {
							return false
						}
						select {
						case <-seedAdmitted:
						case <-time.After(20 * time.Second):
							t.Error("creating publication did not acknowledge the review run")
							return false
						}
						var runID, raw string
						err := db.QueryRow(`SELECT CAST(card.run_id AS TEXT),CAST(card.anchor AS TEXT)
							FROM channel_delivery_actions action
							JOIN channel_delivery_renders render ON render.render_id=action.render_id
							JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
							JOIN decision_cards card ON card.card_id=plan.source_id
							WHERE action.action_token=$1`, token).Scan(&runID, &raw)
						if err != nil {
							t.Errorf("resolve actual provider control: %v", err)
							return false
						}
						anchor, err := decisioncard.DecodeAnchor(string(decisioncard.AnchorKindStageGate), []byte(raw))
						if err != nil {
							t.Errorf("decode actual provider control anchor: %v", err)
							return false
						}
						scope, err := anchor.Scope()
						if err != nil {
							t.Errorf("resolve actual provider control scope: %v", err)
							return false
						}
						return runID == seedRunID && scope.FlowInstance == "reviews"
					})
					t.Cleanup(release)
				}
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": hash,
					"payload": map[string]any{"seed": true}, "idempotency_key": "delivery-crash-seed",
				})
				seedRunID = seed.RunID
				close(seedAdmitted)
				card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
				messageID := 0
				if arrived != nil {
					select {
					case messageID = <-arrived:
					case <-time.After(20 * time.Second):
						t.Fatalf("delivery never reached the actual provider response barrier\n%s", first.output.String())
					}
					if _, ok := telegramCallbackToken(h.provider.Delivery(messageID-1), "approve"); !ok {
						t.Fatalf("provider barrier selected a different source: %#v", h.provider.Delivery(messageID-1))
					}
				} else {
					messageID = waitChannelAnchorReceipt(t, db, card)
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
				token, ok := telegramCallbackToken(h.provider.Delivery(messageID-1), "approve")
				if !ok {
					t.Fatal("real gate delivery lacks its frozen approve control")
				}
				before := channelDeliveryCountForToken(h.provider, token)
				if before != 1 {
					t.Fatalf("review control delivered %d times before crash", before)
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
				if count := channelDeliveryCountForToken(h.provider, token); count != before {
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

func channelDeliveryCountForToken(provider *channelOnboardingTelegramProvider, token string) int {
	count := 0
	for index := 0; ; index++ {
		delivery := provider.Delivery(index)
		if delivery == nil {
			return count
		}
		if actual, ok := telegramCallbackToken(delivery, "approve"); ok && actual == token {
			count++
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
			db := newChannelResponsibilityProjectionDB(t)
			if _, err := db.Exec(`INSERT INTO channel_delivery_plans (source_kind,state) VALUES ('summary',?)`, state); err != nil {
				t.Fatal(err)
			}
			started, done := make(chan struct{}), make(chan struct{})
			go func() {
				close(started)
				defer close(done)
				waitChannelDeliverySendsSettled(t, db, servedparity.BackendDefaultSQLite)
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
	// These are projection fixtures, not evidence of provider acceptance. The
	// native effect matrix proves the journal transitions producing these rows.
	for _, test := range []struct {
		name, source, status, phase, effect string
		want                                int
	}{
		{"pending", "card", "pending", "planned", "", 1},
		{"deferred", "card", "pending", "rendered", "", 1},
		{"superseded", "card", "superseded", "rendered", "", 0},
		{"decided", "card", "decided", "rendered", "", 0},
		{"expired", "card", "expired", "rendered", "", 0},
		{"notice_completed", "notice", "decided", "rendered", "", 0},
		{"authorized", "card", "superseded", "retired", "authorized", 1},
		{"launched", "card", "superseded", "retired", "launched", 1},
		{"observed", "card", "superseded", "retired", "response_observed", 1},
		{"uncertain", "card", "superseded", "uncertain", "outcome_uncertain", 0},
		{"terminal_failure", "card", "superseded", "rendered", "terminal_failure", 0},
		{"summary", "summary", "", "planned", "", 1},
		{"response", "response", "", "rendered", "", 1},
	} {
		t.Run("projection/"+test.name, func(t *testing.T) {
			db := newChannelResponsibilityProjectionDB(t)
			statements := []struct {
				query string
				args  []any
			}{
				{`INSERT INTO channel_delivery_plans VALUES ('delivery',?, 'source',NULL,?)`, []any{test.source, test.phase}},
				{`INSERT INTO decision_cards VALUES ('source',?)`, []any{test.status}},
				{`INSERT INTO mailbox VALUES ('source',?)`, []any{test.status}},
				{`INSERT INTO runtime_external_effect_operations VALUES ('channel_delivery','{"delivery_id":"delivery"}',?)`, []any{test.effect}},
			}
			for _, statement := range statements {
				if _, err := db.Exec(statement.query, statement.args...); err != nil {
					t.Fatal(err)
				}
			}
			count, err := storetest.CountUnsettledChannelDeliveryResponsibilities(context.Background(), db, false)
			if err != nil || count != test.want {
				t.Fatalf("responsibility count=%d want=%d: %v", count, test.want, err)
			}
		})
	}
}

func newChannelResponsibilityProjectionDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "wait.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, ddl := range []string{
		`CREATE TABLE channel_delivery_plans (delivery_id TEXT, source_kind TEXT, source_id TEXT, current_receipt_operation_id TEXT, state TEXT NOT NULL)`,
		`CREATE TABLE channel_delivery_receipts (delivery_id TEXT, effect_operation_id TEXT, state TEXT)`,
		`CREATE TABLE runtime_external_effect_operations (authority_kind TEXT, authority_evidence TEXT, state TEXT)`,
		`CREATE TABLE mailbox (item_id TEXT, status TEXT)`,
		`CREATE TABLE decision_cards (card_id TEXT, status TEXT)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func waitChannelDeliverySendsSettled(t *testing.T, db *sql.DB, backend servedparity.Backend) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		count, err := storetest.CountUnsettledChannelDeliveryResponsibilities(context.Background(), db, backend == servedparity.BackendExplicitPostgres)
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			return
		}
		if time.Now().After(deadline) {
			logUnsettledChannelDeliveryPlans(t, db)
			t.Fatal("initial public deliveries never settled")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
