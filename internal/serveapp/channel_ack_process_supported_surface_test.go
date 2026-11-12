package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestChannelActionAcknowledgmentAbruptProcessDeathPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, presentation := range []string{"cli", "channel"} {
			cuts := []string{"launched", "settled"}
			if presentation == "channel" {
				cuts = append(cuts, "logout_while_launched")
			}
			for _, cut := range cuts {
				t.Run(string(backend)+"/"+presentation+"/"+cut, func(t *testing.T) {
					h, db, _ := startChannelAnchorJourney(t, backend, "ack-crash-token", false, true)
					observation := openChannelFixtureObservation(t, string(backend), h.storeDSN)
					if presentation == "cli" {
						runRegisteredCLIProcessRestart(t, h, cut)
						return
					}
					t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
					first := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
					h.endpoint = first.endpoint(t)
					hash := channelAnchorBundleHash(t, h.rpcEndpoint())
					waitChannelDeliverySendsSettled(t, db, backend)
					seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
						"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true},
						"idempotency_key": "ack-crash-seed",
					})
					card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate, "chat/reviews")
					messageID := waitChannelAnchorReceipt(t, db, card)
					token, ok := telegramCallbackToken(h.provider.Delivery(messageID-1), "Approve")
					if !ok {
						t.Fatal("real gate has no callback control")
					}
					intentQuery := `SELECT state,COALESCE(disposition,'') FROM operator_channel_action_intents WHERE json_extract(fact,'$.token')=$1`
					if backend == servedparity.BackendExplicitPostgres {
						intentQuery = `SELECT state,COALESCE(disposition,'') FROM operator_channel_action_intents WHERE fact->>'token'=$1`
					}
					var arrived <-chan struct{}
					var release func()
					if cut == "launched" || cut == "logout_while_launched" {
						arrived, release = h.provider.PauseNextCallbackResponse()
						t.Cleanup(release)
					}
					callback, signing, _ := h.provider.Registration()
					postChannelSourceControl(t, callback, signing, card, token, messageID)
					if arrived != nil {
						select {
						case <-arrived:
							var status, state, disposition string
							var decisions int
							if err := db.QueryRow(`SELECT status FROM decision_cards WHERE card_id=$1`, card).Scan(&status); err != nil || status != "pending" {
								t.Fatalf("ACK barrier is not before business mutation: %s %v", status, err)
							}
							if err := db.QueryRow(intentQuery, token).Scan(&state, &disposition); err != nil || state != "pending" || disposition != "" {
								t.Fatalf("verified action missing at pre-mutation barrier: %s %v", state, err)
							}
							if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&decisions); err != nil || decisions != 0 {
								t.Fatalf("pre-mutation cut already emitted a decision: %d %v", decisions, err)
							}
						case <-time.After(20 * time.Second):
							t.Fatal("callback did not reach actual provider response barrier")
						}
					} else {
						waitChannelAnchorDecision(t, db, card)
					}
					if cut == "logout_while_launched" {
						proveDeploymentLogoutDuringAcknowledgment(t, h, observation, first, card, seed.RunID, messageID, release)
						return
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
					var state, disposition string
					if err := db.QueryRow(intentQuery, token).Scan(&state, &disposition); err != nil || state != "settled" || disposition != "applied" {
						t.Fatalf("restart did not atomically complete the exact action: %s/%s %v", state, disposition, err)
					}
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
}

func proveDeploymentLogoutDuringAcknowledgment(t *testing.T, h *channelOnboardingE2EHarness, observation storetest.ChannelObservation,
	first *channelOnboardingCrashServeProcess, card, runID string, messageID int, release func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var out, errOut lockedBuffer
	done := make(chan int, 1)
	joined := false
	defer func() {
		cancel()
		release()
		if !joined {
			<-done
		}
	}()
	go func() {
		done <- cliapp.Execute(ctx, []string{"--config", h.opts.ConfigPath, "channel", "logout", "telegram", "420079",
			"--api-server", h.endpoint, "--json"}, &out, &errOut, nil, nil, ChannelRegistrations())
	}()
	select {
	case code := <-done:
		joined = true
		var retired channelonboarding.Result
		if code != 0 || json.Unmarshal([]byte(out.String()), &retired) != nil ||
			retired.Operation.OperationID != h.registration.OperationID || !retired.Operation.LogoutCompleted {
			t.Fatalf("logout could not retire its parent while the provider ACK was paused: exit=%d out=%s err=%s", code, out.String(), errOut.String())
		}
	case <-ctx.Done():
		t.Fatal("logout waited on the paused provider acknowledgment")
	}
	release()
	deadline := time.Now().Add(15 * time.Second)
	for {
		intent, found, err := observation.ObserveChannelIntent(t.Context(), render.IntentObservationQuery{Kind: render.IntentAction,
			Provider: "telegram", InterfaceKey: h.registration.Interface.Key(), ProviderEventID: channelFixtureProviderEventID(t, fmt.Sprint(976000+messageID))})
		if err != nil {
			t.Fatal(err)
		}
		if found && intent.State == "settled" && intent.Disposition == "rejected" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retired parent left action executable: %s/%s", intent.State, intent.Disposition)
		}
		time.Sleep(20 * time.Millisecond)
	}
	assertUnchanged := func() {
		t.Helper()
		state := readRegisteredChannelCard(t, h, card)
		if state.Status != decisioncard.StatusPending {
			t.Fatalf("retired ACK acquired card authority: %s", state.Status)
		}
		decisions, err := observation.EventCardinality(t.Context(), runID, "mailbox.card_decided")
		if err != nil || decisions != 0 {
			t.Fatalf("retired ACK emitted a decision: %d %v", decisions, err)
		}
		if got := len(h.provider.Acknowledgments()); got != 1 {
			t.Fatal("retired ACK was retried", got)
		}
		stateDefault, err := observation.DeliveryDefaultState(t.Context())
		if err != nil || stateDefault != "retired" {
			t.Fatalf("logout retained the deployment delivery default: %s %v", stateDefault, err)
		}
	}
	assertUnchanged()
	if err := first.stop(); err != nil {
		t.Fatal(err)
	}
	second := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
	h.endpoint = second.endpoint(t)
	assertUnchanged()
	if err := second.stop(); err != nil {
		t.Fatal(fmt.Errorf("stop post-logout serve: %w", err))
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
