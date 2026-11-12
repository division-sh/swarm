package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/servedparity"
)

// The provider double and source policy own bot 420079. Preserve the exact
// account/update tuple; a customer or another registered bot cannot supply it.
func channelFixtureProviderEventID(t testing.TB, update string) string {
	t.Helper()
	id, err := runtimeinbound.SemanticFingerprint([]string{"telegram", "420079", update})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestChannelDeliveryUncertainCopyAuthorityPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, schedule := range []string{"applied_before_loss", "applied_after_completion"} {
			t.Run(string(backend)+"/"+schedule, func(t *testing.T) {
				withSummary := schedule == "applied_after_completion"
				h, db, bundleHash := startChannelAnchorJourney(t, backend, "uncertain-copy-token", withSummary)
				// The summary fixture connects after serve is ready; its native
				// recovery entry must finish installation before driving recovery.
				waitNativeInboxCommand(t, h.provider, "chat", "1001", "")
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": bundleHash, "run_id": seedServedPublicationRun(t, h.runtime),
					"payload": map[string]any{"seed": true}, "idempotency_key": "uncertain-copy-seed",
				})
				cardID := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate, "chat/reviews")
				oldMessage := waitChannelAnchorReceipt(t, db, cardID)
				oldToken, found := telegramCallbackToken(h.provider.Delivery(oldMessage-1), "Reject")
				if !found {
					t.Fatal("actual gate card lacks the required-input reject action")
				}
				var delayed <-chan struct{}
				var apply func()
				if schedule == "applied_after_completion" {
					delayed, apply = h.provider.LoseNextEditAcknowledgmentBeforeApply()
					t.Cleanup(apply)
				} else {
					h.provider.LoseNextEditAcknowledgment()
				}
				postUncertainCopyCallback(t, h, oldMessage, oldToken, 901001)
				if delayed != nil {
					select {
					case <-delayed:
					case <-time.After(20 * time.Second):
						t.Fatal("provider did not retain the unacknowledged edit")
					}
				}
				waitUncertainCopyPlan(t, db, cardID)
				postUncertainCopyStaleControls(t, h, db, oldMessage, oldToken, 901010)
				callback, signing, _ := h.provider.Registration()
				resendToken, inboxMessage := requestChannelRecoveryAction(t, h.provider, callback, signing, "Resend card")
				postUncertainCopyCallback(t, h, inboxMessage, resendToken, 901020)
				fresh := waitUncertainCopyDelivery(t, h, inboxMessage, "Input: Reason (text) required")
				postUncertainCopyCallback(t, h, inboxMessage, resendToken, 901020, http.StatusOK)
				postUncertainCopyCallback(t, h, inboxMessage, resendToken, 901021)
				waitChannelRejectedCallback(t, db, resendToken)
				assertUncertainCopyWarning(t, h, cardID, "Decision: pending", 901030, withSummary)
				h.stop(t)
				h.start(t)
				postUncertainCopyStaleControls(t, h, db, oldMessage, oldToken, 901040)
				assertUncertainCopyWarning(t, h, cardID, "Decision: pending", 901050, withSummary)
				postUncertainCopyText(t, h, fresh, "fresh-copy-private-reason", 901060)
				waitUncertainCopyDecision(t, db, cardID)
				waitChannelReceiptText(t, h, fresh, "Decision: reject", true)
				if apply != nil {
					apply()
					waitChannelReceiptText(t, h, oldMessage, "Input: Reason (text) required", false)
				}
				postUncertainCopyStaleControls(t, h, db, oldMessage, oldToken, 901070)
				assertUncertainCopyWarning(t, h, cardID, "Decision: reject", 901080, withSummary)
				h.stop(t)
				h.start(t)
				postUncertainCopyStaleControls(t, h, db, oldMessage, oldToken, 901090)
				assertUncertainCopyWarning(t, h, cardID, "Decision: reject", 901100, withSummary)
				var decisions, copies, uncertainty int
				if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&decisions); err != nil || decisions != 1 {
					t.Fatalf("old controls or duplicate actions changed completion cardinality: %d, %v", decisions, err)
				}
				if err := db.QueryRow(`SELECT COUNT(*),SUM(CASE WHEN state='uncertain' THEN 1 ELSE 0 END)
					FROM channel_delivery_plans WHERE source_kind='card' AND source_id=$1`, cardID).Scan(&copies, &uncertainty); err != nil || copies != 2 || uncertainty != 1 {
					t.Fatalf("resend/restart settled the predecessor or created extra copies: %d/%d, %v", copies, uncertainty, err)
				}
				oldEdits := 0
				for _, edit := range h.provider.Edits() {
					if fmt.Sprint(edit["message_id"]) == fmt.Sprint(oldMessage) {
						oldEdits++
					}
					if strings.Contains(fmt.Sprint(edit["text"]), "fresh-copy-private-reason") {
						t.Fatal("private input appeared in an externally visible edit")
					}
				}
				if oldEdits != 1 {
					t.Fatalf("uncertain provider write was automatically retried/re-edited: %d", oldEdits)
				}
				assertUncertainCopyLogoutConnectFence(t, h, cardID, backend)
			})
		}
	}
}

func assertUncertainCopyLogoutConnectFence(t *testing.T, h *channelOnboardingE2EHarness, cardID string, backend servedparity.Backend) {
	t.Helper()
	observer := openChannelFixtureObservation(t, string(backend), h.storeDSN)
	oldCommand := waitNativeInboxCommand(t, h.provider, "chat", "1001", "")
	originalIndex, err := observer.RegistrationIndex(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	priorParent := h.registration.OperationID
	logoutDeploymentChannelFixtureAccount(t, h)
	h.registration = connectDeploymentChannelFixtureOperator(t, h, "uncertain-copy-token", 901209, 1002, 7002, "private")
	parents, confirmed, retired, err := observer.RegistrationCounts(t.Context())
	if err != nil ||
		parents != 2 || confirmed != 1 || retired != 1 || h.registration.OperationID == priorParent {
		t.Fatalf("logout/connect retained a second reservation: parents=%d confirmed=%d retired=%d err=%v", parents, confirmed, retired, err)
	}
	currentIndex, err := observer.RegistrationIndex(t.Context())
	if err != nil || currentIndex != originalIndex {
		t.Fatalf("logout/connect changed the account uniqueness constraint: %v", err)
	}
	t.Logf("logout/connect: historical_parents=%d confirmed_current=%d retired_logout=%d unique_index_unchanged=true operator_chat=1002", parents, confirmed, retired)
	command := waitNativeInboxCommand(t, h.provider, "chat", "1002", "")
	callback, signing, _ := h.provider.Registration()
	before := 0
	for h.provider.Delivery(before) != nil {
		before++
	}
	for _, entry := range []struct {
		chat, user, update int
		command            string
	}{{1001, 7000, 901110, oldCommand}, {1002, 7002, 901111, command}} {
		status := http.StatusOK
		if entry.chat == 1001 {
			// The retired audience is no longer the deployment's operator.
			// Its holder-routed intent must still settle entry_rejected below.
			status = http.StatusAccepted
		}
		if names := postChannelTelegramUpdateStatus(t, callback, signing, map[string]any{
			"update_id": entry.update, "message": map[string]any{
				"message_id": entry.update, "from": map[string]any{"id": entry.user},
				"chat": map[string]any{"id": entry.chat, "type": "private"}, "text": "/" + entry.command,
			},
		}, status); len(names) != 0 {
			t.Fatalf("operator Inbox command escaped into business events: %v", names)
		}
	}
	message := waitUncertainCopyDelivery(t, h, before, "Inbox\n")
	output := h.provider.Delivery(message - 1)
	if fmt.Sprint(output["chat_id"]) != "1002" || strings.Contains(fmt.Sprint(output["text"]), "Older copies may be outdated") || strings.Contains(fmt.Sprint(output["text"]), cardID[:8]) {
		t.Fatalf("logout/connect exposed old-epoch uncertainty to another destination: %v", output)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		// Independent SHA-256 of ["telegram","420079","901110"], not an
		// identifier learned from the incoming receipt being checked.
		rejected, err := observer.RejectedEntryCount(t.Context(), "788f337676ac8e8813e269fe48d0dfa5327d81c5893120d287a2f8864e97644b")
		if err != nil {
			t.Fatal(err)
		}
		if rejected == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("old destination retained inbox readback authority after logout/connect")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for index := before; ; index++ {
		output := h.provider.Delivery(index)
		if output == nil {
			break
		}
		if fmt.Sprint(output["chat_id"]) == "1001" {
			t.Fatalf("logged-out audience received an authorized readback: %v", output)
		}
	}
}

func logoutDeploymentChannelFixtureAccount(t *testing.T, h *channelOnboardingE2EHarness) {
	t.Helper()
	var out, errOut lockedBuffer
	code := cliapp.Execute(t.Context(), []string{"--config", h.opts.ConfigPath, "channel", "logout", "telegram", "420079", "--api-server", h.endpoint, "--json"}, &out, &errOut, nil, nil, OpenChannelRegistration)
	var result channelonboarding.Result
	if code != 0 || json.Unmarshal([]byte(out.String()), &result) != nil || result.Operation.OperationID != h.registration.OperationID ||
		!result.Operation.LogoutCompleted || result.Operation.Phase != channelonboarding.PhaseRetired {
		t.Fatalf("deployment logout did not retire the registered account: exit=%d out=%s err=%s", code, out.String(), errOut.String())
	}
	h.registration = result.Operation
	out, errOut = lockedBuffer{}, lockedBuffer{}
	code = cliapp.Execute(t.Context(), []string{"--config", h.opts.ConfigPath, "channel", "list", "--api-server", h.endpoint, "--json"}, &out, &errOut, nil, nil, OpenChannelRegistration)
	var listing struct {
		Accounts []channelonboarding.Result `json:"accounts"`
	}
	if code != 0 || json.Unmarshal([]byte(out.String()), &listing) != nil || len(listing.Accounts) != 0 {
		t.Fatalf("logged-out account retained a public reservation: exit=%d out=%s err=%s", code, out.String(), errOut.String())
	}
	t.Log("public deployment logout completed; channel list has zero current accounts")
}

func connectDeploymentChannelFixtureOperator(t *testing.T, h *channelOnboardingE2EHarness, credential string, claimUpdateID int64, chatID, userID int, chatType string, beforeClaim ...func()) channelonboarding.Operation {
	t.Helper()
	restore := installChannelOnboardingCLIInput(t, credential+"\n")
	defer restore()
	out, errOut := &lockedBuffer{}, &lockedBuffer{}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	done, progress := make(chan int, 1), make(chan int, 1)
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	go func() {
		args := []string{"--config", h.opts.ConfigPath, "channel", "connect", "telegram", "--yes",
			"--client-language", "en", "--api-server", h.endpoint}
		if h.registration.IdentityOperationID == "" {
			args = append(args, "420079")
		}
		code := cliapp.Execute(ctx, args, out, errOut, nil, nil, OpenChannelRegistration)
		done <- code
		progress <- code
	}()
	challenge := waitChannelOnboardingChallenge(t, out, errOut, progress)
	if len(beforeClaim) != 0 {
		beforeClaim[0]()
	}
	callback, signing, _ := h.provider.Registration()
	// A new ceremony is a new provider event, not reuse of the prior claim's
	// immutable receipt under a different parent and challenge.
	claim := submitChannelOnboardingClaimWithChatType(t, callback, signing, challenge, claimUpdateID, int64(userID), int64(chatID), chatType, "replacement_operator")
	if claim.StatusCode != http.StatusOK || len(claim.EventIDs) != 0 || len(claim.EventNames) != 0 {
		t.Fatalf("deployment connect claim escaped the deployment receiver: %+v", claim)
	}
	select {
	case code := <-done:
		joined = true
		if code != 0 {
			t.Fatalf("deployment connect exit=%d out=%s err=%s", code, out.String(), errOut.String())
		}
	case <-ctx.Done():
		t.Fatalf("deployment connect did not complete: out=%s err=%s", out.String(), errOut.String())
	}
	if chatType != "private" && !strings.Contains(out.String(), "Future notices") && !strings.Contains(out.String(), "future notices, decision cards, and their updates") {
		t.Fatalf("deployment operator claim omitted shared audience disclosure: %s", out.String())
	}
	var listing struct {
		Accounts []channelonboarding.Result `json:"accounts"`
	}
	var listed, listErr lockedBuffer
	code := cliapp.Execute(t.Context(), []string{"--config", h.opts.ConfigPath, "channel", "list", "--api-server", h.endpoint, "--json"}, &listed, &listErr, nil, nil, OpenChannelRegistration)
	if code != 0 || json.Unmarshal([]byte(listed.String()), &listing) != nil || len(listing.Accounts) != 1 ||
		listing.Accounts[0].Binding == nil || listing.Accounts[0].Binding.ConversationRef != fmt.Sprint(chatID) ||
		!listing.Accounts[0].Operation.DeploymentRegistration || !listing.Accounts[0].Operation.AccountConfirmed ||
		listing.Accounts[0].Operation.Phase != channelonboarding.PhaseSucceeded {
		t.Fatalf("deployment connect lost its confirmed current binding: exit=%d out=%s err=%s", code, listed.String(), listErr.String())
	}
	return listing.Accounts[0].Operation
}

func postUncertainCopyCallback(t *testing.T, h *channelOnboardingE2EHarness, message int, token string, update int, expected ...int) {
	t.Helper()
	status := http.StatusOK
	if !h.registration.DeploymentRegistration {
		status = http.StatusAccepted
	}
	if len(expected) != 0 {
		status = expected[0]
	}
	callback, signing, _ := h.provider.Registration()
	if names := postChannelTelegramUpdateStatus(t, callback, signing, map[string]any{
		"update_id": update, "callback_query": map[string]any{
			"id": fmt.Sprint(update), "from": map[string]any{"id": 7000},
			"message": map[string]any{"message_id": message, "chat": map[string]any{"id": 1001, "type": "private"}}, "data": token,
		},
	}, status); len(names) != 0 {
		t.Fatalf("operator control escaped into business events: %v", names)
	}
}

func postUncertainCopyText(t *testing.T, h *channelOnboardingE2EHarness, message int, text string, update int, expected ...int) {
	t.Helper()
	status := http.StatusOK
	if !h.registration.DeploymentRegistration {
		status = http.StatusAccepted
	}
	if len(expected) != 0 {
		status = expected[0]
	}
	callback, signing, _ := h.provider.Registration()
	if names := postChannelTelegramUpdateStatus(t, callback, signing, map[string]any{
		"update_id": update, "message": map[string]any{
			"message_id": update, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": 1001, "type": "private"}, "text": text,
			"reply_to_message": map[string]any{"message_id": message},
		},
	}, status); len(names) != 0 {
		t.Fatalf("quoted operator input escaped into business events: %v", names)
	}
}

func postUncertainCopyStaleControls(t *testing.T, h *channelOnboardingE2EHarness, db *sql.DB, message int, token string, update int) {
	t.Helper()
	postUncertainCopyCallback(t, h, message, token, update)
	postUncertainCopyCallback(t, h, message, token, update, http.StatusOK)
	postUncertainCopyCallback(t, h, message, token, update+1)
	deadline := time.Now().Add(15 * time.Second)
	for {
		var settled int
		if err := db.QueryRow(`SELECT COUNT(*) FROM operator_channel_action_intents WHERE state='settled'
			AND disposition IN ('stale','rejected') AND provider_event_id IN ($1,$2)`, channelFixtureProviderEventID(t, fmt.Sprint(update)), channelFixtureProviderEventID(t, fmt.Sprint(update+1))).Scan(&settled); err != nil {
			t.Fatal(err)
		}
		if settled == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("this stale callback occurrence lacks an exact durable non-mutation disposition")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, edit := range h.provider.Edits() {
		if fmt.Sprint(edit["message_id"]) != fmt.Sprint(message) {
			continue
		}
		if cancel, found := telegramCallbackToken(edit, "Cancel input"); found {
			postUncertainCopyCallback(t, h, message, cancel, update+3)
			postUncertainCopyCallback(t, h, message, cancel, update+3, http.StatusOK)
			for {
				var rejected int
				if err := db.QueryRow(`SELECT COUNT(*) FROM operator_channel_action_intents WHERE state='settled'
					AND disposition IN ('stale','rejected') AND provider_event_id=$1`, channelFixtureProviderEventID(t, fmt.Sprint(update+3))).Scan(&rejected); err != nil {
					t.Fatal(err)
				}
				if rejected == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the uncertain prompt's Cancel input control retained mutation authority")
				}
				time.Sleep(20 * time.Millisecond)
			}
			break
		}
	}
	answer := fmt.Sprintf("obsolete-private-answer-%d", update)
	postUncertainCopyText(t, h, message, answer, update+2)
	postUncertainCopyText(t, h, message, answer, update+2, http.StatusOK)
	deadline = time.Now().Add(15 * time.Second)
	for {
		var settled int
		if err := db.QueryRow(`SELECT COUNT(*) FROM operator_channel_text_intents WHERE state='settled'
			AND disposition='teaching' AND provider_event_id=$1`, channelFixtureProviderEventID(t, fmt.Sprint(update+2))).Scan(&settled); err != nil {
			t.Fatal(err)
		}
		if settled > 0 {
			return
		}
		if time.Now().After(deadline) {
			var state, disposition, fact string
			err := db.QueryRow(`SELECT state,COALESCE(disposition,''),CAST(fact AS TEXT) FROM operator_channel_text_intents WHERE provider_event_id=$1`, channelFixtureProviderEventID(t, fmt.Sprint(update+2))).Scan(&state, &disposition, &fact)
			t.Fatalf("stale quoted input lacks a durable non-mutation disposition: %s/%s %s %v", state, disposition, fact, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitUncertainCopyPlan(t *testing.T, db *sql.DB, cardID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM channel_delivery_plans WHERE source_id=$1 AND state='uncertain'`, cardID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("unacknowledged edit did not become durably uncertain")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitUncertainCopyDelivery(t *testing.T, h *channelOnboardingE2EHarness, after int, text string) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		for index := after; ; index++ {
			message := h.provider.Delivery(index)
			if message == nil {
				break
			}
			if strings.Contains(fmt.Sprint(message["text"]), text) {
				return index + 1
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("fresh copy lacks %q", text)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitUncertainCopyDecision(t *testing.T, db *sql.DB, cardID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var status, verdict string
		if err := db.QueryRow(`SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, cardID).Scan(&status, &verdict); err != nil {
			t.Fatal(err)
		}
		if status == "decided" && verdict == "reject" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("fresh copy did not finish the actual decision: %s/%s", status, verdict)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertUncertainCopyWarning(t *testing.T, h *channelOnboardingE2EHarness, cardID, decision string, update int, withSummary bool) {
	t.Helper()
	if withSummary {
		message, token := channelUncertaintySummaryEntry(t, h.provider)
		callback, signing, _ := h.provider.Registration()
		before := 0
		for h.provider.Delivery(before) != nil {
			before++
		}
		postChannelTelegramOperatorUpdate(t, callback, signing, map[string]any{
			"update_id": update + 1,
			"callback_query": map[string]any{
				"id": fmt.Sprintf("uncertain-card-summary-%d", update), "from": map[string]any{"id": 7000},
				"message": map[string]any{"message_id": message, "chat": map[string]any{"id": 1001, "type": "private"}},
				"data":    token,
			},
		})
		response := waitUncertainCopyDelivery(t, h, before, "Older copies may be outdated")
		assertUncertainCopyWarningText(t, h.provider.Delivery(response-1), cardID, decision)
	}
	command := waitNativeInboxCommand(t, h.provider, "chat", "1001", "")
	before := 0
	for h.provider.Delivery(before) != nil {
		before++
	}
	callback, signing, _ := h.provider.Registration()
	postChannelTelegramOperatorUpdate(t, callback, signing, map[string]any{
		"update_id": update, "message": map[string]any{
			"message_id": update, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": 1001, "type": "private"}, "text": "/" + command,
		},
	})
	message := waitUncertainCopyDelivery(t, h, before, "Older copies may be outdated")
	assertUncertainCopyWarningText(t, h.provider.Delivery(message-1), cardID, decision)
}

func assertUncertainCopyWarningText(t *testing.T, message map[string]any, cardID, decision string) {
	t.Helper()
	text := fmt.Sprint(message["text"])
	if !strings.Contains(text, cardID[:8]) || !strings.Contains(text, decision) || strings.Contains(text, "obsolete-private-answer") {
		t.Fatalf("uncertainty readback lost its exact canonical outcome or privacy: %s", text)
	}
	if decision != "Decision: pending" {
		if telegramHasActionPrefix(message, "Resend card") {
			t.Fatal("completion made the uncertain terminal decision resendable")
		}
	}
}
