package serveapp

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestChannelDeliveryUncertainCopyAuthorityPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, schedule := range []string{"applied_before_loss", "applied_after_completion"} {
			t.Run(string(backend)+"/"+schedule, func(t *testing.T) {
				withSummary := schedule == "applied_after_completion"
				h, _, bundleHash := startChannelAnchorJourney(t, backend, "uncertain-copy-token", withSummary)
				intentIdentity := captureChannelIntentIdentity(t, h)
				intentReader := openChannelAnchorObservation(t, h)
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "work.requested", "bundle_hash": bundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "uncertain-copy-seed"})
				cardID := waitChannelAnchorCard(t, channelAnchorStorageObservation(t, h), seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
				oldMessage := waitChannelAnchorReceipt(t, channelAnchorStorageObservation(t, h), cardID)
				oldToken, found := telegramCallbackToken(h.provider.Delivery(oldMessage-1), "reject")
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
				waitUncertainCopyPlan(t, channelSettlementStorageObservation(t, h), cardID)
				postUncertainCopyStaleControls(t, h, intentReader, intentIdentity, oldMessage, oldToken, 901010)
				callback, signing, _ := h.provider.Registration()
				resendToken, inboxMessage := requestChannelRecoveryAction(t, h.provider, callback, signing, "Resend card")
				postUncertainCopyCallback(t, h, inboxMessage, resendToken, 901020)
				fresh := waitUncertainCopyDelivery(t, h, inboxMessage, "Input: reason (text) required")
				postUncertainCopyCallback(t, h, inboxMessage, resendToken, 901020, http.StatusOK)
				postUncertainCopyCallback(t, h, inboxMessage, resendToken, 901021)
				waitObjectChannelDisposition(t, intentReader, objectIntentQuery(intentIdentity, channeldelivery.IntentAction, "901021"), "stale", "rejected")
				assertUncertainCopyWarning(t, h, cardID, "Decision: pending", 901030, withSummary)
				if err := intentReader.Close(); err != nil {
					t.Fatal(err)
				}
				h.stop(t)
				h.start(t)
				intentReader = openChannelAnchorObservation(t, h)
				postUncertainCopyStaleControls(t, h, intentReader, intentIdentity, oldMessage, oldToken, 901040)
				assertUncertainCopyWarning(t, h, cardID, "Decision: pending", 901050, withSummary)
				postUncertainCopyText(t, h, fresh, "fresh-copy-private-reason", 901060)
				waitUncertainCopyDecision(t, channelAnchorStorageObservation(t, h), cardID)
				waitChannelReceiptText(t, h, fresh, "Decision: reject", true)
				if apply != nil {
					apply()
					waitChannelReceiptText(t, h, oldMessage, "Input: reason (text) required", false)
				}
				postUncertainCopyStaleControls(t, h, intentReader, intentIdentity, oldMessage, oldToken, 901070)
				assertUncertainCopyWarning(t, h, cardID, "Decision: reject", 901080, withSummary)
				if err := intentReader.Close(); err != nil {
					t.Fatal(err)
				}
				h.stop(t)
				h.start(t)
				intentReader = openChannelAnchorObservation(t, h)
				postUncertainCopyStaleControls(t, h, intentReader, intentIdentity, oldMessage, oldToken, 901090)
				assertUncertainCopyWarning(t, h, cardID, "Decision: reject", 901100, withSummary)
				var decisions, copies, uncertainty int
				{
					var err error
					decisions, err = channelAnchorStorageObservation(t, h).CountRunNamedEventStorage(context.Background(), seed.RunID, "mailbox.card_decided")
					if err != nil || decisions != 1 {
						t.Fatalf("old controls or duplicate actions changed completion cardinality: %d, %v", decisions, err)
					}
				}
				if err := func() error {
					observed, err := channelSettlementStorageObservation(t, h).ReadCardCopyCountsStorage(context.Background(), cardID)
					copies = observed.Total
					uncertainty = observed.Uncertain
					return err
				}(); err != nil || copies != 2 || uncertainty != 1 {
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
				assertUncertainCopyRebindFence(t, h, intentReader, cardID)
			})
		}
	}
}

func assertUncertainCopyRebindFence(t *testing.T, h *channelOnboardingE2EHarness, reader channeldelivery.Observer, cardID string) {
	t.Helper()
	oldCommand := waitNativeInboxCommand(t, h.provider, "chat", "1001", "")
	confirmationIndex := 0
	for h.provider.Confirmation(confirmationIndex) != nil {
		confirmationIndex++
	}
	runChannelOnboardingCLIJourney(t, h.opts.ConfigPath, h.endpoint, h.provider, "rebind", "uncertain-copy-token", 1002, "private", confirmationIndex)
	// Both are newly received on this ingress; the old destination/control
	// is not proof that the occurrence was admitted under the old interface.
	intentIdentity := captureChannelIntentIdentity(t, h)
	command := waitNativeInboxCommand(t, h.provider, "chat", "1002", "")
	callback, signing, _ := h.provider.Registration()
	before := 0
	for h.provider.Delivery(before) != nil {
		before++
	}
	for _, entry := range []struct {
		chat, user, update int
		command            string
	}{{1001, 7000, 901110, oldCommand}, {1002, 7000 + confirmationIndex, 901111, command}} {
		postChannelTelegramUpdate(t, callback, signing, map[string]any{
			"update_id": entry.update, "message": map[string]any{
				"message_id": entry.update, "from": map[string]any{"id": entry.user},
				"chat": map[string]any{"id": entry.chat, "type": "private"}, "text": "/" + entry.command,
			},
		})
	}
	message := waitUncertainCopyDelivery(t, h, before, "Inbox\n")
	output := h.provider.Delivery(message - 1)
	if fmt.Sprint(output["chat_id"]) != "1002" || strings.Contains(fmt.Sprint(output["text"]), "Older copies may be outdated") || strings.Contains(fmt.Sprint(output["text"]), cardID[:8]) {
		t.Fatalf("rebind exposed old-epoch uncertainty to another destination: %v", output)
	}
	waitObjectChannelDisposition(t, reader, objectIntentQuery(intentIdentity, channeldelivery.IntentText, "901110"), "entry_rejected")
	for index := before; ; index++ {
		output := h.provider.Delivery(index)
		if output == nil {
			break
		}
		if fmt.Sprint(output["chat_id"]) == "1001" {
			t.Fatalf("rebound old audience received an authorized readback: %v", output)
		}
	}
}

func postUncertainCopyCallback(t *testing.T, h *channelOnboardingE2EHarness, message int, token string, update int, expected ...int) {
	t.Helper()
	status := http.StatusAccepted
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
	status := http.StatusAccepted
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

func postUncertainCopyStaleControls(t *testing.T, h *channelOnboardingE2EHarness, reader channeldelivery.Observer, identity channeldelivery.IntentObservationQuery, message int, token string, update int) {
	t.Helper()
	postUncertainCopyCallback(t, h, message, token, update)
	postUncertainCopyCallback(t, h, message, token, update, http.StatusOK)
	postUncertainCopyCallback(t, h, message, token, update+1)
	deadline := time.Now().Add(15 * time.Second)
	for {
		first, _ := observeChannelDisposition(t, reader, objectIntentQuery(identity, channeldelivery.IntentAction, fmt.Sprint(update)), "stale", "rejected")
		second, _ := observeChannelDisposition(t, reader, objectIntentQuery(identity, channeldelivery.IntentAction, fmt.Sprint(update+1)), "stale", "rejected")
		if first && second {
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
				if rejected, _ := observeChannelDisposition(t, reader, objectIntentQuery(identity, channeldelivery.IntentAction, fmt.Sprint(update+3)), "stale", "rejected"); rejected {
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
		settled, result := observeChannelDisposition(t, reader, objectIntentQuery(identity, channeldelivery.IntentText, fmt.Sprint(update+2)), "teaching")
		if settled {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale quoted input %+v lacks a durable non-mutation disposition: %+v", objectIntentQuery(identity, channeldelivery.IntentText, fmt.Sprint(update+2)), result)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitUncertainCopyPlan(t *testing.T, reader storetest.ChannelSettlementObservation, cardID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var count int
		if err := func() error {
			observed, err := reader.CountUncertainSourcePlansStorage(context.Background(), cardID)
			count = observed
			return err
		}(); err != nil {
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

func waitUncertainCopyDecision(t *testing.T, reader storetest.ChannelAnchorObservation, cardID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		observed, err := reader.ReadChannelAnchorDecisionStorage(context.Background(), cardID)
		if err != nil {
			t.Fatal(err)
		}
		status, verdict := observed.Status, observed.Verdict
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
		postChannelTelegramUpdate(t, callback, signing, map[string]any{
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
	postChannelTelegramUpdate(t, callback, signing, map[string]any{
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
