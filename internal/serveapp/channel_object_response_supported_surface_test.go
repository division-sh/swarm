package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestChannelLearnedObjectRecoveryPagingPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, schedule := range []string{"current", "restart", "edit_ack_loss"} {
			t.Run(string(backend)+"/"+schedule, func(t *testing.T) {
				h, _, p, command, hash := startObjectChannelInputJourney(t, backend, false)
				cards := createObjectUncertainCards(t, h, p, hash)
				receipt, deliveryID := requestObjectRecoveryInbox(t, h, p, command, "recovery-entry")
				if schedule == "restart" {
					closeObjectIntentObservation(t, p)
					h.stop(t)
					h.start(t)
					p.intentReader = openChannelAnchorObservation(t, h)
				}
				if schedule == "edit_ack_loss" {
					proveObjectResponseEditLoss(t, h, p, deliveryID, receipt)
					receipt, deliveryID = requestObjectRecoveryInbox(t, h, p, command, "recovery-fresh-entry")
				}
				proveObjectAllRecoveryChoices(t, h, p, receipt, cards)
				waitObjectDeliveryState(t, channelSettlementStorageObservation(t, h), deliveryID, "sent")
			})
		}
	}
}

func createObjectUncertainCards(t *testing.T, h *channelOnboardingE2EHarness, p *objectChannelProvider, hash string) []string {
	t.Helper()
	cards := make([]string, 3)
	for index := range cards {
		p.mu.Lock()
		p.loseNextCard = true
		p.mu.Unlock()
		seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"detail": strings.Repeat("uncertain recovery detail;", 60)}, "idempotency_key": fmt.Sprintf("multi-recovery-%d", index)})
		cards[index] = waitChannelAnchorCard(t, channelAnchorStorageObservation(t, h), seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
		waitUncertainCopyPlan(t, channelSettlementStorageObservation(t, h), cards[index])
	}
	return cards
}

func requestObjectRecoveryInbox(t *testing.T, h *channelOnboardingE2EHarness, p *objectChannelProvider, command, id string) (string, string) {
	t.Helper()
	postObjectChannelFact(t, p, id, map[string]any{"text": "/" + command, "entry": map[string]any{"reference": command, "address": "mock_bot"}})
	waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, id), "entry")
	deliveryID, receipt := waitObjectRequestedResponse(t, channelSettlementStorageObservation(t, h), id)
	return receipt, deliveryID
}

func waitObjectRequestedResponse(t *testing.T, reader storetest.ChannelSettlementObservation, eventID string) (string, string) {
	t.Helper()
	return waitObjectIntentResponse(t, reader, channeldelivery.IntentText, eventID)
}

func waitObjectIntentResponse(t *testing.T, reader storetest.ChannelSettlementObservation, kind channeldelivery.IntentKind, eventID string) (string, string) {
	t.Helper()
	if kind != channeldelivery.IntentText && kind != channeldelivery.IntentAction {
		t.Fatalf("unsupported response intent kind %q", kind)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		var deliveryID, raw string
		err := func() error {
			observed, err := reader.ReadIntentSentResponseStorage(context.Background(), kind, eventID)
			deliveryID = observed.DeliveryID
			raw = observed.Reference
			return err
		}()
		if err == nil {
			var receipt struct {
				Reference struct {
					ID string `json:"id"`
				} `json:"delivery_reference"`
			}
			if err := json.Unmarshal([]byte(raw), &receipt); err != nil || receipt.Reference.ID == "" {
				t.Fatalf("invalid object response receipt %s: %v", raw, err)
			}
			return deliveryID, receipt.Reference.ID
		}
		if err != sql.ErrNoRows || time.Now().After(deadline) {
			t.Fatalf("requested response %s not delivered: %v", eventID, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveObjectAllRecoveryChoices(t *testing.T, h *channelOnboardingE2EHarness, p *objectChannelProvider, receipt string, cards []string) {
	t.Helper()
	seen := map[string]bool{}
	for page := 0; page < 10 && len(seen) < len(cards); page++ {
		p.mu.Lock()
		input := p.message(receipt)
		controls, _ := input["controls"].([]any)
		var next, resend string
		for _, control := range controls {
			row, _ := control.(map[string]any)
			if strings.HasPrefix(fmt.Sprint(row["name"]), "Resend card") {
				resend, _ = row["value"].(string)
			}
			if row["name"] == "More choices" {
				next, _ = row["value"].(string)
			}
		}
		p.mu.Unlock()
		if resend != "" && !seen[resend] {
			seen[resend] = true
			postObjectInputAction(t, p, fmt.Sprintf("recover-resend-%d", page), receipt, resend)
			waitObjectFreshCopyCount(t, channelSettlementStorageObservation(t, h), len(seen))
			id := fmt.Sprintf("recover-repeat-%d", page)
			postObjectChannelFact(t, p, id, map[string]any{"token": resend, "cursor": id, "message": map[string]any{"id": receipt}})
			waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentAction, id), "stale", "rejected")
		}
		if len(seen) < len(cards) {
			if next == "" {
				t.Fatalf("recovery dropped choices after %d pages: %#v", page, input)
			}
			postObjectInputAction(t, p, fmt.Sprintf("recover-page-%d", page), receipt, next)
			waitObjectEditAfterAction(t, p, receipt, next)
		}
	}
	if len(seen) != len(cards) {
		t.Fatalf("only %d/%d explicit recovery choices reachable", len(seen), len(cards))
	}
	closeObjectIntentObservation(t, p)
	h.stop(t)
	h.start(t)
	p.intentReader = openChannelAnchorObservation(t, h)
	for _, card := range cards {
		fresh := waitObjectCardReceipt(t, channelSettlementStorageObservation(t, h), card)
		if fresh == "" {
			t.Fatal("restart lost healthy fresh-copy receipt")
		}
		var copies, uncertain int
		if err := func() error {
			observed, err := channelSettlementStorageObservation(t, h).ReadCardCopyCountsStorage(context.Background(), card)
			copies = observed.Total
			uncertain = observed.Uncertain
			return err
		}(); err != nil || copies != 2 || uncertain != 1 {
			t.Fatalf("recovery replayed or settled predecessor: %d/%d %v", copies, uncertain, err)
		}
	}
}

func waitObjectFreshCopyCount(t *testing.T, reader storetest.ChannelSettlementObservation, count int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var current int
		if err := func() error {
			observed, err := reader.CountSentFreshCardCopiesStorage(context.Background())
			current = observed
			return err
		}(); err != nil {
			t.Fatal(err)
		}
		if current == count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("healthy explicit copies=%d, want %d", current, count)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveObjectResponseEditLoss(t *testing.T, h *channelOnboardingE2EHarness, p *objectChannelProvider, deliveryID, receipt string) {
	t.Helper()
	old := waitObjectMessageControl(t, p, receipt, "More choices")
	p.mu.Lock()
	p.loseResponseEdit = receipt
	p.mu.Unlock()
	postObjectInputAction(t, p, "response-lost-edit", receipt, old)
	waitObjectDeliveryState(t, channelSettlementStorageObservation(t, h), deliveryID, "uncertain")
	for _, restart := range []bool{false, true} {
		if restart {
			closeObjectIntentObservation(t, p)
			h.stop(t)
			h.start(t)
			p.intentReader = openChannelAnchorObservation(t, h)
		}
		id := fmt.Sprintf("rsp-stale-%t", restart)
		postObjectChannelFact(t, p, id, map[string]any{"token": old, "cursor": id, "message": map[string]any{"id": receipt}})
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentAction, id), "stale", "rejected")
		postObjectChannelFact(t, p, id+"-text", map[string]any{"text": "object-private-obsolete", "reply": map[string]any{"id": receipt}})
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, id+"-text"), "teaching")
	}
	p.mu.Lock()
	edits := 0
	for _, input := range p.calls["/v2/edit"] {
		ref, _ := input["reference"].(map[string]any)
		if ref["id"] == receipt {
			edits++
		}
	}
	p.mu.Unlock()
	if edits != 1 {
		t.Fatalf("uncertain response edit dispatched %d times", edits)
	}
	waitObjectDeliveryState(t, channelSettlementStorageObservation(t, h), deliveryID, "uncertain")
}

func waitObjectDeliveryState(t *testing.T, reader storetest.ChannelSettlementObservation, deliveryID, expected string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var state string
		if err := func() error {
			observed, err := reader.ReadDeliveryStateStorage(context.Background(), deliveryID)
			state = observed
			return err
		}(); err != nil {
			t.Fatal(err)
		}
		if state == expected {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("response plan %s state=%s, want %s", deliveryID, state, expected)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
