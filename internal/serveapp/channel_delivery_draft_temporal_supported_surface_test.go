package serveapp

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestChannelDraftTerminalRestartPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, transition := range []string{"cancel", "expire", "external_decision", "run_stop"} {
			t.Run(string(backend)+"/"+transition, func(t *testing.T) {
				ttl := time.Duration(0)
				if transition == "expire" {
					ttl = 4 * time.Second
				}
				h, _, hash := startChannelAnchorJourneyWithDraftTTL(t, backend, "draft-temporal-token", false, ttl)
				intentIdentity := captureChannelIntentIdentity(t, h)
				intentReader := openChannelAnchorObservation(t, h)
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": "draft-temporal-seed"})
				cardID := waitChannelAnchorCard(t, channelAnchorStorageObservation(t, h), seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
				message := waitChannelAnchorReceipt(t, channelAnchorStorageObservation(t, h), cardID)
				initial := readChannelHistoricalRender(t, channelSettlementStorageObservation(t, h), cardID)
				reject, found := telegramCallbackToken(h.provider.Delivery(message-1), "reject")
				if !found {
					t.Fatalf("actual card has no required-input verdict: card=%s message=%d delivery=%v render=%s", cardID, message, h.provider.Delivery(message-1), initial.input)
				}
				postUncertainCopyCallback(t, h, message, reject, 902001)
				waitChannelReceiptText(t, h, message, "Input: reason (text) required", false)
				acknowledgedPrompt := waitChannelDraftPromptSettlement(t, channelSettlementStorageObservation(t, h), cardID)
				prompt := readChannelHistoricalRender(t, channelSettlementStorageObservation(t, h), cardID)
				cancel := latestChannelDraftCancel(t, h, message)
				draftStatus, cardStatus, decisions := "cancelled", "pending", 0
				var response map[string]any
				switch transition {
				case "cancel":
					if err := intentReader.Close(); err != nil {
						t.Fatal(err)
					}
					h.stop(t)
					h.start(t)
					intentReader = openChannelAnchorObservation(t, h)
					postUncertainCopyCallback(t, h, message, cancel, 902010)
				case "expire":
					var detail struct {
						Card struct {
							Cadence decisioncard.Cadence `json:"effective_cadence"`
						} `json:"decision_card"`
					}
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": cardID}, &detail)
					if detail.Card.Cadence.InputDraftTTL != ttl.String() {
						t.Fatalf("card did not consume configured canonical TTL: %+v", detail.Card.Cadence)
					}
					if err := intentReader.Close(); err != nil {
						t.Fatal(err)
					}
					h.stop(t)
					if remaining := time.Until(acknowledgedPrompt.Prompt.ExpiresAt); remaining > 0 {
						time.Sleep(remaining)
					}
					h.start(t)
					intentReader = openChannelAnchorObservation(t, h)
					draftStatus = "expired"
				case "external_decision":
					params := lifecycleDecisionParamsForCard(t, h.rpcEndpoint(), cardID, "approve")
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.decide", params, &response)
					cardStatus, decisions = "decided", 1
				case "run_stop":
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.stop", map[string]any{"run_id": seed.RunID, "idempotency_key": "draft-run-stop"}, &response)
					cardStatus = "superseded"
				}
				waitChannelDraftStatus(t, channelSettlementStorageObservation(t, h), cardID, draftStatus)
				for _, restart := range []bool{false, true} {
					if restart {
						if err := intentReader.Close(); err != nil {
							t.Fatal(err)
						}
						h.stop(t)
						h.start(t)
						intentReader = openChannelAnchorObservation(t, h)
					}
					update := 902020
					if restart {
						update += 10
					}
					postUncertainCopyCallback(t, h, message, cancel, update)
					postUncertainCopyCallback(t, h, message, cancel, update, http.StatusOK)
					waitObjectChannelDisposition(t, intentReader, objectIntentQuery(intentIdentity, channeldelivery.IntentAction, fmt.Sprint(update)), "stale", "rejected")
					postUncertainCopyText(t, h, message, "obsolete-private-draft-answer", update+1)
					postUncertainCopyText(t, h, message, "obsolete-private-draft-answer", update+1, http.StatusOK)
					waitObjectChannelDisposition(t, intentReader, objectIntentQuery(intentIdentity, channeldelivery.IntentText, fmt.Sprint(update+1)), "teaching")
					var actualStatus string
					var actualDecisions int
					if err := func() error {
						observed, err := channelSettlementStorageObservation(t, h).ReadChannelAnchorDecisionStorage(context.Background(), cardID)
						actualStatus = observed.Status
						return err
					}(); err != nil || actualStatus != cardStatus {
						t.Fatalf("obsolete input changed canonical card: %s %v", actualStatus, err)
					}
					{
						var err error
						actualDecisions, err = channelAnchorStorageObservation(t, h).CountRunNamedEventStorage(context.Background(), seed.RunID, "mailbox.card_decided")
						if err != nil || actualDecisions != decisions {
							t.Fatalf("obsolete input changed decision count: %d %v", actualDecisions, err)
						}
					}
					assertChannelHistoricalRender(t, channelSettlementStorageObservation(t, h), cardID, initial)
					assertChannelHistoricalRender(t, channelSettlementStorageObservation(t, h), cardID, prompt)
					waitChannelDraftStatus(t, channelSettlementStorageObservation(t, h), cardID, draftStatus)
				}
				for _, edit := range h.provider.Edits() {
					if strings.Contains(fmt.Sprint(edit["text"]), "obsolete-private-draft-answer") {
						t.Fatal("rejected private answer leaked into remote presentation")
					}
				}
			})
		}
	}
}

type channelHistoricalRender struct {
	id, input, hash, snapshot string
}

func readChannelHistoricalRender(t *testing.T, reader storetest.ChannelSettlementObservation, cardID string) channelHistoricalRender {
	t.Helper()
	var frozen channelHistoricalRender
	if err := func() error {
		observed, err := reader.ReadChannelCardRenderStorage(context.Background(), cardID)
		frozen.id = observed.RenderID
		frozen.input = observed.Input
		frozen.hash = observed.Hash
		frozen.snapshot = observed.Snapshot
		return err
	}(); err != nil {
		t.Fatal(err)
	}
	return frozen
}

func assertChannelHistoricalRender(t *testing.T, reader storetest.ChannelSettlementObservation, cardID string, frozen channelHistoricalRender) {
	t.Helper()
	var input, hash, snapshot string
	if err := func() error {
		observed, err := reader.ReadChannelRenderStorage(context.Background(), frozen.id)
		input = observed.Input
		hash = observed.Hash
		return err
	}(); err != nil {
		t.Fatal(err)
	}
	if err := func() error {
		observed, err := reader.ReadCardSnapshotStorage(context.Background(), cardID)
		snapshot = observed
		return err
	}(); err != nil {
		t.Fatal(err)
	}
	if input != frozen.input || hash != frozen.hash || snapshot != frozen.snapshot {
		t.Fatal("draft transition or restart overwrote canonical frozen history")
	}
}

func latestChannelDraftCancel(t *testing.T, h *channelOnboardingE2EHarness, message int) string {
	t.Helper()
	edits := h.provider.Edits()
	for index := len(edits) - 1; index >= 0; index-- {
		if fmt.Sprint(edits[index]["message_id"]) == fmt.Sprint(message) {
			if token, found := telegramCallbackToken(edits[index], "Cancel input"); found {
				return token
			}
		}
	}
	t.Fatal("acknowledged input prompt lacks its actual Cancel input control")
	return ""
}

func waitChannelDraftPromptSettlement(t *testing.T, reader storetest.ChannelSettlementObservation, cardID string) channeldelivery.Frozen {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var input, hash string
		err := func() error {
			observed, err := reader.ReadAcknowledgedCardRenderStorage(context.Background(), cardID)
			input = observed.Input
			hash = observed.Hash
			return err
		}()
		if err == nil {
			canonical, err := canonicaljson.Canonicalize([]byte(input))
			if err != nil {
				t.Fatal(err)
			}
			frozen, err := channeldelivery.Decode(canonical, hash)
			if err != nil {
				t.Fatal(err)
			}
			if frozen.Prompt != nil && frozen.Prompt.DraftID != "" {
				return frozen
			}
		} else if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("actual managed prompt edit has not acknowledged its exact current receipt/render")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitChannelDraftStatus(t *testing.T, reader storetest.ChannelSettlementObservation, cardID, status string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var actual string
		if err := func() error {
			observed, err := reader.ReadChannelDraftStatusStorage(context.Background(), cardID)
			actual = observed
			return err
		}(); err != nil {
			t.Fatal(err)
		}
		if actual == status {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("canonical draft status=%s, want %s", actual, status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
