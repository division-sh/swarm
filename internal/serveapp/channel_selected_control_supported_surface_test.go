package serveapp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// This is a downstream admission proof, not a public selected-fork creation
// journey. Public begin_input refuses; the retained drafts are controlled
// negatives for the otherwise unreachable partial and final input consumers.
func TestChannelSelectedForkInputBoundaryMatrix(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, mode := range []string{"plain", "quoted", "chosen", "skip"} {
			for _, partial := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/partial=%t", backend, mode, partial), func(t *testing.T) {
					var selected any
					captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { selected = p.deps.EventStore })
					root := canonicalrouting.CopyChannelLearnedObjectOptionalInputJourney(t, partial)
					h, _, p, _, hash := startObjectChannelJourney(t, backend, root, telegramPhraseBotLLMRuntime{}, "")
					card, receipt, reject := beginObjectInputCard(t, h, p, hash, 0)
					firstCard := card
					postObjectInputAction(t, p, "selected-input-begin", receipt, reject)
					waitChannelDraftPromptSettlement(t, channelSettlementStorageObservation(t, h), card)
					var other string
					if mode == "chosen" {
						otherCard, otherReceipt, otherReject := beginObjectInputCard(t, h, p, hash, 1)
						other = otherCard
						postObjectInputAction(t, p, "selected-other-begin", otherReceipt, otherReject)
						waitChannelDraftPromptSettlement(t, channelSettlementStorageObservation(t, h), other)
					}
					before := selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), card)
					var token, chooser string
					if mode == "chosen" {
						postObjectChannelFact(t, p, "selected-retained-answer", map[string]any{"text": "retained answer"})
						waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, "selected-retained-answer"), "chooser")
						_, chooser = waitObjectRequestedResponse(t, channelSettlementStorageObservation(t, h), "selected-retained-answer")
						token = firstObjectDraftChoice(t, p, chooser)
						card = objectChannelChoiceCard(t, channelProcessStorageObservation(t, h), token)
						if other == card {
							other = firstCard
						}
						before = selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), card)
					}
					var beforeOther [7]string
					if other != "" {
						beforeOther = selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), other)
					}
					var runID string
					if err := func() error {
						observed, err := channelSettlementStorageObservation(t, h).ReadCardRunStorage(context.Background(), card)
						runID = observed
						return err
					}(); err != nil {
						t.Fatal(err)
					}
					fault, err := storetest.CaptureChannelInputBindingFaultForTest(context.Background(), selected, runID)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := fault.RestoreForTest(ctx); err != nil {
							t.Error(err)
						}
					})
					if err := fault.InstallForTest(context.Background()); err != nil {
						t.Fatal(err)
					}
					if mode == "skip" {
						for page := 0; page < 5 && token == ""; page++ {
							p.mu.Lock()
							controls, _ := p.message(receipt)["controls"].([]any)
							for _, control := range controls {
								row, _ := control.(map[string]any)
								if row["name"] == "Skip field" {
									token, _ = row["value"].(string)
								}
							}
							p.mu.Unlock()
							if token != "" {
								break
							}
							more := waitObjectMessageControl(t, p, receipt, "More choices")
							postObjectInputAction(t, p, fmt.Sprintf("selected-skip-page-%d", page), receipt, more)
							waitObjectEditAfterAction(t, p, receipt, more)
						}
						if token == "" {
							t.Fatal("optional prompt cannot reach Skip field")
						}
					}
					for attempt := 0; attempt < 2; attempt++ {
						switch mode {
						case "chosen":
							postObjectInputAction(t, p, "selected-choice", chooser, token)
							waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentAction, "selected-choice"), "unsupported")
							waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, "selected-retained-answer"), "unsupported")
						case "skip":
							postObjectInputAction(t, p, "selected-skip", receipt, token)
							waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentAction, "selected-skip"), "unsupported")
						default:
							fact := map[string]any{"text": "refused answer"}
							if mode == "quoted" {
								fact["reply"] = map[string]any{"id": receipt}
							}
							postObjectChannelFact(t, p, "selected-text", fact)
							postObjectChannelFact(t, p, "selected-text", fact)
							waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, "selected-text"), "unsupported")
						}
						if after := selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), card); before != after {
							t.Fatalf("unsupported %s changed card/draft: before=%v after=%v", mode, before, after)
						}
						var decisions int
						{
							var err error
							decisions, err = channelAnchorStorageObservation(t, h).CountRunNamedEventStorage(context.Background(), runID, "mailbox.card_decided")
							if err != nil || decisions != 0 {
								t.Fatalf("unsupported input emitted decisions: %d %v", decisions, err)
							}
						}
						if other != "" {
							if state := selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), other); state != beforeOther {
								t.Fatalf("chooser refusal consumed unrelated draft: %v", state)
							}
						}
					}
				})
			}
		}
	}
}

func selectedChannelDraftState(t *testing.T, reader storetest.ChannelSettlementObservation, card string) [7]string {
	t.Helper()
	var state [7]string
	if err := func() error {
		observed, err := reader.ReadChannelDraftCardPhysicalStorage(context.Background(), card)
		state[0] = observed[0]
		state[1] = observed[1]
		state[2] = observed[2]
		state[3] = observed[3]
		state[4] = observed[4]
		state[5] = observed[5]
		state[6] = observed[6]
		return err
	}(); err != nil {
		t.Fatal(err)
	}
	return state
}
