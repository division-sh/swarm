package channeldelivery

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
)

func TestVisibleControlWordsStayDeliverableAcrossRenderFamilies(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "channel", DeliveryEpoch: 1,
		ExternalAccountRef: "human", ConversationRef: "room", ConversationScope: operatorchannel.ConversationScopeDirect}
	for name, captions := range map[string][]string{
		"case_fold":              {"Accept", "accept"},
		"unicode_fold":           {"Skip", "\u017fkip"},
		"generated":              {"View full", "More choices", "Cancel input", "Skip field"},
		"whitespace":             {" Accept\nnow\t", "Accept now", "\u202eAccept\u202c"},
		"empty_after_projection": {"\u202e\u202c", "Accepted"},
		"truncation":             {"Long caption left with identical suffix", "Long caption right with identical suffix"},
	} {
		for _, family := range []string{"card", "card_prompt", "prompt", "chooser", "recovery"} {
			for _, labelBound := range []int{1, 8, 24, 64} {
				t.Run(fmt.Sprintf("%s/%s/label_%d", name, family, labelBound), func(t *testing.T) {
					frozen := freezeControlWordsFixture(t, family, captions, audience)
					originalText := frozen.FullText
					page, err := WithPresentation(frozen, packs.PresentationBounds{Actions: 3, TextRunes: 256, LabelRunes: labelBound}, 0)
					if err != nil {
						t.Fatal(err)
					}
					count, err := ActionPageCount(page)
					if err != nil {
						t.Fatal(err)
					}
					for index := 0; index < count; index++ {
						page, err = WithPresentation(page, page.Bounds, index)
						if err != nil {
							t.Fatal(err)
						}
						controls, err := ControlsForRender(page)
						if err != nil {
							t.Fatal(err)
						}
						decoded, err := Decode(page.Input, page.Hash)
						if err != nil {
							t.Fatal(err)
						}
						repeated, err := ControlsForRender(decoded)
						if err != nil || !reflect.DeepEqual(controls, repeated) || decoded.FullText != originalText ||
							!reflect.DeepEqual(decoded.Choices, frozen.Choices) || !reflect.DeepEqual(decoded.DraftChoices, frozen.DraftChoices) ||
							!reflect.DeepEqual(decoded.RecoveryChoices, frozen.RecoveryChoices) {
							t.Fatalf("projection changed authored evidence or repeat selectors: %v", err)
						}
						for i := range controls {
							controls[i].Token = uuid.NewString()
							for j := 0; j < i; j++ {
								if MatchesTextControl(controls[j].Label, controls[i].Label) {
									t.Fatalf("ambiguous selectors: %+v", controls)
								}
							}
						}
						text, err := TextReplyPresentation(page, controls)
						if err != nil || len([]rune(text)) > page.Bounds.TextRunes {
							t.Fatalf("valid captions blocked delivery: %+v: %v", controls, err)
						}
						for _, control := range controls {
							if !strings.Contains(text, "\nAction: "+control.Label) || strings.Contains(text, control.Token) ||
								!MatchesTextControl(control.Label, " "+strings.ToUpper(control.Label)+"\n") {
								t.Fatal("visible selector lost parity or exposed private token")
							}
						}
					}
				})
			}
		}
	}
}

func freezeControlWordsFixture(t *testing.T, family string, captions []string, audience Audience) Frozen {
	t.Helper()
	var frozen Frozen
	var err error
	switch family {
	case "chooser":
		choices := make([]DraftChoice, len(captions))
		for i, label := range captions {
			choices[i] = DraftChoice{DraftID: uuid.NewString(), CardID: uuid.NewString(), Label: label}
		}
		frozen, err = FreezeDraftChooser(uuid.NewString(), uuid.NewString(), choices, audience)
	case "recovery":
		choices := make([]RecoveryChoice, len(captions))
		for i, label := range captions {
			choices[i] = RecoveryChoice{DeliveryID: uuid.NewString(), Label: label}
		}
		frozen, err = FreezeRecoveryInbox(uuid.NewString(), strings.Repeat("retained history\n", 30), choices, audience)
	case "card", "card_prompt", "prompt":
		card := controlWordsCard(t, captions)
		prompt := DraftPrompt{DraftID: uuid.NewString(), Verdict: "choice00", ExpiresAt: time.Now().Add(time.Hour)}
		if family == "prompt" {
			frozen, err = FreezeInputCardPrompt(uuid.NewString(), card, prompt, uuid.NewString(), audience)
		} else if family == "card_prompt" {
			frozen, err = FreezeCard(card, 1, "", audience, prompt)
		} else {
			frozen, err = FreezeCard(card, 1, "", audience, DraftPrompt{})
		}
	default:
		t.Fatalf("unknown control fixture %q", family)
	}
	if err != nil {
		t.Fatal(err)
	}
	return frozen
}

func controlWordsCard(t *testing.T, captions []string) decisioncard.Card {
	t.Helper()
	entity := uuid.NewString()
	source, err := events.NewRootRoutingSource(entity)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		Route: flowidentity.RouteForInstancePath("root"), FlowID: ".", EntityID: entity,
		Stage: "review", StageActivationID: uuid.NewString(), Source: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	outcomes := make(map[string]runtimecontracts.WorkflowGateOutcomePlan, len(captions))
	for i, label := range captions {
		verdict := fmt.Sprintf("choice%02d", i)
		outcomes[verdict] = runtimecontracts.WorkflowGateOutcomePlan{Verdict: verdict, Label: label,
			Input: map[string]runtimecontracts.WorkflowGateInputField{"reason": {Type: "text"}}, InputOrder: []string{"reason"}}
	}
	snapshot, err := decisioncard.FreezeSnapshot("review", "Review", map[string]any{"detail": strings.Repeat("source\n", 100)}, outcomes)
	if err != nil {
		t.Fatal(err)
	}
	card, err := decisioncard.New(decisioncard.Card{CardID: uuid.NewString(), RunID: uuid.NewString(), Anchor: anchor,
		Snapshot: snapshot, ExecutionMode: "live", BundleHash: "sha256:test", WorkflowVersion: "1", CreatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return card
}

func TestSummaryGuidanceUsesNeutralInboxBaseline(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "channel", DeliveryEpoch: 1,
		ExternalAccountRef: "human", ConversationRef: "room", ConversationScope: operatorchannel.ConversationScopeDirect}
	frozen, err := FreezeSummary(uuid.NewString(), 2, audience)
	if err != nil || strings.Contains(frozen.FullText, "chat menu") || !strings.Contains(frozen.FullText, "/inbox") ||
		!strings.Contains(frozen.FullText, "Open inbox") {
		t.Fatalf("neutral guidance requires an unavailable native menu: %q: %v", frozen.FullText, err)
	}
}

func TestControlWordsPageWhenPositionExceedsLabelBound(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "channel", DeliveryEpoch: 1,
		ExternalAccountRef: "human", ConversationRef: "room", ConversationScope: operatorchannel.ConversationScopeDirect}
	choices := make([]DraftChoice, 29)
	for i := range choices {
		choices[i] = DraftChoice{DraftID: uuid.NewString(), CardID: uuid.NewString(), Label: "Same"}
	}
	frozen, err := FreezeDraftChooser(uuid.NewString(), uuid.NewString(), choices, audience)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err = WithPresentation(frozen, packs.PresentationBounds{Actions: 16, TextRunes: 512, LabelRunes: 1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	count, err := ActionPageCount(frozen)
	if err != nil || count < 4 {
		t.Fatalf("one-rune selectors cannot represent every admitted page: count=%d: %v", count, err)
	}
	seen := map[string]int{}
	for index := 0; index < count; index++ {
		frozen, err = WithPresentation(frozen, frozen.Bounds, index)
		if err != nil {
			t.Fatal(err)
		}
		controls, err := ControlsForRender(frozen)
		if err != nil || len(controls) > 9 {
			t.Fatalf("unrepresentable decimal page: %+v: %v", controls, err)
		}
		for _, control := range controls {
			if len([]rune(control.Label)) != 1 {
				t.Fatal("selector exceeded exact label bound")
			}
			if control.Kind == "select_draft" {
				seen[control.DraftID]++
			}
		}
	}
	for _, choice := range choices {
		if seen[choice.DraftID] != 1 {
			t.Fatal("disambiguation or pagination lost or duplicated a semantic control")
		}
	}
}
