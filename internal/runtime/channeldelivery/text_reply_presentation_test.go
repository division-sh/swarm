package channeldelivery

import (
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/google/uuid"
)

func TestControlPagesCoverEveryChoiceWithinTextAndCountBounds(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "channel", DeliveryEpoch: 1,
		ExternalAccountRef: "human", ConversationRef: "room", ConversationScope: operatorchannel.ConversationScopeShared}
	choices := make([]DraftChoice, 29)
	for i := range choices {
		choices[i] = DraftChoice{DraftID: uuid.NewString(), CardID: uuid.NewString(), Label: fmt.Sprintf("Item %02d", i)}
	}
	frozen, err := FreezeDraftChooser(uuid.NewString(), uuid.NewString(), choices, audience)
	if err != nil {
		t.Fatal(err)
	}
	for _, capacity := range []int{2, 3, 16} {
		for _, textBound := range []int{128, 256, 512} {
			t.Run(fmt.Sprintf("controls_%d/text_%d", capacity, textBound), func(t *testing.T) {
				page, err := WithPresentation(frozen, packs.PresentationBounds{Actions: capacity, TextRunes: textBound, LabelRunes: 64}, 0)
				if err != nil {
					t.Fatal(err)
				}
				count, err := ActionPageCount(page)
				if err != nil || count < 2 {
					t.Fatalf("paged source count=%d: %v", count, err)
				}
				seen := map[string]int{}
				for i := 0; i < count; i++ {
					page, err = WithPresentation(page, page.Bounds, i)
					if err != nil {
						t.Fatal(err)
					}
					controls, err := ControlsForRender(page)
					if err != nil || len(controls) > capacity {
						t.Fatalf("page controls=%v: %v", controls, err)
					}
					for j := range controls {
						controls[j].Token = uuid.NewString()
						if controls[j].Kind == "select_draft" {
							seen[controls[j].DraftID]++
						}
					}
					text, err := TextReplyPresentation(page, controls)
					if err != nil || len([]rune(text)) > textBound {
						t.Fatalf("control text lost its bound: %q: %v", text, err)
					}
					for _, control := range controls {
						if !strings.Contains(text, "Action: "+control.Label) || strings.Contains(text, control.Token) {
							t.Fatal("control word lost or private token exposed")
						}
					}
					decoded, err := Decode(page.Input, page.Hash)
					if err != nil || decoded.ActionPage.Index != i {
						t.Fatalf("immutable page did not round-trip: %v", err)
					}
				}
				for _, choice := range choices {
					if seen[choice.DraftID] != 1 {
						t.Fatal("semantic choice omitted or repeated across one page cycle")
					}
				}
			})
		}
	}
}

func TestFullTextPagesRetainEveryRuneAfterReferenceAndControlBudget(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "channel", DeliveryEpoch: 1,
		ExternalAccountRef: "human", ConversationRef: "room", ConversationScope: operatorchannel.ConversationScopeDirect}
	fullText := strings.Repeat("abcdef\u00e9\U0001f600\n", 100)
	source, err := FreezeResponse(uuid.NewString(), fullText, audience)
	if err != nil {
		t.Fatal(err)
	}
	source, err = WithPresentation(source, packs.PresentationBounds{Actions: 16, TextRunes: 128, LabelRunes: 64}, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, count, err := FullTextPage(fullText, 0, source.Bounds)
	if err != nil {
		t.Fatal(err)
	}
	var reconstructed strings.Builder
	for index := 0; index < count; index++ {
		page, err := FreezeResponsePage(uuid.NewString(), source, uuid.NewString(), index, audience)
		if err != nil {
			t.Fatal(err)
		}
		page, err = WithPresentation(page, source.Bounds, 0)
		if err != nil {
			t.Fatal(err)
		}
		controls, err := ControlsForRender(page)
		if err != nil {
			t.Fatal(err)
		}
		for i := range controls {
			controls[i].Token = uuid.NewString()
		}
		text, err := TextReplyPresentation(page, controls)
		if err != nil || len([]rune(text)) > source.Bounds.TextRunes || strings.Contains(text, "\n...\n") {
			t.Fatalf("immutable page lost source bytes: %q: %v", text, err)
		}
		_, content, _ := strings.Cut(page.FullText, "\n")
		reconstructed.WriteString(content)
	}
	if reconstructed.String() != fullText {
		t.Fatal("full-text continuation omitted or duplicated source runes")
	}
}

func TestTextReplyPresentationPreservesControlsAndNeverExposesTokens(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "channel", DeliveryEpoch: 1,
		ExternalAccountRef: "human", ConversationRef: "room", ConversationScope: operatorchannel.ConversationScopeShared}
	frozen, err := FreezeResponse(uuid.NewString(), strings.Repeat("source evidence\n", 100), audience)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err = WithPresentation(frozen, packs.PresentationBounds{Actions: 16, TextRunes: 180, LabelRunes: 64}, 0)
	if err != nil {
		t.Fatal(err)
	}
	actions := []Action{{Token: uuid.NewString(), Label: "Retire"}, {Token: uuid.NewString(), Label: "View full"}}
	text, err := TextReplyPresentation(frozen, actions)
	if err != nil {
		t.Fatal(err)
	}
	if len([]rune(text)) > frozen.Bounds.TextRunes || !strings.Contains(text, "Reference: "+frozen.SourceID) ||
		!strings.Contains(text, "Action: Retire") || !strings.Contains(text, "Action: View full") || !strings.Contains(text, "\n...\n") {
		t.Fatalf("bounded text = %q", text)
	}
	for _, action := range actions {
		if strings.Contains(text, action.Token) {
			t.Fatal("private action token was exposed")
		}
	}
	for name, labels := range map[string][]string{
		"blank": {""}, "whitespace": {" Retire"}, "newline": {"Retire\nApprove"},
		"too long": {strings.Repeat("r", 65)}, "duplicate": {"Retire", "retire"},
		"unicode fold duplicate": {"Skip", "\u017fkip"}, "invalid utf8": {string([]byte{0xff})},
	} {
		t.Run(name, func(t *testing.T) {
			controls := make([]Action, len(labels))
			for i, label := range labels {
				controls[i] = Action{Token: uuid.NewString(), Label: label}
			}
			if _, err := TextReplyPresentation(frozen, controls); err == nil {
				t.Fatal("non-exact controls accepted")
			}
		})
	}
	for _, label := range []string{"Retire", " RETIRE\n", "retire"} {
		if !MatchesTextControl("Retire", label) {
			t.Fatalf("explicit control word %q not recognized", label)
		}
	}
	if MatchesTextControl("Retire", "please Retire") || MatchesTextControl("", " ") {
		t.Fatal("non-control text recognized")
	}
}
