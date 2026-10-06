package channeldelivery

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/google/uuid"
)

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
