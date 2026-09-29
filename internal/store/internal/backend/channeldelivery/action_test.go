package channeldelivery

import (
	"fmt"
	"github.com/division-sh/swarm/internal/packs"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

func TestAllRenderFamiliesConsumeSelectedPresentationBounds(t *testing.T) {
	audience := render.Audience{PrincipalID: uuid.NewString(), InterfaceKey: "test-channel", DeliveryEpoch: 1,
		ExternalAccountRef: "account", ConversationRef: "chat", ConversationScope: operatorchannel.ConversationScopeDirect}
	for _, bounds := range []packs.PresentationBounds{
		{Actions: 8, TextRunes: 4096, LabelRunes: 64},
		{Actions: 2, TextRunes: 512, LabelRunes: 24},
	} {
		for _, family := range []string{"recovery", "chooser", "notice", "summary", "response"} {
			t.Run(fmt.Sprintf("%s/%d", family, bounds.Actions), func(t *testing.T) {
				var frozen render.Frozen
				var err error
				wantChoices := 0
				switch family {
				case "chooser":
					choices := make([]render.DraftChoice, 16)
					for index := range choices {
						choices[index] = render.DraftChoice{DraftID: uuid.NewString(), CardID: uuid.NewString(),
							Label: strings.Repeat("long title ", 20) + fmt.Sprint(index)}
					}
					wantChoices = len(choices)
					frozen, err = render.FreezeDraftChooser(uuid.NewString(), uuid.NewString(), choices, audience)
				case "recovery":
					choices := make([]render.RecoveryChoice, 16)
					for index := range choices {
						choices[index] = render.RecoveryChoice{DeliveryID: uuid.NewString(), Label: "Resend " + strings.Repeat("x", 60) + fmt.Sprint(index)}
					}
					wantChoices = len(choices)
					frozen, err = render.FreezeRecoveryInbox(uuid.NewString(), strings.Repeat("x", 5000), choices, audience)
				case "notice":
					frozen, err = render.FreezeNotice(render.Notice{ID: uuid.NewString(), Type: "notify_human", Priority: "normal", Summary: strings.Repeat("x", 5000)}, audience)
				case "summary":
					frozen, err = render.FreezeSummary(uuid.NewString(), 9, audience)
				case "response":
					frozen, err = render.FreezeResponse(uuid.NewString(), strings.Repeat("x", 5000), audience)
				}
				if err != nil {
					t.Fatal(err)
				}
				frozen, err = render.WithPresentation(frozen, bounds, 0)
				if err != nil {
					t.Fatal(err)
				}
				pages, err := render.ActionPageCount(frozen)
				if err != nil {
					t.Fatal(err)
				}
				seen := map[string]bool{}
				viewFull := false
				for page := 0; page < pages; page++ {
					current, err := render.WithPresentation(frozen, bounds, page)
					if err != nil {
						t.Fatal(err)
					}
					current, err = render.Decode(current.Input, current.Hash)
					if err != nil {
						t.Fatal(err)
					}
					text, _, err := render.PresentationText(current)
					if err != nil || len([]rune(text)) > bounds.TextRunes {
						t.Fatalf("text bounds: %v", err)
					}
					actions, err := actionsForFrozen(current)
					if err != nil || len(actions) > bounds.Actions {
						t.Fatalf("actions=%+v err=%v", actions, err)
					}
					for _, action := range actions {
						if len([]rune(action.Label)) > bounds.LabelRunes {
							t.Fatalf("unbounded label: %q", action.Label)
						}
						if action.Kind == "view_full" {
							viewFull = true
						}
						if action.Kind == "select_draft" {
							seen[action.DraftID] = true
						}
						if action.Kind == "resend" {
							seen[action.RecoveryDeliveryID] = true
						}
					}
				}
				if len(seen) != wantChoices {
					t.Fatalf("reachable=%d want=%d", len(seen), wantChoices)
				}
				if len([]rune(frozen.FullText)) > bounds.TextRunes && !viewFull {
					t.Fatal("View full is unreachable")
				}
			})
		}
	}
}

func TestRecoveryReservesNavigationAndFullTextControls(t *testing.T) {
	audience := render.Audience{PrincipalID: uuid.NewString(), InterfaceKey: "test-channel", DeliveryEpoch: 1,
		ExternalAccountRef: "account", ConversationRef: "chat", ConversationScope: operatorchannel.ConversationScopeDirect}
	choices := make([]render.RecoveryChoice, 8)
	for index := range choices {
		choices[index] = render.RecoveryChoice{DeliveryID: uuid.NewString(), Label: fmt.Sprintf("Resend %d", index)}
	}
	frozen, err := render.FreezeRecoveryInbox(uuid.NewString(), strings.Repeat("x", 5000), choices, audience)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err = render.WithPresentation(frozen, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64}, 0)
	if err != nil {
		t.Fatal(err)
	}
	actions, err := actionsForFrozen(frozen)
	if err != nil {
		t.Fatal(err)
	}
	if len(actions) != 8 || actions[0].Kind != "view_full" || actions[7].Kind != "more_controls" {
		t.Fatalf("recovery did not reserve its navigation and full-text controls: %+v", actions)
	}
}
