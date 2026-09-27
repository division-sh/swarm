package channeldelivery

import (
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

func TestDraftChooserPagesKeepExactTextAndBoundedActions(t *testing.T) {
	audience := render.Audience{PrincipalID: uuid.NewString(), InterfaceKey: "test-channel", DeliveryEpoch: 1,
		ExternalAccountRef: "account", ConversationRef: "chat", ConversationScope: operatorchannel.ConversationScopeDirect}
	textID := uuid.NewString()
	choices := make([]render.DraftChoice, 16)
	for index := range choices {
		choices[index] = render.DraftChoice{DraftID: uuid.NewString(), CardID: uuid.NewString(),
			Label: fmt.Sprintf("Card %02d", index+1)}
	}
	for page, want := range []int{8, 8, 2} {
		publicationID := uuid.NewString()
		frozen, err := render.FreezeDraftChooser(publicationID, textID, choices, page, audience)
		if err != nil {
			t.Fatal(err)
		}
		if frozen.DraftChooser == nil || frozen.DraftChooser.TextPublicationID != textID ||
			frozen.DraftChooser.PageIndex != page || len(frozen.DraftChoices) != len(choices) ||
			strings.Contains(frozen.FullText, "private answer") {
			t.Fatalf("page %d lost immutable chooser identity: %#v", page, frozen)
		}
		actions, err := actionsForFrozen(frozen)
		if err != nil || len(actions) != want {
			t.Fatalf("page %d actions=%+v err=%v, want %d", page, actions, err, want)
		}
		for index, action := range actions {
			if action.Kind == "next_draft_page" {
				if page == 2 || index != len(actions)-1 {
					t.Fatalf("page %d unexpected continuation: %+v", page, actions)
				}
				continue
			}
			choice := choices[page*render.DraftChooserPageSize+index]
			if action.Kind != "select_draft" || action.DraftID != choice.DraftID ||
				action.CardID != choice.CardID || action.TextPublicationID != textID {
				t.Fatalf("page %d action %d lost exact choice: %+v", page, index, action)
			}
		}
	}
	if _, err := render.FreezeDraftChooser(uuid.NewString(), textID, choices, 3, audience); err == nil {
		t.Fatal("chooser accepted an out-of-range page")
	}
	choices[1] = choices[0]
	if _, err := render.FreezeDraftChooser(uuid.NewString(), textID, choices, 0, audience); err == nil {
		t.Fatal("chooser accepted a duplicate draft")
	}
}
