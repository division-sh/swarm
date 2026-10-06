package channeldelivery

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/division-sh/swarm/internal/packs"
)

func ControlsForRender(f Frozen) ([]Action, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	pages, err := projectedControlPages(f)
	if err != nil {
		return nil, err
	}
	index := 0
	if f.ActionPage != nil {
		index = f.ActionPage.Index % len(pages)
	}
	return append([]Action(nil), pages[index]...), nil
}

func semanticControls(f Frozen) []Action {
	controls := make([]Action, 0, len(f.Choices)+len(f.DraftChoices)+3)
	for _, choice := range f.Choices {
		controls = append(controls, Action{Kind: "verdict", Verdict: choice.Verdict, Label: choice.Label})
	}
	if f.Prompt != nil {
		control := Action{Kind: "cancel_input", DraftID: f.Prompt.DraftID, Label: "Cancel input"}
		if f.InputCard != nil {
			control.CardID, control.ParentReceiptOperationID = f.InputCard.CardID, f.InputCard.ParentReceiptOperationID
		}
		controls = append(controls, control)
		if f.Prompt.Optional {
			control.Kind, control.Label = "skip_input", "Skip field"
			controls = append(controls, control)
		}
	}
	if f.DraftChooser != nil {
		for _, choice := range f.DraftChoices {
			controls = append(controls, Action{Kind: "select_draft", DraftID: choice.DraftID, CardID: choice.CardID,
				TextPublicationID: f.DraftChooser.TextPublicationID, Label: choice.Label})
		}
	}
	if f.Recovery != nil {
		for _, choice := range f.RecoveryChoices {
			controls = append(controls, Action{Kind: "resend", RecoveryDeliveryID: choice.DeliveryID, Label: choice.Label})
		}
	}
	if f.SourceKind == "summary" {
		controls = append(controls, Action{Kind: "open_inbox", Label: "Open inbox"})
	}
	if f.SourceKind == "notice" && !f.NoticeAcknowledged {
		controls = append(controls, Action{Kind: "acknowledge_notice", Label: "Acknowledge"})
	}
	if f.Page != nil && f.Page.Index+1 < f.Page.Count {
		controls = append(controls, Action{Kind: "next_page", Label: "Next page"})
	}
	return controls
}

func projectedControlPages(f Frozen) ([][]Action, error) {
	if err := f.Bounds.Validate(); err != nil {
		return nil, err
	}
	controls := semanticControls(f)
	pages, err := partitionControlPages(f, controls)
	if err != nil {
		return nil, err
	}
	for _, page := range pages {
		if len([]rune(f.FullText)) > f.Bounds.TextRunes-textReplySuffixRunes(f.SourceID, page) {
			if f.Page != nil {
				return nil, fmt.Errorf("immutable text page exceeds its exact presentation budget")
			}
			controls = append([]Action{{Kind: "view_full", Label: "View full"}}, controls...)
			return partitionControlPages(f, controls)
		}
	}
	return pages, nil
}

func partitionControlPages(f Frozen, controls []Action) ([][]Action, error) {
	const minimumExcerpt = len("\n...\n") + 2
	available := f.Bounds.TextRunes - minimumExcerpt
	project := func(page []Action) ([]Action, bool) {
		if len(page) > f.Bounds.Actions {
			return nil, false
		}
		words, err := visibleControlWords(page, f.Bounds)
		return words, err == nil && textReplySuffixRunes(f.SourceID, words) <= available
	}
	if page, fits := project(controls); fits {
		return [][]Action{page}, nil
	}
	if len(controls) == 0 || f.Bounds.Actions < 2 {
		return nil, fmt.Errorf("selected text/control bounds cannot retain exact reply controls")
	}
	more := Action{Kind: "more_controls", Label: "More choices"}
	pages := make([][]Action, 0)
	for len(controls) > 0 {
		page := make([]Action, 0, f.Bounds.Actions)
		for len(controls) > 0 {
			candidate := append(append([]Action(nil), page...), controls[0], more)
			if _, fits := project(candidate); !fits {
				break
			}
			page = append(page, controls[0])
			controls = controls[1:]
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("selected text/control bounds cannot page one exact control")
		}
		projected, fits := project(append(page, more))
		if !fits {
			return nil, fmt.Errorf("selected text/control bounds cannot retain projected control page")
		}
		pages = append(pages, projected)
	}
	return pages, nil
}

// The immutable page owns visible selectors, not the authored captions. Number
// every control on a colliding page so generated and authored words cannot alias.
func visibleControlWords(controls []Action, bounds packs.PresentationBounds) ([]Action, error) {
	projected := append([]Action(nil), controls...)
	collision := false
	for i := range projected {
		caption := strings.Map(func(r rune) rune {
			if unicode.Is(unicode.Cf, r) {
				return -1
			}
			if unicode.IsControl(r) {
				return ' '
			}
			return r
		}, controls[i].Label)
		caption = strings.Join(strings.Fields(caption), " ")
		projected[i].Label = strings.TrimSpace(bounds.Label(caption))
		collision = collision || projected[i].Label == ""
		for j := 0; j < i; j++ {
			collision = collision || MatchesTextControl(projected[j].Label, projected[i].Label)
		}
	}
	if !collision {
		return projected, nil
	}
	for i := range projected {
		number := strconv.Itoa(i + 1)
		if len(number) > bounds.LabelRunes {
			return nil, fmt.Errorf("selected label bound cannot represent this control page")
		}
		captionBounds := bounds
		captionBounds.LabelRunes -= len(number) + 1
		caption := ""
		if captionBounds.LabelRunes > 0 {
			caption = strings.TrimSpace(captionBounds.Label(projected[i].Label))
		}
		projected[i].Label = number
		if caption != "" {
			projected[i].Label += " " + caption
		}
	}
	return projected, nil
}

func textReplySuffixRunes(sourceID string, controls []Action) int {
	count := len([]rune("\nReference: " + sourceID))
	for _, control := range controls {
		count += len([]rune("\nAction: " + control.Label))
	}
	return count
}
