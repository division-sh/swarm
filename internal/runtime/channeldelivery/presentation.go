package channeldelivery

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// PresentationText bounds the provider message while leaving Frozen.Input and
// its hash as the complete immutable source for authenticated View full pages.
func PresentationText(f Frozen) (string, bool, error) {
	if err := f.Validate(); err != nil {
		return "", false, err
	}
	if err := f.Bounds.Validate(); err != nil {
		return "", false, err
	}
	controls, err := ControlsForRender(f)
	if err != nil {
		return "", false, err
	}
	return presentationExcerpt(f.FullText, f.Bounds.TextRunes-textReplySuffixRunes(f.SourceID, controls))
}

func TextReplyPresentation(f Frozen, actions []Action) (string, error) {
	if err := f.Validate(); err != nil {
		return "", err
	}
	if err := f.Bounds.Validate(); err != nil {
		return "", err
	}
	if len(actions) > f.Bounds.Actions {
		return "", fmt.Errorf("text/reply controls exceed admitted page capacity")
	}
	lines := []string{"", "Reference: " + f.SourceID}
	words := make([]string, 0, len(actions))
	for _, action := range actions {
		if action.Label == "" || strings.TrimSpace(action.Label) != action.Label || !utf8.ValidString(action.Label) ||
			action.Token == "" || strings.ContainsAny(action.Label, "\r\n") || len([]rune(action.Label)) > f.Bounds.LabelRunes {
			return "", fmt.Errorf("text/reply control cannot be represented exactly")
		}
		for _, word := range words {
			if MatchesTextControl(word, action.Label) {
				return "", fmt.Errorf("text/reply controls have an ambiguous action word")
			}
		}
		words = append(words, action.Label)
		lines = append(lines, "Action: "+action.Label)
	}
	suffix := strings.Join(lines, "\n")
	room := f.Bounds.TextRunes - len([]rune(suffix))
	if room < len("\n...\n")+2 {
		return "", fmt.Errorf("selected text bound cannot retain exact reply reference and action words")
	}
	text, _, err := presentationExcerpt(f.FullText, room)
	if err != nil {
		return "", err
	}
	return text + suffix, nil
}

func MatchesTextControl(label, text string) bool {
	return label != "" && strings.EqualFold(label, strings.TrimSpace(text))
}

func presentationExcerpt(fullText string, limit int) (string, bool, error) {
	runes := []rune(fullText)
	if len(runes) <= limit {
		return fullText, false, nil
	}
	const marker = "\n...\n"
	room := limit - len([]rune(marker))
	if room < 2 {
		return "", false, fmt.Errorf("channel excerpt limit is too small")
	}
	head := room * 2 / 3
	return string(runes[:head]) + marker + string(runes[len(runes)-(room-head):]), true, nil
}
