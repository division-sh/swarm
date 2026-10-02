package packs

import "fmt"

// PresentationBounds is the selected compiled contract pinned with each plan
// and immutable render, never inferred from provider names at dispatch.
type PresentationBounds struct {
	Actions    int `json:"actions"`
	TextRunes  int `json:"text_runes"`
	LabelRunes int `json:"label_runes"`
}

func (b PresentationBounds) Validate() error {
	if b.Actions < 2 || b.TextRunes <= len("Page 1000/1000\n") || b.LabelRunes < 1 {
		return fmt.Errorf("selected channel bounds cannot express delivery navigation")
	}
	return nil
}

func (b PresentationBounds) Label(text string) string {
	runes := []rune(text)
	if len(runes) <= b.LabelRunes {
		return text
	}
	if b.LabelRunes <= 3 {
		return string(runes[:b.LabelRunes])
	}
	// Retain the identifying suffix of chooser/recovery labels with their title.
	tail := (b.LabelRunes - 3) / 3
	head := b.LabelRunes - 3 - tail
	return string(runes[:head]) + "..." + string(runes[len(runes)-tail:])
}
