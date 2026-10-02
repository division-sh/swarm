package channeldelivery

import "fmt"

// PresentationText bounds the provider message while leaving Frozen.Input and
// its hash as the complete immutable source for authenticated View full pages.
func PresentationText(f Frozen) (string, bool, error) {
	if err := f.Validate(); err != nil {
		return "", false, err
	}
	if err := f.Bounds.Validate(); err != nil {
		return "", false, err
	}
	runes := []rune(f.FullText)
	if len(runes) <= f.Bounds.TextRunes {
		return f.FullText, false, nil
	}
	const marker = "\n...\n"
	room := f.Bounds.TextRunes - len([]rune(marker))
	if room < 2 {
		return "", false, fmt.Errorf("channel excerpt limit is too small")
	}
	head := room * 2 / 3
	return string(runes[:head]) + marker + string(runes[len(runes)-(room-head):]), true, nil
}
