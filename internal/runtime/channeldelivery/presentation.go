package channeldelivery

import "fmt"

const ChannelExcerptRunes = 3500

// PresentationText bounds the provider message while leaving Frozen.Input and
// its hash as the complete immutable source for authenticated View full pages.
func PresentationText(f Frozen) (string, bool, error) {
	if err := f.Validate(); err != nil {
		return "", false, err
	}
	runes := []rune(f.FullText)
	if len(runes) <= ChannelExcerptRunes {
		return f.FullText, false, nil
	}
	const marker = "\n...\nOpen View full for the complete text.\n...\n"
	room := ChannelExcerptRunes - len([]rune(marker))
	if room < 2 {
		return "", false, fmt.Errorf("channel excerpt limit is too small")
	}
	head := room * 2 / 3
	return string(runes[:head]) + marker + string(runes[len(runes)-(room-head):]), true, nil
}
