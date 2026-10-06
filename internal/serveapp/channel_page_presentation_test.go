package serveapp

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Consume only the exact typed footer at the end, never a source-content marker.
func channelPageSourceText(text string, words []string) (string, error) {
	actions := ""
	for _, word := range words {
		actions += "\nAction: " + word
	}
	const referencePrefix = "\nReference: "
	footerBytes := len(referencePrefix) + len("00000000-0000-0000-0000-000000000000") + len(actions)
	if len(text) < footerBytes {
		return "", fmt.Errorf("page has no complete reference/control footer")
	}
	source, footer := text[:len(text)-footerBytes], text[len(text)-footerBytes:]
	if !strings.HasPrefix(footer, referencePrefix) || !strings.HasSuffix(footer, actions) {
		return "", fmt.Errorf("page reference/control footer contradicts its visible controls")
	}
	reference := footer[len(referencePrefix) : len(footer)-len(actions)]
	if uuid.Validate(reference) != nil || footer != referencePrefix+reference+actions {
		return "", fmt.Errorf("page footer has no exact source identity")
	}
	return source, nil
}

func TestChannelPageOraclePreservesFooterShapedSourceBytes(t *testing.T) {
	for _, words := range [][]string{nil, {"Next page"}, {"\u00e9 next"}} {
		original := "Page 1/2\nSource text\nReference: " + uuid.NewString() + "\nAction: Next page\nReference: source tail\n"
		footer := "\nReference: " + uuid.NewString()
		for _, word := range words {
			footer += "\nAction: " + word
		}
		body, err := channelPageSourceText(original+footer, words)
		if err != nil || body != original {
			t.Fatalf("oracle changed source bytes: %q: %v", body, err)
		}
		for _, malformed := range []string{original, original + strings.Replace(footer, "Reference:", "Reference?", 1), original + footer + "\n"} {
			if _, err := channelPageSourceText(malformed, words); err == nil {
				t.Fatal("oracle accepted malformed or missing physical footer")
			}
		}
	}
}
