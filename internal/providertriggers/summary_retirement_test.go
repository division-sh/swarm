package providertriggers

import (
	"strings"
	"testing"
)

func TestProviderManifestRetiresSummarySelectorBeforeValueInterpretation(t *testing.T) {
	for _, declaration := range []string{
		"author_summary_field: null",
		"author_summary_field: ''",
		"author_summary_field: false",
		"author_summary_field: {}",
		"author_summary_field: []",
		"author_summary_field: text",
		"<<: &legacy {author_summary_field: null}",
		"event: &event inbound.telegram.text_message\n    author_summary_field: *event",
		"author_summary_field: null\n    author_summary_field: text",
	} {
		t.Run(declaration, func(t *testing.T) {
			raw := []byte("provider: telegram\nnormalized_events:\n  - " + declaration + "\n")
			for _, decode := range []func([]byte) (Manifest, error){ParseManifest, parseManifestStrict} {
				_, err := decode(raw)
				if err == nil || !strings.Contains(err.Error(), "author_summary_field") || (!strings.Contains(err.Error(), "is not supported") && !strings.Contains(err.Error(), "duplicate effective YAML key")) {
					t.Fatalf("selector presence admitted: %v", err)
				}
			}
		})
	}
}

func TestProviderManifestSummarySpellingInsidePayloadIsNotASelector(t *testing.T) {
	manifest, err := ParseManifest([]byte("provider: telegram\nnormalized_events:\n  - event: inbound.telegram.text_message\n    fields:\n      author_summary_field:\n        from: message.text\n        schema: {type: string}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.NormalizedEvents[0].Fields["author_summary_field"].From != "message.text" {
		t.Fatal("payload projection was consumed as a presentation selector")
	}
}
