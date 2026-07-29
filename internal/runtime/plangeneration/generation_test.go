package plangeneration

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPlanGenerationCodecRejectsMalformedAndAbsentAuthority(t *testing.T) {
	generation, err := FromCanonicalValue(map[string]any{"operation": "deliver"})
	if err != nil {
		t.Fatalf("FromCanonicalValue: %v", err)
	}
	raw, err := json.Marshal(generation)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var recovered Generation
	if err := json.Unmarshal(raw, &recovered); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if !recovered.Equal(generation) {
		t.Fatalf("round-trip generation = %q, want %q", recovered.Diagnostic(), generation.Diagnostic())
	}

	for _, raw := range []string{
		`""`,
		`" sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`,
		`"sha256:not-a-digest"`,
		`"sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"`,
		`"md5:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`,
	} {
		var decoded Generation
		if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
			t.Fatalf("malformed generation %s was accepted", raw)
		}
	}
	if _, err := json.Marshal(Generation{}); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("zero generation marshal error = %v", err)
	}
}
