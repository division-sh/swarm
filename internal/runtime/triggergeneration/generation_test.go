package triggergeneration

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCatalogGenerationIsOpaqueCanonicalAndRoundTrips(t *testing.T) {
	generation := FromCanonicalBytes([]byte(`{"catalog":"v1"}`))
	raw, err := json.Marshal(generation)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var recovered Generation
	if err := json.Unmarshal(raw, &recovered); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if !recovered.Equal(generation) {
		t.Fatalf("recovered generation = %q, want %q", recovered.Diagnostic(), generation.Diagnostic())
	}
	for _, malformed := range []string{
		"",
		" " + generation.Diagnostic(),
		strings.ToUpper(generation.Diagnostic()),
		"sha256:" + generation.Diagnostic(),
	} {
		if _, err := Parse(malformed); err == nil {
			t.Fatalf("malformed generation %q was accepted", malformed)
		}
	}
	if _, err := json.Marshal(Generation{}); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("zero generation marshal error = %v", err)
	}
}
