package store

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExtractSubscriptions(t *testing.T) {
	if got := extractSubscriptions(nil); got != nil {
		t.Fatalf("expected nil")
	}
	if got := extractSubscriptions([]byte("nope")); got != nil {
		t.Fatalf("expected nil for invalid json")
	}
	raw := []byte(`{"subscriptions":["a","b"]}`)
	got := extractSubscriptions(raw)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected subscriptions: %#v", got)
	}
}

func TestNormalizeJSONPayload_RedactsSensitiveText(t *testing.T) {
	// Invalid JSON: wraps raw with redacted string.
	out := normalizeJSONPayload([]byte("email me at x@example.com or call +1 (555) 123-4567"))
	if !json.Valid([]byte(out)) {
		t.Fatalf("expected valid json wrapper, got %q", out)
	}
	if strings.Contains(out, "x@example.com") || strings.Contains(out, "555") {
		t.Fatalf("expected email/phone redacted in wrapper: %q", out)
	}

	// Valid JSON: redacts name fields and free text.
	out = normalizeJSONPayload([]byte(`{"name":"Alice Smith","notes":"reach me at y@example.com","nested":{"full_name":"Bob Jones"}}`))
	if !json.Valid([]byte(out)) {
		t.Fatalf("expected valid json, got %q", out)
	}
	if strings.Contains(out, "Alice") || strings.Contains(out, "Bob") {
		t.Fatalf("expected names redacted, got %q", out)
	}
	if strings.Contains(out, "y@example.com") {
		t.Fatalf("expected email redacted, got %q", out)
	}

	// Payment references should be redacted.
	out = normalizeJSONPayload([]byte(`{"payment_ref":"pi_1234567890ABCDEF","notes":"charge ch_abcdef123456 done"}`))
	if strings.Contains(out, "pi_1234567890ABCDEF") || strings.Contains(out, "ch_abcdef123456") {
		t.Fatalf("expected payment refs redacted, got %q", out)
	}
	if !strings.Contains(out, "[PAYMENT_REF]") {
		t.Fatalf("expected [PAYMENT_REF] marker, got %q", out)
	}

	// ISO timestamps should be preserved in logs/traces (must not be redacted as phone).
	out = normalizeJSONPayload([]byte(`{"timestamp":"2026-02-21T02:47:05Z","notes":"at 2026-02-21T02:47:05Z"}`))
	if strings.Contains(out, "[PHONE]") {
		t.Fatalf("expected timestamp not redacted as phone, got %q", out)
	}
}
