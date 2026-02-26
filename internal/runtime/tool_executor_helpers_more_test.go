package runtime

import (
	"net/http"
	"strings"
	"testing"
)

func TestToolExecutor_HelperFunctions_MoreBranches(t *testing.T) {
	// safeTelemetryText: json.Marshal failure path.
	txt := safeTelemetryText(map[string]any{
		"token": "super-secret",
		"fn":    func() {},
	})
	if !strings.Contains(txt, "[REDACTED]") {
		t.Fatalf("expected redaction in telemetry, got %q", txt)
	}

	// truncateTelemetry: no-op and truncation.
	if got := truncateTelemetry("abc", 0); got != "abc" {
		t.Fatalf("expected no-op when max<=0, got %q", got)
	}
	if got := truncateTelemetry("abc", 10); got != "abc" {
		t.Fatalf("expected no truncation, got %q", got)
	}
	if got := truncateTelemetry("abcdef", 3); got != "abc..." {
		t.Fatalf("expected truncation, got %q", got)
	}

	// asString: nil and non-string.
	if asString(nil) != "" {
		t.Fatalf("expected empty for nil")
	}
	if asString(123) != "123" {
		t.Fatalf("expected fmt fallback, got %q", asString(123))
	}

	// defaultExternalMethod branches.
	if defaultExternalMethod("domain_availability_check") != http.MethodGet {
		t.Fatalf("expected GET")
	}
	if defaultExternalMethod("domain_purchase") != http.MethodPost {
		t.Fatalf("expected POST")
	}

	// applyExternalHeaders trimming and skipping empties.
	req, _ := http.NewRequest(http.MethodGet, "http://x", nil)
	applyExternalHeaders(req, map[string]any{
		" X ":  " y ",
		"":     "z",
		"noop": "",
	})
	if req.Header.Get("X") != "y" {
		t.Fatalf("expected header X=y, got %q", req.Header.Get("X"))
	}
	if req.Header.Get("noop") != "" {
		t.Fatalf("expected noop to be skipped")
	}

	// applyExternalCredentialHeaders: default auth header and bearer prefixing.
	req2, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	applyExternalCredentialHeaders(req2, map[string]any{
		"api_key": "k1",
		"headers": map[string]any{"X-Extra": "v"},
	}, "dns_configure")
	if req2.Header.Get("X-Extra") != "v" {
		t.Fatalf("expected X-Extra set")
	}
	if got := req2.Header.Get("Authorization"); got != "Bearer k1" {
		t.Fatalf("expected bearer auth, got %q", got)
	}

	// Custom auth header should avoid bearer prefixing.
	req3, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	applyExternalCredentialHeaders(req3, map[string]any{
		"auth_header": "X-Api-Key",
		"token":       "t1",
	}, "whatsapp_business_api")
	if got := req3.Header.Get("X-Api-Key"); got != "t1" {
		t.Fatalf("expected custom auth header, got %q", got)
	}

	// Already-bearer token should not be double-prefixed.
	req4, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	applyExternalCredentialHeaders(req4, map[string]any{
		"token": "Bearer z",
	}, "instagram_api")
	if got := req4.Header.Get("Authorization"); got != "Bearer z" {
		t.Fatalf("expected preserved bearer token, got %q", got)
	}

	// parseExternalResponseBody: empty, json, and non-json.
	if got := parseExternalResponseBody(nil).(map[string]any); len(got) != 0 {
		t.Fatalf("expected empty map, got %#v", got)
	}
	if got := parseExternalResponseBody([]byte(`{"ok":true}`)).(map[string]any)["ok"]; got != true {
		t.Fatalf("expected parsed json, got %#v", got)
	}
	if got := parseExternalResponseBody([]byte(" hi ")).(string); got != "hi" {
		t.Fatalf("expected trimmed string, got %q", got)
	}
}

