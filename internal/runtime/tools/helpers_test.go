package tools

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestMailboxNormalization(t *testing.T) {
	if mt, err := NormalizeMailboxType("vertical.promotion_review"); err != nil || mt != "vertical_approval" {
		t.Fatalf("unexpected mailbox type normalization: %q %v", mt, err)
	}
	if mt, err := NormalizeMailboxType("vertical_approval"); err != nil || mt != "vertical_approval" {
		t.Fatalf("unexpected mailbox type passthrough: %q %v", mt, err)
	}
	if _, err := NormalizeMailboxType("bogus"); err == nil {
		t.Fatal("expected invalid mailbox type")
	}
	if mp, err := NormalizeMailboxPriority("urgent"); err != nil || mp != "high" {
		t.Fatalf("unexpected mailbox priority normalization: %q %v", mp, err)
	}
	if mp, err := NormalizeMailboxPriority("critical"); err != nil || mp != "critical" {
		t.Fatalf("unexpected mailbox priority passthrough: %q %v", mp, err)
	}
	if _, err := NormalizeMailboxPriority("bogus"); err == nil {
		t.Fatal("expected invalid mailbox priority")
	}
}

func TestExternalHelperFunctions(t *testing.T) {
	txt := SafeTelemetryText(map[string]any{
		"token": "super-secret",
		"fn":    func() {},
	})
	if !strings.Contains(txt, "[REDACTED]") {
		t.Fatalf("expected redaction in telemetry, got %q", txt)
	}
	largePayload := map[string]any{}
	for i := 0; i < 80; i++ {
		largePayload[fmt.Sprintf("k_%d", i)] = strings.Repeat("x", 90)
	}
	largeText := SafeTelemetryText(largePayload)
	if len(largeText) <= 400 {
		t.Fatalf("expected telemetry truncation budget > 400 chars, got len=%d", len(largeText))
	}
	if len(largeText) > maxToolTelemetryChars+3 {
		t.Fatalf("expected telemetry capped at %d (+ellipsis), got len=%d", maxToolTelemetryChars, len(largeText))
	}

	if got := TruncateTelemetry("abc", 0); got != "abc" {
		t.Fatalf("expected no-op when max<=0, got %q", got)
	}
	if got := TruncateTelemetry("abcdef", 3); got != "abc..." {
		t.Fatalf("expected truncation, got %q", got)
	}
	if AsString(nil) != "" || AsString(123) != "123" {
		t.Fatalf("unexpected AsString behavior")
	}

	if DefaultExternalMethod("domain_availability_check") != http.MethodGet {
		t.Fatalf("expected GET")
	}
	if DefaultExternalMethod("domain_purchase") != http.MethodPost {
		t.Fatalf("expected POST")
	}

	req, _ := http.NewRequest(http.MethodGet, "http://x", nil)
	ApplyExternalHeaders(req, map[string]any{
		" X ":  " y ",
		"":     "z",
		"noop": "",
	})
	if req.Header.Get("X") != "y" || req.Header.Get("noop") != "" {
		t.Fatalf("unexpected header state: %#v", req.Header)
	}

	req2, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	ApplyExternalCredentialHeaders(req2, map[string]any{
		"api_key": "k1",
		"headers": map[string]any{"X-Extra": "v"},
	}, "dns_configure")
	if req2.Header.Get("X-Extra") != "v" || req2.Header.Get("Authorization") != "Bearer k1" {
		t.Fatalf("unexpected credential headers: %#v", req2.Header)
	}

	req3, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	ApplyExternalCredentialHeaders(req3, map[string]any{
		"auth_header": "X-Api-Key",
		"token":       "t1",
	}, "whatsapp_business_api")
	if got := req3.Header.Get("X-Api-Key"); got != "t1" {
		t.Fatalf("expected custom auth header, got %q", got)
	}

	if got := ParseExternalResponseBody(nil).(map[string]any); len(got) != 0 {
		t.Fatalf("expected empty map, got %#v", got)
	}
	if got := ParseExternalResponseBody([]byte(`{"ok":true}`)).(map[string]any)["ok"]; got != true {
		t.Fatalf("expected parsed json, got %#v", got)
	}
	if got := ParseExternalResponseBody([]byte(" hi ")).(string); got != "hi" {
		t.Fatalf("expected trimmed string, got %q", got)
	}
}

func TestDefaultExternalCredentialEnv(t *testing.T) {
	t.Setenv("REGISTRAR_API_ENDPOINT", "https://reg.example")
	t.Setenv("REGISTRAR_API_KEY", "rk")
	t.Setenv("CLOUDFLARE_API_ENDPOINT", "")
	t.Setenv("CLOUDFLARE_API_TOKEN", "cfk")
	t.Setenv("WHATSAPP_API_ENDPOINT", "https://wa.example")
	t.Setenv("WHATSAPP_API_KEY", "wak")
	t.Setenv("INSTAGRAM_API_ENDPOINT", "https://ig.example")
	t.Setenv("INSTAGRAM_API_KEY", "igk")

	if got := DefaultExternalCredentialEnv("domain_purchase"); got["endpoint"] != "https://reg.example" || got["api_key"] != "rk" {
		t.Fatalf("domain creds mismatch: %#v", got)
	}
	if got := DefaultExternalCredentialEnv("dns_configure"); !strings.Contains(got["endpoint"], "cloudflare.com") || got["api_key"] != "cfk" {
		t.Fatalf("dns creds mismatch: %#v", got)
	}
	if got := DefaultExternalCredentialEnv("whatsapp_business_api"); got["endpoint"] != "https://wa.example" || got["api_key"] != "wak" {
		t.Fatalf("wa creds mismatch: %#v", got)
	}
	if got := DefaultExternalCredentialEnv("instagram_api"); got["endpoint"] != "https://ig.example" || got["api_key"] != "igk" {
		t.Fatalf("ig creds mismatch: %#v", got)
	}
	if got := DefaultExternalCredentialEnv("unknown"); len(got) != 0 {
		t.Fatalf("expected empty map")
	}
}

func TestRedactTelemetryValue(t *testing.T) {
	in := map[string]any{
		"token":    "secret-token-value",
		"password": "pw",
		"notes":    "payment confirmed ch_abcdef123456",
		"meta": map[string]any{
			"Authorization": "Bearer X",
			"count":         2,
			"items":         []any{"a", map[string]any{"api_key": "k"}},
		},
	}
	out := RedactTelemetryValue(in).(map[string]any)
	if out["token"] != "[REDACTED]" || out["password"] != "[REDACTED]" {
		t.Fatalf("expected sensitive keys redacted: %#v", out)
	}
	meta := out["meta"].(map[string]any)
	if meta["Authorization"] != "[REDACTED]" {
		t.Fatalf("expected nested auth redacted: %#v", meta)
	}
	if strings.Contains(AsString(out["notes"]), "ch_abcdef123456") || !strings.Contains(AsString(out["notes"]), "[PAYMENT_REF]") {
		t.Fatalf("expected payment ref redacted, got %#v", out["notes"])
	}
}
