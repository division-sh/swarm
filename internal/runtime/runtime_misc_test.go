package runtime

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"empireai/internal/models"
	llm "empireai/internal/runtime/llm"
	runtimepipeline "empireai/internal/runtime/pipeline"
)

func TestTimeutil_WeekStartAndNextReset(t *testing.T) {
	now := time.Date(2026, 2, 14, 10, 0, 0, 0, time.UTC)
	start := WeekStartUTC(now, "monday")
	if start.Weekday() != time.Monday || start.Hour() != 0 {
		t.Fatalf("unexpected week start: %v", start)
	}
	next := NextWeekResetUTC(now, "monday")
	if !next.After(now) {
		t.Fatalf("expected next reset after now: now=%v next=%v", now, next)
	}

	if parseWeekday("tuesday") != time.Tuesday {
		t.Fatal("expected tuesday")
	}
	if parseWeekday("bad") != time.Monday {
		t.Fatal("expected default monday")
	}
}

func TestDirectiveParser(t *testing.T) {
	parser := runtimepipeline.DirectiveParser{}
	parsed := parser.Parse("Run saas_trend in Paraguay focus on fintech, payroll avoid crypto budget $1200")
	if parsed.Mode != "saas_trend" {
		t.Fatalf("expected mode saas_trend, got %q", parsed.Mode)
	}
	if !parsed.ExplicitMode {
		t.Fatalf("expected explicit mode=true")
	}
	if parsed.Geography != "Paraguay" {
		t.Fatalf("expected geography Paraguay, got %q", parsed.Geography)
	}
	if parsed.BudgetCap != 1200 {
		t.Fatalf("expected budget cap 1200, got %v", parsed.BudgetCap)
	}
	if len(parsed.TaxonomyFocus) == 0 {
		t.Fatalf("expected taxonomy_focus parsed")
	}
	if len(parsed.AvoidSectors) == 0 {
		t.Fatalf("expected avoid_sectors parsed")
	}
	if parsed.Intent == "" {
		t.Fatalf("expected intent to be set")
	}
}

func TestDirectiveParser_ExtractsCorpusPath(t *testing.T) {
	parsed := (runtimepipeline.DirectiveParser{}).Parse("US, corpus, corpus_path=/data/test-signals-25.jsonl")
	if parsed.Mode != "corpus" {
		t.Fatalf("expected corpus mode, got %q", parsed.Mode)
	}
	if parsed.CorpusPath != "/data/test-signals-25.jsonl" {
		t.Fatalf("expected corpus_path extracted, got %q", parsed.CorpusPath)
	}
}

func TestRuntimeErrorFormattingIncludesEnvelopeAndCause(t *testing.T) {
	base := errors.New("schema validation failed: $.mode is required")
	err := WrapRuntimeError(
		"schema_validation_failed",
		"tool-executor",
		"handle_emit_tool.validate_schema_pre_normalize",
		false,
		base,
		"emit payload schema validation failed",
	)
	if err == nil {
		t.Fatal("expected wrapped runtime error")
	}
	text := err.Error()
	for _, want := range []string{
		"runtime_error",
		"code=schema_validation_failed",
		"component=tool-executor",
		"operation=handle_emit_tool.validate_schema_pre_normalize",
		"retryable=false",
		"emit payload schema validation failed",
		"schema validation failed: $.mode is required",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected %q in runtime error text, got: %s", want, text)
		}
	}
}

func TestFormatRuntimeErrorFallbacks(t *testing.T) {
	if got := strings.TrimSpace(FormatRuntimeError(nil)); got != "" {
		t.Fatalf("expected empty string for nil error, got %q", got)
	}
	plain := errors.New("plain failure")
	if got := FormatRuntimeError(plain); got != "plain failure" {
		t.Fatalf("expected plain passthrough, got %q", got)
	}
}

func TestSchemaHelperPrimitives(t *testing.T) {
	if !isNumeric(1) || !isNumeric(1.5) || !isNumeric(uint32(3)) {
		t.Fatal("expected numeric primitives to be accepted")
	}
	if isNumeric("1") {
		t.Fatal("string should not be treated as numeric primitive")
	}

	if !isInteger(1) || !isInteger(int64(2)) || !isInteger(2.0) {
		t.Fatal("expected integer values to pass integer check")
	}
	if isInteger(2.5) || isInteger("2") {
		t.Fatal("expected non-integer values to fail integer check")
	}

	if got := requiredList([]any{"a", " ", "b"}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("requiredList []any mismatch: %+v", got)
	}
	if got := requiredList([]string{"x", "y"}); len(got) != 2 {
		t.Fatalf("requiredList []string mismatch: %+v", got)
	}
	if got := requiredList("x"); got != nil {
		t.Fatalf("requiredList default should return nil, got %+v", got)
	}
}

func TestRuntimeHelperFunctions_Misc(t *testing.T) {

	filtered := filterOutVerticalScopedAgentIDs([]string{"opco-ceo-v1", "empire-coordinator", "vp-product-v1", "x"}, "v1")
	if len(filtered) != 2 || filtered[0] != "empire-coordinator" {
		t.Fatalf("filterOutVerticalScopedAgentIDs mismatch: %+v", filtered)
	}

	deduped := dedupeToolCalls([]llm.ToolCall{
		{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "saas_gap"}},
		{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "saas_gap"}},
		{Name: "emit_scan_requested", Arguments: map[string]any{"mode": "saas_trend"}},
		{Name: "", Arguments: map[string]any{}},
	})
	if len(deduped) != 2 {
		t.Fatalf("expected deduped tool calls length 2, got %d", len(deduped))
	}

	reg := newMCPTurnRegistry()
	now := time.Now().UTC()
	reg.put("old", mcpTurnContext{CreatedAt: now.Add(-20 * time.Minute), ExpiresAt: now.Add(-time.Minute)})
	reg.put("new", mcpTurnContext{CreatedAt: now, ExpiresAt: now.Add(20 * time.Minute)})
	reg.mu.Lock()
	reg.pruneLocked(now)
	reg.mu.Unlock()
	if _, ok := reg.get("old"); ok {
		t.Fatal("expected old mcp turn context pruned")
	}
	if _, ok := reg.get("new"); !ok {
		t.Fatal("expected new mcp turn context retained")
	}

	if got := toStringList([]any{"a", " ", 3}); len(got) != 2 || got[0] != "a" || got[1] != "3" {
		t.Fatalf("toStringList []any mismatch: %+v", got)
	}
	if got := toStringList(`["x","y"]`); len(got) != 2 || got[0] != "x" {
		t.Fatalf("toStringList json string mismatch: %+v", got)
	}
	if got := toStringList("x, y"); len(got) != 2 || got[1] != "y" {
		t.Fatalf("toStringList csv mismatch: %+v", got)
	}

	if name, country, _ := parseDirectiveGeography("SaaS in Paraguay for clinics"); name != "Paraguay" || country != "Paraguay" {
		t.Fatalf("parseDirectiveGeography known country mismatch: name=%q country=%q", name, country)
	}
	if name, _, _ := parseDirectiveGeography("SaaS in custom market where internet is high"); name != "Custom Market" {
		t.Fatalf("parseDirectiveGeography phrase extraction mismatch: %q", name)
	}
}

func TestExtractCategoryList(t *testing.T) {
	got := extractCategoryList(map[string]any{
		"taxonomy_categories": []any{"a", "b"},
	})
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("extractCategoryList mismatch: %+v", got)
	}

	got2 := extractCategoryList(map[string]any{
		"taxonomy_categories": []string{"x", "y"},
	})
	if len(got2) != 2 || got2[0] != "x" || got2[1] != "y" {
		t.Fatalf("extractCategoryList list mismatch: %+v", got2)
	}

	_ = models.AgentConfig{}
}

func TestNormalizeEventTokenAndResolveProviderEventType(t *testing.T) {
	if normalizeEventToken("  Payment.Succeeded ") != "payment_succeeded" {
		t.Fatal("unexpected normalization")
	}
	if normalizeEventToken("") != "event" {
		t.Fatal("expected default event")
	}

	if resolveProviderEventType("domain", map[string]any{"status": "Confirmed-OK"}) != "confirmed_ok" {
		t.Fatal("expected domain status token")
	}
	if resolveProviderEventType("stripe", map[string]any{"type": "invoice.paid"}) != "invoice_paid" {
		t.Fatal("expected stripe type token")
	}
	if resolveProviderEventType("stripe", map[string]any{}) != "payment_event" {
		t.Fatal("expected stripe default")
	}
}

func TestVerifyProviderSignature_StripeAndDefaultAndNoSecret(t *testing.T) {
	body := []byte(`{"ok":true}`)

	if !verifyProviderSignature("email", "", body, http.Header{}) {
		t.Fatal("expected email unsigned accepted")
	}
	if verifyProviderSignature("stripe", "", body, http.Header{}) {
		t.Fatal("expected unsigned stripe rejected")
	}

	secret := "whsec_test"
	ts := "1700000000"
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(ts))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))

	h := http.Header{}
	h.Set("Stripe-Signature", "t="+ts+",v1="+expected)
	if !verifyProviderSignature("stripe", secret, body, h) {
		t.Fatal("expected stripe signature to verify")
	}
	h.Set("Stripe-Signature", "t="+ts+",v1=deadbeef")
	if verifyProviderSignature("stripe", secret, body, h) {
		t.Fatal("expected stripe signature mismatch")
	}

	h = http.Header{}
	h.Set("X-Webhook-Token", "tok")
	if !verifyProviderSignature("domain", "tok", body, h) {
		t.Fatal("expected token header to verify")
	}
	h = http.Header{}
	h.Set("Authorization", "Bearer tok")
	if !verifyProviderSignature("domain", "tok", body, h) {
		t.Fatal("expected bearer token to verify")
	}
}

func TestParseWeekday_AllCases(t *testing.T) {
	cases := map[string]time.Weekday{
		"sunday":    time.Sunday,
		"monday":    time.Monday,
		"tuesday":   time.Tuesday,
		"wednesday": time.Wednesday,
		"thursday":  time.Thursday,
		"friday":    time.Friday,
		"saturday":  time.Saturday,
		"  FrIdAy ": time.Friday,
		"nope":      time.Monday,
	}
	for in, want := range cases {
		if got := parseWeekday(in); got != want {
			t.Fatalf("parseWeekday(%q)=%v want %v", in, got, want)
		}
	}
}
