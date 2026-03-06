package runtime

import (
	"errors"
	"strings"
	"testing"
	"time"
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
	parser := DirectiveParser{}
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
	parsed := (DirectiveParser{}).Parse("US, corpus, corpus_path=/data/test-signals-25.jsonl")
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
