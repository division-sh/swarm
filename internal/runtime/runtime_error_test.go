package runtime

import (
	"errors"
	"strings"
	"testing"
)

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
