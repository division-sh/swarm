package llm

import (
	"slices"
	"testing"

	"empireai/internal/config"
)

func TestAppendClaudePrintModeArgs_AddsVerboseForStreamJSON(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"

	args := appendClaudePrintModeArgs([]string{"-p", "--output-format", "stream-json"}, cfg)

	if !slices.Contains(args, "--include-partial-messages") {
		t.Fatalf("args = %#v, want --include-partial-messages", args)
	}
	if !slices.Contains(args, "--verbose") {
		t.Fatalf("args = %#v, want --verbose", args)
	}
}

func TestAppendClaudePrintModeArgs_LeavesJSONUnchanged(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.ClaudeCLI.OutputFormat = "json"

	args := appendClaudePrintModeArgs([]string{"-p", "--output-format", "json"}, cfg)

	if slices.Contains(args, "--include-partial-messages") {
		t.Fatalf("args = %#v, do not want --include-partial-messages", args)
	}
	if slices.Contains(args, "--verbose") {
		t.Fatalf("args = %#v, do not want --verbose", args)
	}
}
