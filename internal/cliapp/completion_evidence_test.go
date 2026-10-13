package cliapp

import (
	"strings"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func completionEvidenceFixtureForCLI() runtimeeffects.CompletionEvidence {
	return (runtimeeffects.CompletionEvidence{Request: runtimeeffects.CompletionRequest{
		ModelAlias: "regular", RequestedModel: "request-A", BackendProfile: "claude_cli", Provider: "claude", Transport: "cli"},
		Models: []runtimeeffects.CompletionModelObservation{}, CostProvenance: runtimeeffects.CompletionCostUnavailable}).Canonical()
}

func TestCompletion2270CLIReadback(t *testing.T) {
	e := completionEvidenceFixtureForCLI()
	e.Models = []runtimeeffects.CompletionModelObservation{{Model: "reported-B", Source: runtimeeffects.CompletionModelCLIUsage}, {Model: "reported-C", Source: runtimeeffects.CompletionModelCLIUsage}}
	if err := validateCLICompletionEvidence("test", e, ""); err != nil {
		t.Fatal(err)
	}
	if err := validateCLICompletionEvidence("test", e, "request-A"); err == nil {
		t.Fatal("accepted request fallback in observed model readback")
	}
	text := cliCompletionEvidenceSummary(e)
	for _, want := range []string{"requested request-A", "reported-B", "reported-C", "claude.result.modelUsage", "cost unavailable"} {
		if !strings.Contains(text, want) {
			t.Fatalf("human evidence omitted %q: %s", want, text)
		}
	}
}
