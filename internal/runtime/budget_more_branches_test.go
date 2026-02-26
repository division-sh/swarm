package runtime

import (
	"context"
	"testing"

	"empireai/internal/config"
)

func TestBudgetHelpers_ModelTierAndEstimateLLMCost(t *testing.T) {
	if got := modelTier("claude-haiku-4-5"); got != "haiku" {
		t.Fatalf("expected haiku tier, got %q", got)
	}
	if got := modelTier("CLAUDE-OPUS-4"); got != "opus" {
		t.Fatalf("expected opus tier, got %q", got)
	}
	if got := modelTier("claude-sonnet-4-5"); got != "sonnet" {
		t.Fatalf("expected sonnet tier, got %q", got)
	}
	if got := modelTier("unknown-model"); got != "sonnet" {
		t.Fatalf("expected default sonnet tier, got %q", got)
	}

	bt := &BudgetTracker{}
	if got := bt.estimateLLMCostCents("claude-haiku-4-5", 1_000_000, 1_000_000); got != 480 {
		t.Fatalf("haiku cost mismatch: got %d want 480", got)
	}
	if got := bt.estimateLLMCostCents("claude-opus-4", 1_000_000, 1_000_000); got != 9000 {
		t.Fatalf("opus cost mismatch: got %d want 9000", got)
	}
	if got := bt.estimateLLMCostCents("claude-sonnet-4-5", 1_000_000, 1_000_000); got != 1800 {
		t.Fatalf("sonnet cost mismatch: got %d want 1800", got)
	}
	if got := bt.estimateLLMCostCents("claude-haiku-4-5", -100, -100); got != 0 {
		t.Fatalf("negative token clamp mismatch: got %d want 0", got)
	}
}

func TestBudgetHelpers_MergeMeta_AndEvaluateAndEmitGuard(t *testing.T) {
	merged := mergeMeta(map[string]any{"a": 1}, map[string]any{"b": 2})
	m, ok := merged.(map[string]any)
	if !ok || m["a"] != 1 || m["b"] != 2 {
		t.Fatalf("mergeMeta map branch mismatch: %#v", merged)
	}
	wrapped := mergeMeta("value", map[string]any{"k": "v"})
	w, ok := wrapped.(map[string]any)
	if !ok || w["meta"] != "value" {
		t.Fatalf("mergeMeta wrap branch mismatch: %#v", wrapped)
	}

	ctx := context.Background()
	var nilTracker *BudgetTracker
	if err := nilTracker.evaluateAndEmit(ctx, ""); err != nil {
		t.Fatalf("nil tracker evaluateAndEmit should noop, got %v", err)
	}
	partial := &BudgetTracker{cfg: &config.Config{}}
	if err := partial.evaluateAndEmit(ctx, "vertical-1"); err != nil {
		t.Fatalf("partial tracker evaluateAndEmit should noop, got %v", err)
	}
}

