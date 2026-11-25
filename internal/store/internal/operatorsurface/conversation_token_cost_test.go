package operatorsurface

import (
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func TestProjectConversationTokenCost(t *testing.T) {
	const haiku, sonnet = "claude-haiku-4-5-20251001", "claude-sonnet-4-6"
	type turn = operatorread.OperatorPublicConversationTurn
	setModel := func(v *turn, model string) {
		v.ResolvedModel, v.CompletionEvidence.Request.RequestedModel, v.CompletionEvidence.Models[0].Model = model, model, model
	}
	one := int64(1)
	for _, tc := range []struct {
		name   string
		mutate func(*turn)
		usd    float64 // -1 means omitted
	}{
		{"haiku", func(*turn) {}, 0.0123},
		{"sonnet", func(v *turn) { setModel(v, sonnet) }, 0.0369},
		{"zero", func(v *turn) { v.Tokens.Input, v.Tokens.Output = 0, 0 }, 0},
		{"unlisted_model", func(v *turn) { setModel(v, "custom-"+haiku) }, -1},
		{"no_alias", func(v *turn) { setModel(v, "claude-haiku-4-5") }, -1},
		{"other_provider", func(v *turn) {
			r := &v.CompletionEvidence.Request
			r.Provider, r.BackendProfile = "openai_compatible", "openai_compatible"
		}, -1},
		{"compaction", func(v *turn) { v.CompletionEvidence.Request.Purpose = effects.ContextCompactionPurpose }, -1},
		{"mock", func(v *turn) { v.ExecutionMode = "mock" }, -1},
		{"absent_model", func(v *turn) { v.CompletionEvidence.Models = nil }, -1},
		{"mixed_models", func(v *turn) {
			v.CompletionEvidence.Models = append(v.CompletionEvidence.Models, effects.CompletionModelObservation{Model: sonnet, Source: effects.CompletionModelCLIUsage})
		}, -1},
		{"different_request", func(v *turn) { v.CompletionEvidence.Request.RequestedModel = sonnet }, -1},
		{"different_count", func(v *turn) { v.CompletionEvidence.Models[0].InputTokens = &one }, -1},
		{"uncovered_cache_read", func(v *turn) { v.CompletionEvidence.Models[0].CacheReadInputTokens = &one }, -1},
		{"uncovered_cache_write", func(v *turn) { v.CompletionEvidence.Models[0].CacheCreationInputTokens = &one }, -1},
		{"absent_usage", func(v *turn) { v.Tokens = nil }, -1},
		{"estimated_usage", func(v *turn) { v.Tokens.Exactness = "estimated" }, -1},
		{"usage_problem", func(v *turn) {
			v.CompletionEvidence.Problems = []effects.CompletionEvidenceProblem{{Source: "response.usage", Dimension: "tokens", Code: "invalid"}}
		}, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := turn{
				ExecutionMode: "live", ResolvedModel: haiku,
				Tokens: &operatorread.OperatorConversationTokenUsage{Input: 2300, Output: 2000, Exactness: "exact"},
				CompletionEvidence: effects.CompletionEvidence{
					Request: effects.CompletionRequest{ModelAlias: "regular", RequestedModel: haiku, BackendProfile: "anthropic", Provider: "anthropic", Transport: "api"},
					Models:  []effects.CompletionModelObservation{{Model: haiku, Source: effects.CompletionModelResponse}},
				},
			}
			tc.mutate(&v)
			got := projectConversationTokenCost(v)
			if tc.usd == -1 {
				if got != nil {
					t.Fatalf("uncovered evidence priced: %+v", got)
				}
			} else if got == nil || got.USD != tc.usd || got.RateVersion != "anthropic-standard-2026-11-09" {
				t.Fatalf("estimate = %+v, want USD %v at the published version", got, tc.usd)
			}
		})
	}
}
