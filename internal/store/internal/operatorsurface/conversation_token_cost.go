package operatorsurface

import (
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
)

// Anthropic's 2026-11-09 rate card, pp. 4-5: standard global USD per million
// ordinary input and output tokens. This version does not price cache terms.
// https://assets.claude.com/a04aca860a54b7323e33f449706e9b43680e1346.pdf
const conversationTokenRateVersion = "anthropic-standard-2026-11-09"

func projectConversationTokenCost(turn operatorread.OperatorPublicConversationTurn) *operatorread.OperatorTokenCostEstimate {
	tokens, evidence := turn.Tokens, turn.CompletionEvidence
	request := evidence.Request
	// Only the retained ordinary Anthropic Messages request class uses these
	// two count terms without a cache, batch or geography pricing selector.
	if turn.ExecutionMode != "live" || request.BackendProfile != selection.BackendAnthropic ||
		request.Provider != selection.ProviderAnthropic || request.Transport != selection.TransportAPI || request.Purpose != "" ||
		tokens == nil || tokens.Exactness != string(effects.CompletionUsageExact) || len(evidence.Models) != 1 || len(evidence.Problems) != 0 {
		return nil
	}
	model := evidence.Models[0]
	if model.Source != effects.CompletionModelResponse || model.Model != turn.ResolvedModel || model.Model != request.RequestedModel ||
		(model.InputTokens != nil && *model.InputTokens != tokens.Input) || (model.OutputTokens != nil && *model.OutputTokens != tokens.Output) ||
		(model.CacheReadInputTokens != nil && *model.CacheReadInputTokens != 0) ||
		(model.CacheCreationInputTokens != nil && *model.CacheCreationInputTokens != 0) {
		return nil
	}
	var inputRate, outputRate float64
	switch model.Model {
	case "claude-haiku-4-5-20251001":
		inputRate, outputRate = 1, 5
	case "claude-sonnet-4-6":
		inputRate, outputRate = 3, 15
	default:
		return nil
	}
	return &operatorread.OperatorTokenCostEstimate{
		USD:         (float64(tokens.Input)*inputRate + float64(tokens.Output)*outputRate) / 1_000_000,
		RateVersion: conversationTokenRateVersion,
	}
}
