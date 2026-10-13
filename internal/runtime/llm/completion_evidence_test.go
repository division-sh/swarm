package llm

import (
	"context"
	"reflect"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func TestCompletion2270RequestModelIsNotProviderObservation(t *testing.T) {
	u, err := claudeCompletionUsageFromRaw([]byte(`{"type":"result","model":"reported-model","usage":{"input_tokens":10,"output_tokens":2}}`))
	if err != nil {
		t.Fatal(err)
	}
	if u.ResolvedModel != "reported-model" {
		t.Fatalf("served model replaced by request: %q", u.ResolvedModel)
	}
}

func TestCompletion2270AdapterEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, raw, model string
		models           int
		exactness        runtimeeffects.CompletionUsageExactness
		cost             *float64
		wantErr          bool
	}{
		{"terminal", `{"type":"result","model":"served","usage":{"input_tokens":10,"output_tokens":2}}`, "served", 1, runtimeeffects.CompletionUsageExact, nil, false},
		{"single-contributor", `{"type":"result","modelUsage":{"served":{"inputTokens":10,"costUSD":0.2}},"usage":{"input_tokens":10,"output_tokens":2}}`, "served", 1, runtimeeffects.CompletionUsageExact, nil, false},
		{"multiple-contributors", `{"type":"result","modelUsage":{"b":{"inputTokens":6},"a":{"inputTokens":4}},"usage":{"input_tokens":10,"output_tokens":2}}`, "", 2, runtimeeffects.CompletionUsageExact, nil, false},
		{"terminal-with-contributors", `{"type":"result","model":"served","modelUsage":{"a":{},"b":{}},"usage":{"input_tokens":10,"output_tokens":2}}`, "served", 3, runtimeeffects.CompletionUsageExact, nil, false},
		{"missing-model", `{"type":"result","usage":{"input_tokens":10,"output_tokens":2}}`, "", 0, runtimeeffects.CompletionUsageExact, nil, false},
		{"zero-price", `{"type":"result","model":"served","total_cost_usd":0,"usage":{"input_tokens":10,"output_tokens":2}}`, "served", 1, runtimeeffects.CompletionUsageExact, completionEvidencePtr(0.0), false},
		{"no-tokens-with-price", `{"type":"result","model":"served","total_cost_usd":0.3}`, "served", 1, runtimeeffects.CompletionUsageUnavailable, completionEvidencePtr(0.3), true},
		{"bad-tokens", `{"type":"result","model":"served","total_cost_usd":0.3,"usage":{"input_tokens":-1,"output_tokens":2}}`, "served", 1, runtimeeffects.CompletionUsageUnavailable, completionEvidencePtr(0.3), true},
		{"bad-price", `{"type":"result","model":"served","total_cost_usd":-1,"usage":{"input_tokens":10,"output_tokens":2}}`, "served", 1, runtimeeffects.CompletionUsageExact, nil, true},
		{"price-overflow", `{"type":"result","model":"served","total_cost_usd":1e999,"usage":{"input_tokens":10,"output_tokens":2}}`, "served", 1, runtimeeffects.CompletionUsageExact, nil, true},
		{"bad-model", `{"type":"result","model":{},"total_cost_usd":0.3,"usage":{"input_tokens":10,"output_tokens":2}}`, "", 0, runtimeeffects.CompletionUsageExact, completionEvidencePtr(0.3), true},
		{"contradictory-terminal", `{"type":"result","model":"a","model":"b","total_cost_usd":0.3,"usage":{"input_tokens":10,"output_tokens":2}}`, "", 0, runtimeeffects.CompletionUsageExact, completionEvidencePtr(0.3), true},
		{"contradictory-price", `{"type":"result","model":"served","total_cost_usd":0.3,"total_cost_usd":0.5,"usage":{"input_tokens":10,"output_tokens":2}}`, "served", 1, runtimeeffects.CompletionUsageExact, nil, true},
		{"bad-model-usage", `{"type":"result","model":"served","modelUsage":[],"usage":{"input_tokens":10,"output_tokens":2}}`, "served", 1, runtimeeffects.CompletionUsageExact, nil, true},
		{"bad-contributor-price", `{"type":"result","modelUsage":{"served":{"inputTokens":10,"costUSD":-1}},"usage":{"input_tokens":10,"output_tokens":2}}`, "served", 1, runtimeeffects.CompletionUsageExact, nil, true},
		{"duplicate-contributor", `{"type":"result","modelUsage":{"a":{},"a":{}},"usage":{"input_tokens":10,"output_tokens":2}}`, "", 0, runtimeeffects.CompletionUsageExact, nil, true},
		{"invalid-cache", `{"type":"result","model":"served","total_cost_usd":0.3,"usage":{"input_tokens":10,"output_tokens":2,"cache_creation_input_tokens":1,"cache_creation":{"ephemeral_5m_input_tokens":2}}}`, "served", 1, runtimeeffects.CompletionUsageUnavailable, completionEvidencePtr(0.3), true},
	} {
		for _, format := range []string{"json", "ndjson"} {
			t.Run(tc.name+"/"+format, func(t *testing.T) {
				raw := tc.raw
				if format == "ndjson" {
					raw = "{\"type\":\"system\",\"model\":\"init-not-served\"}\n{\"type\":\"result\",\"model\":\"earlier\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}\n" + raw
				}
				usage, err := claudeCompletionUsageFromRaw([]byte(raw))
				if (err != nil) != tc.wantErr || usage.ResolvedModel != tc.model || len(usage.Evidence.Models) != tc.models || usage.Exactness != tc.exactness || !reflect.DeepEqual(usage.ProviderReportedCostUSD, tc.cost) {
					t.Fatalf("usage=%+v evidence=%+v error=%v", usage, usage.Evidence, err)
				}
				if tc.wantErr && len(usage.Evidence.Problems) == 0 {
					t.Fatal("invalid dimension falsely certified without typed problem evidence")
				}
				if err := usage.Validate(); err != nil {
					t.Fatalf("independent valid evidence was not retained: %v", err)
				}
			})
		}
	}
}

func TestCompletion2270NoContributorDoubleCharge(t *testing.T) {
	usage, err := claudeCompletionUsageFromRaw([]byte(`{"type":"result","total_cost_usd":0.25,"modelUsage":{"a":{"inputTokens":10,"costUSD":0.1},"b":{"inputTokens":20,"costUSD":0.2}},"usage":{"input_tokens":30,"output_tokens":2}}`))
	if err != nil {
		t.Fatal(err)
	}
	spend := completionSpendForContext(context.Background(), llmselection.Profile{ID: "claude_cli", Provider: "claude", Transport: "cli"}, AgentTurnRecord{}, usage,
		llmselection.ResolvedModel{ModelAlias: "cheap", ConcreteModel: "requested-haiku"})
	if spend.CostUSD != 0.25 || *usage.InputTokens != 30 || usage.ResolvedModel != "" || len(usage.Evidence.Models) != 2 {
		t.Fatalf("per-model reports changed total accounting: spend=%+v usage=%+v", spend, usage)
	}
}

func completionEvidencePtr[T any](v T) *T { return &v }

func TestCompletion2270EstimateUsesFrozenRequestModel(t *testing.T) {
	usage := completionObservedUsage(1_000_000, 0, "reported-opus")
	spend := completionSpendForContext(context.Background(), llmselection.Profile{ID: "anthropic", Provider: "anthropic", Transport: "api"}, AgentTurnRecord{}, usage,
		llmselection.ResolvedModel{ModelAlias: "cheap", ConcreteModel: "requested-haiku"})
	if spend.CostUSD != 0.8 {
		t.Fatalf("observation changed the existing request estimate: cost=%g, want 0.8", spend.CostUSD)
	}
}
