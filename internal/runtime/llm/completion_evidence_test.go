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

func TestCompletion2270AbsentCLIModelIsExplicit(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		absent       bool
		invalid      bool
	}{
		{"omitted", "", true, false},
		{"null", `"model":null,"modelUsage":null,`, true, false},
		{"empty", `"model":"","modelUsage":{},`, true, false},
		{"terminal", `"model":"reported-B",`, false, false},
		{"contributor", `"model":null,"modelUsage":{"reported-B":{}},`, false, false},
		{"multiple-contributors", `"modelUsage":{"reported-B":{},"reported-C":{}},`, false, false},
		{"invalid-terminal", `"model":17,`, false, true},
		{"invalid-contributors", `"modelUsage":[],`, false, true},
	} {
		for _, format := range []string{"json", "ndjson"} {
			t.Run(tc.name+"/"+format, func(t *testing.T) {
				raw := `{"type":"result",` + tc.fields + `"usage":{"input_tokens":10,"output_tokens":2},"total_cost_usd":0.25}`
				if format == "ndjson" {
					raw = "{\"type\":\"result\",\"model\":\"earlier\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}\n" + raw
				}
				usage, err := claudeCompletionUsageFromRaw([]byte(raw))
				if (err != nil) != tc.invalid || usage.Exactness != runtimeeffects.CompletionUsageExact || usage.InputTokens == nil || *usage.InputTokens != 10 || usage.OutputTokens == nil || *usage.OutputTokens != 2 || usage.ProviderReportedCostUSD == nil || *usage.ProviderReportedCostUSD != 0.25 {
					t.Fatalf("model evidence erased valid independent dimensions: usage=%+v err=%v", usage, err)
				}
				var absent, invalid int
				for _, problem := range usage.Evidence.Problems {
					if problem.Dimension == "model" {
						if problem.Code == "absent" {
							absent++
						}
						if problem.Code == "invalid" {
							invalid++
						}
					}
				}
				if (absent == 1) != tc.absent || absent > 1 || (invalid > 0) != tc.invalid {
					t.Fatalf("model absence/invalidity was not explicit: %+v", usage.Evidence)
				}
				if tc.absent && (len(usage.Evidence.Models) != 0 || usage.ResolvedModel != "") {
					t.Fatalf("missing model resurrected earlier observations: %+v", usage.Evidence)
				}
				if err := usage.Validate(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func completionEvidencePtr[T any](v T) *T { return &v }

func TestCompletion2270ContributorDimensionsIndependent(t *testing.T) {
	for _, tc := range []struct {
		name, fields  string
		tokens, price bool
	}{
		{"duplicate-cost", `"inputTokens":10,"costUSD":0.1,"costUSD":0.2`, true, false},
		{"invalid-cost", `"inputTokens":10,"costUSD":1e999`, true, false},
		{"duplicate-counter", `"inputTokens":10,"inputTokens":20,"costUSD":0.2`, false, true},
		{"invalid-counter", `"inputTokens":-1,"costUSD":0.2`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(`{"type":"result","modelUsage":{"bad":{` + tc.fields + `},"good":{"inputTokens":5,"costUSD":0.1}},"total_cost_usd":0.3,"usage":{"input_tokens":15,"output_tokens":2}}`)
			usage, err := claudeCompletionUsageFromRaw(raw)
			if err == nil || len(usage.Evidence.Models) != 2 || usage.ResolvedModel != "" || usage.InputTokens == nil || *usage.InputTokens != 15 || usage.ProviderReportedCostUSD == nil || *usage.ProviderReportedCostUSD != 0.3 {
				t.Fatalf("one contributor dimension erased independent reports: %+v, %v", usage, err)
			}
			bad, good := usage.Evidence.Models[0], usage.Evidence.Models[1]
			if bad.Model != "bad" || good.Model != "good" || good.InputTokens == nil || *good.InputTokens != 5 || good.ProviderReportedCostUSD == nil || *good.ProviderReportedCostUSD != 0.1 || (bad.InputTokens != nil) != tc.tokens || (bad.ProviderReportedCostUSD != nil) != tc.price {
				t.Fatalf("contributor dimension falsely certified or dropped: %+v", usage.Evidence)
			}
			if err := usage.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompletion2270EstimateUsesFrozenRequestModel(t *testing.T) {
	usage := apiCompletionUsageFromRaw([]byte(`{"model":"reported-opus","usage":{"input_tokens":1000000,"output_tokens":0}}`), "input_tokens", "output_tokens")
	spend := completionSpendForContext(context.Background(), llmselection.Profile{ID: "anthropic", Provider: "anthropic", Transport: "api"}, AgentTurnRecord{}, usage,
		llmselection.ResolvedModel{ModelAlias: "cheap", ConcreteModel: "requested-haiku"})
	if spend.CostUSD != 0.8 {
		t.Fatalf("observation changed the existing request estimate: cost=%g, want 0.8", spend.CostUSD)
	}
}

func TestCompletion2270APIIndependentDimensions(t *testing.T) {
	for _, protocol := range []struct{ name, input, output string }{
		{"anthropic_api", "input_tokens", "output_tokens"},
		{"openai_responses", "input_tokens", "output_tokens"},
		{"openai_compatible", "prompt_tokens", "completion_tokens"},
	} {
		for _, tc := range []struct {
			name, model, tokens, wantModel string
			wantExact                      bool
		}{
			{"reported", `"model":"served",`, `10`, "served", true},
			{"absent-model", ``, `10`, "", true},
			{"invalid-model", `"model":17,`, `10`, "", true},
			{"duplicate-model", `"model":"first","model":"second",`, `10`, "", true},
			{"bad-tokens", `"model":"served",`, `"invalid"`, "served", false},
			{"negative-tokens", `"model":"served",`, `-1`, "served", false},
			{"zero-tokens", `"model":"served",`, `0`, "served", true},
		} {
			t.Run(protocol.name+"/"+tc.name, func(t *testing.T) {
				raw := []byte(`{` + tc.model + `"usage":{"` + protocol.input + `":` + tc.tokens + `,"` + protocol.output + `":2}}`)
				usage := apiCompletionUsageFromRaw(raw, protocol.input, protocol.output)
				if usage.ResolvedModel != tc.wantModel || (usage.Exactness == runtimeeffects.CompletionUsageExact) != tc.wantExact {
					t.Fatalf("one invalid dimension erased another: %+v", usage)
				}
				if err := usage.Validate(); err != nil {
					t.Fatal(err)
				}
				if tc.wantExact {
					*usage.InputTokens = 99
					repeated := apiCompletionUsageFromRaw(raw, protocol.input, protocol.output)
					if *repeated.InputTokens == 99 {
						t.Fatal("normalized tokens alias provider storage")
					}
				}
			})
		}
	}
}
