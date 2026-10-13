package llm

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCompletion2270TerminalFraming(t *testing.T) {
	const compact = `{"type":"result","model":"served","usage":{"input_tokens":10,"output_tokens":2},"total_cost_usd":0.3}`
	const multiline = "{\"type\":\"result\",\n\"model\":\"served\",\"usage\":{\"input_tokens\":10,\"output_tokens\":2},\"total_cost_usd\":0.3}"
	var indented bytes.Buffer
	if err := json.Indent(&indented, []byte(compact), "", "  "); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, raw, model string
		input, output    int64
		cost             float64
	}{
		{"compact", compact, "served", 10, 2, 0.3},
		{"type-inline-multiline", multiline, "served", 10, 2, 0.3},
		{"brace-alone-indented", indented.String(), "served", 10, 2, 0.3},
		{"ndjson-last-result", compact + "\n" + `{"type":"result","model":"last","usage":{"input_tokens":9,"output_tokens":8},"total_cost_usd":0.4}`, "last", 9, 8, 0.4},
		{"ndjson-trailing-metadata", compact + "\n" + `{"type":"system","model":"metadata"}`, "served", 10, 2, 0.3},
		{"stream-multiline-frame", compact + "\n" + multiline, "served", 10, 2, 0.3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			usage, err := claudeCompletionUsageFromRaw([]byte(tc.raw))
			if err != nil || usage.ResolvedModel != tc.model || usage.InputTokens == nil || *usage.InputTokens != tc.input || usage.OutputTokens == nil || *usage.OutputTokens != tc.output || usage.ProviderReportedCostUSD == nil || *usage.ProviderReportedCostUSD != tc.cost || len(usage.Evidence.Problems) != 0 {
				t.Fatalf("complete frame lost or misattributed facts: %+v, %v", usage, err)
			}
		})
	}
	// A complete metadata document's nested result is not a stream frame.
	usage, err := claudeCompletionUsageFromRaw([]byte("{\"type\":\"system\",\"metadata\":\n" + compact + "\n}"))
	if err == nil || len(usage.Evidence.Models) != 0 || usage.InputTokens != nil || usage.ProviderReportedCostUSD != nil {
		t.Fatalf("internal newlines fabricated a terminal frame: %+v, %v", usage, err)
	}
}

func TestCompletion2270InvalidUTF8LastTerminal(t *testing.T) {
	const earlier = `{"type":"result","model":"earlier","usage":{"input_tokens":1,"output_tokens":2},"total_cost_usd":0.1}`
	for _, tc := range []struct {
		name, field, model string
		invalid            bool
	}{
		{"terminal-invalid-bytes", "\"model\":\"bad\xff\"", "", true},
		{"contributor-invalid-bytes", "\"modelUsage\":{\"bad\xff\":{\"inputTokens\":9}}", "", true},
		{"contributor-valid-sibling", "\"modelUsage\":{\"bad\xff\":{\"inputTokens\":9},\"valid-sibling\":{\"inputTokens\":2}}", "valid-sibling", true},
		{"terminal-valid-escape", `"model":"valid-\u00ff"`, "valid-\u00ff", false},
		{"contributor-valid-escape", `"modelUsage":{"valid-\u00ff":{"inputTokens":9}}`, "valid-\u00ff", false},
		{"terminal-genuine-replacement", `"model":"valid-\ufffd"`, "valid-\ufffd", false},
	} {
		for _, order := range []string{"type-first", "type-last"} {
			t.Run(tc.name+"/"+order, func(t *testing.T) {
				body := tc.field + `,"usage":{"input_tokens":9,"output_tokens":8},"total_cost_usd":0.3`
				last := `{"type":"result",` + body + `}`
				if order == "type-last" {
					last = `{` + body + `,"type":"result"}`
				}
				usage, err := claudeCompletionUsageFromRaw([]byte(earlier + "\n" + last))
				if (err != nil) != tc.invalid || usage.ResolvedModel != tc.model || usage.InputTokens == nil || *usage.InputTokens != 9 || usage.OutputTokens == nil || *usage.OutputTokens != 8 || usage.ProviderReportedCostUSD == nil || *usage.ProviderReportedCostUSD != 0.3 {
					t.Fatalf("later terminal disappeared or lost independent facts: %+v, %v", usage, err)
				}
				if tc.invalid && len(usage.Evidence.Problems) == 0 {
					t.Fatal("malformed Unicode was accepted without problem evidence")
				}
				for _, observation := range usage.Evidence.Models {
					if observation.Model != tc.model {
						t.Fatalf("lossy decoder fabricated model %q", observation.Model)
					}
				}
			})
		}
	}
}
