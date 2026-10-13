package effects

import (
	"encoding/json"
	"reflect"
	"testing"
)

func completion2270Evidence() CompletionEvidence {
	in, out, cost := int64(10), int64(2), 0.25
	return CompletionEvidence{Request: CompletionRequest{ModelAlias: "regular", RequestedModel: "request-A", BackendProfile: "claude_cli", Provider: "claude", Transport: "cli"},
		Models: []CompletionModelObservation{{Model: "reported-B", Source: CompletionModelCLITerminal},
			{Model: "contributor-C", Source: CompletionModelCLIUsage, InputTokens: &in, OutputTokens: &out, ProviderReportedCostUSD: &cost}},
		CostProvenance: CompletionCostReported, ProviderReportedCostUSD: &cost}
}

func TestCompletion2270EvidenceValidation(t *testing.T) {
	valid := completion2270Evidence()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*CompletionEvidence){
		"missing-request": func(e *CompletionEvidence) { e.Request.RequestedModel = "" },
		"two-terminal-labels": func(e *CompletionEvidence) {
			e.Models = append(e.Models, CompletionModelObservation{Model: "different", Source: CompletionModelCLITerminal})
		},
		"mixed-terminal-sources": func(e *CompletionEvidence) { e.Models[0].Source = CompletionModelResponse },
		"duplicate-contributor":  func(e *CompletionEvidence) { e.Models = append(e.Models, e.Models[1]) },
		"unknown-source":         func(e *CompletionEvidence) { e.Models[0].Source = "current-config" },
		"blank-label":            func(e *CompletionEvidence) { e.Models[0].Model = " " },
		"negative-tokens":        func(e *CompletionEvidence) { *e.Models[1].InputTokens = -1 },
		"negative-price":         func(e *CompletionEvidence) { *e.Models[1].ProviderReportedCostUSD = -1 },
		"missing-reported-total": func(e *CompletionEvidence) { e.ProviderReportedCostUSD = nil },
		"unknown-price-owner":    func(e *CompletionEvidence) { e.CostProvenance = "exact-tokens" },
	} {
		t.Run(name, func(t *testing.T) {
			e := valid.Clone()
			mutate(&e)
			if err := e.Validate(); err == nil {
				t.Fatalf("accepted invalid evidence: %+v", e)
			}
		})
	}
}

func TestCompletion2270EvidenceCopiesDoNotAlias(t *testing.T) {
	evidence := completion2270Evidence()
	want := evidence.Canonical()
	usage := CompletionUsage{Evidence: evidence, ResolvedModel: evidence.ResolvedModel(), Exactness: CompletionUsageExact,
		InputTokens: evidence.Models[1].InputTokens, OutputTokens: evidence.Models[1].OutputTokens, ProviderReportedCostUSD: evidence.ProviderReportedCostUSD}
	copy := (CompletionSettlement{Usage: usage, AgentTurn: &CompletionAgentTurn{ResponsePayload: json.RawMessage(`{"content":"original"}`)}}).Clone()
	encoded, err := copy.Usage.Evidence.Encode()
	if err != nil {
		t.Fatal(err)
	}
	evidence.Models[0].Model = "mutated"
	*evidence.Models[1].InputTokens = 999
	*evidence.ProviderReportedCostUSD = 999
	if !reflect.DeepEqual(copy.Usage.Evidence.Canonical(), want) || *copy.Usage.InputTokens != 10 || *copy.Usage.ProviderReportedCostUSD != 0.25 {
		t.Fatalf("settlement aliases mutable evidence: %+v", copy.Usage)
	}
	decoded, err := DecodeCompletionEvidence(encoded)
	if err != nil || !reflect.DeepEqual(decoded, want) {
		t.Fatalf("encoded evidence changed: %+v, %v", decoded, err)
	}
	if _, err := DecodeCompletionEvidence([]byte(`{"request":{},"request":{}}`)); err == nil {
		t.Fatal("accepted contradictory persisted request fields")
	}
}
