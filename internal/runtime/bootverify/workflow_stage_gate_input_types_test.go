package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestStageGateEmitCompatibilityConsumesCanonicalInputTypeOwner(t *testing.T) {
	for _, tc := range []struct {
		input string
		event string
	}{
		{input: "text", event: "text"},
		{input: "text", event: "string"},
		{input: "integer", event: "integer"},
		{input: "numeric", event: "numeric(12,2)"},
		{input: "boolean", event: "boolean"},
		{input: "timestamp", event: "timestamp"},
		{input: "uuid", event: "uuid"},
	} {
		if !stageGateTypesCompatible(tc.input, tc.event) {
			t.Fatalf("gate input %s is incompatible with event type %s", tc.input, tc.event)
		}
	}
	for _, input := range []string{"string", "int", "float", "jsonb", "text[]"} {
		if stageGateTypesCompatible(input, input) {
			t.Fatalf("non-canonical gate input %s was compatible", input)
		}
	}
}

func TestStageGateVerificationRejectsProgrammaticNonCanonicalInputType(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Name: "launch", InitialStage: "awaiting_review", Stages: []runtimecontracts.WorkflowStageContract{{ID: "awaiting_review"}, {ID: "building"}},
		Gates: []runtimecontracts.WorkflowGatePlan{{
			Stage: "awaiting_review", Decision: "launch_review",
			Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
				"reject": {
					AdvancesTo: "building",
					Input:      map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "string", Required: true}},
				},
			},
		}},
	}}
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	found := false
	for _, finding := range report.Errors() {
		if finding.CheckID == stageGateValidationCheckID && strings.Contains(finding.Message, "unsupported stage gate input type") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("stage gate verification findings = %#v, want noncanonical input blocker", report.Errors())
	}
}

func TestStageGateVerificationRejectsProgrammaticNonExactCanonicalInputType(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Name: "launch", InitialStage: "awaiting_review", Stages: []runtimecontracts.WorkflowStageContract{{ID: "awaiting_review"}, {ID: "building"}},
		Gates: []runtimecontracts.WorkflowGatePlan{{
			Stage: "awaiting_review", Decision: "launch_review",
			Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
				"reject": {
					AdvancesTo: "building",
					Input:      map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: " TEXT ", Required: true}},
				},
			},
		}},
	}}
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	for _, finding := range report.Errors() {
		if finding.CheckID == stageGateValidationCheckID && strings.Contains(finding.Message, "is not canonical") {
			return
		}
	}
	t.Fatalf("stage gate verification findings = %#v, want exact canonical-spelling blocker", report.Errors())
}

func TestStageGateLiteralEmitConsumesExactResolvedEventFieldSchema(t *testing.T) {
	minLength := 3
	minScore := 0.0
	maxScore := 1.0
	tests := []struct {
		name       string
		field      runtimecontracts.EventFieldSpec
		valid      any
		invalid    any
		rootTypes  runtimecontracts.TypeCatalogDocument
		wantDetail string
	}{
		{name: "text", field: runtimecontracts.EventFieldSpec{Type: "text"}, valid: "ready", invalid: true, wantDetail: "must be string"},
		{name: "integer", field: runtimecontracts.EventFieldSpec{Type: "integer"}, valid: 7, invalid: 7.5, wantDetail: "must be integer"},
		{name: "numeric", field: runtimecontracts.EventFieldSpec{Type: "numeric"}, valid: 0.5, invalid: "0.5", wantDetail: "must be number"},
		{name: "boolean", field: runtimecontracts.EventFieldSpec{Type: "boolean"}, valid: true, invalid: "true", wantDetail: "must be boolean"},
		{name: "timestamp format", field: runtimecontracts.EventFieldSpec{Type: "timestamp"}, valid: "2026-07-13T03:00:00Z", invalid: "tomorrow", wantDetail: "RFC3339"},
		{name: "uuid format", field: runtimecontracts.EventFieldSpec{Type: "uuid"}, valid: "17e1d38a-ed95-49e7-a5e9-6421e15aa503", invalid: "card-1", wantDetail: "must be uuid"},
		{
			name: "enum", field: runtimecontracts.EventFieldSpec{Type: "ReviewMode"}, valid: "fast", invalid: "slow", wantDetail: "enum",
			rootTypes: runtimecontracts.TypeCatalogDocument{Enums: map[string]runtimecontracts.EnumTypeDecl{"ReviewMode": {Values: []string{"fast", "deep"}}}},
		},
		{
			name: "pattern", field: runtimecontracts.EventFieldSpec{Type: "text", Refinements: runtimecontracts.SchemaRefinements{Pattern: "^[a-z]+$"}},
			valid: "ready", invalid: "NOT-READY", wantDetail: "pattern",
		},
		{
			name: "length", field: runtimecontracts.EventFieldSpec{Type: "text", Refinements: runtimecontracts.SchemaRefinements{Length: runtimecontracts.SchemaLengthRefinement{Min: &minLength}}},
			valid: "ready", invalid: "no", wantDetail: "length",
		},
		{
			name: "range", field: runtimecontracts.EventFieldSpec{Type: "numeric", Refinements: runtimecontracts.SchemaRefinements{Range: runtimecontracts.SchemaRangeRefinement{Min: &minScore, Max: &maxScore}}},
			valid: 0.5, invalid: 2.0, wantDetail: "<=",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			validReport := stageGateLiteralEmitReport(tc.field, tc.rootTypes, tc.valid)
			if finding := stageGateLiteralFinding(validReport); finding != "" {
				t.Fatalf("valid literal produced stage-gate finding: %s", finding)
			}

			invalidReport := stageGateLiteralEmitReport(tc.field, tc.rootTypes, tc.invalid)
			finding := stageGateLiteralFinding(invalidReport)
			if finding == "" || !strings.Contains(finding, tc.wantDetail) {
				t.Fatalf("invalid literal findings = %#v, want stage-gate detail %q", invalidReport.Errors(), tc.wantDetail)
			}
		})
	}
}

func stageGateLiteralEmitReport(field runtimecontracts.EventFieldSpec, rootTypes runtimecontracts.TypeCatalogDocument, literal any) Report {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootTypes: rootTypes,
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"review.completed": {
				Payload:  runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"value": field}},
				Required: []string{"value"},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "launch", InitialStage: "awaiting_review", Stages: []runtimecontracts.WorkflowStageContract{{ID: "awaiting_review"}, {ID: "complete"}},
			Gates: []runtimecontracts.WorkflowGatePlan{{
				Stage: "awaiting_review", Decision: "launch_review",
				Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"approve": {
						AdvancesTo: "complete",
						Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
							"value": runtimecontracts.LiteralExpression(literal),
						}},
					},
				},
			}},
		},
	}
	return Run(context.Background(), semanticview.Wrap(bundle), Options{})
}

func stageGateLiteralFinding(report Report) string {
	for _, finding := range report.Errors() {
		if finding.CheckID == stageGateValidationCheckID && strings.Contains(finding.Message, "literal emit field") {
			return finding.Message
		}
	}
	return ""
}
