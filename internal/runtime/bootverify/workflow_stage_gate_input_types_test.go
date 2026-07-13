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
