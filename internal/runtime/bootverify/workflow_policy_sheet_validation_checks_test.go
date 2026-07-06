package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPolicySheetValidationValueRowsAcceptsConsumedDispositionRoute(t *testing.T) {
	handler := bootverifyValidationHandler(true, "deploy.manifest_invalid")
	findings := bootverifyValidationFindings(handler)
	if len(findings) != 0 {
		t.Fatalf("validation findings = %#v, want none", findings)
	}
}

func TestPolicySheetValidationValueRowsRejectsDeadResult(t *testing.T) {
	handler := bootverifyValidationHandler(false, "")
	findings := bootverifyValidationFindings(handler)
	if !bootverifyValidationFindingContains(findings, "is not consumed") {
		t.Fatalf("validation findings = %#v, want dead result failure", findings)
	}
}

func TestPolicySheetValidationValueRowsRejectsUndeclaredInvalidDisposition(t *testing.T) {
	handler := bootverifyValidationHandler(true, "deploy.other_invalid")
	findings := bootverifyValidationFindings(handler)
	if !bootverifyValidationFindingContains(findings, "is not declared as a policy.validation class disposition") {
		t.Fatalf("validation findings = %#v, want disposition mismatch failure", findings)
	}
}

func bootverifyValidationFindings(handler runtimecontracts.SystemNodeEventHandler) []Finding {
	pinCandidate := true
	bundle := &runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{
			Validation: map[string]runtimecontracts.PolicyValidationSet{
				"deploy_manifest": {
					Classes: map[string]runtimecontracts.PolicyValidationClass{
						"invalid": {Disposition: "deploy.manifest_invalid"},
					},
					Inputs: map[string]string{
						"source_ref":          "string",
						"manifest_source_ref": "string",
					},
					Rules: []runtimecontracts.PolicyValidationRule{{
						ID:           "VR-001",
						Class:        "invalid",
						Text:         "Manifest source ref must match request source ref.",
						PinCandidate: &pinCandidate,
						Check: runtimecontracts.PolicyValidationCheck{
							Equal: &runtimecontracts.PolicyValidationEqualCheck{
								Left:  "input.source_ref",
								Right: "input.manifest_source_ref",
							},
						},
					}},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"deploy_node": {
				ID: "deploy_node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"deploy.requested": handler,
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	return checkPolicySheetValidationValueRows(newCheckerContext(context.Background(), source, Options{}))
}

func bootverifyValidationHandler(includeConsumer bool, emitEvent string) runtimecontracts.SystemNodeEventHandler {
	validation := &runtimecontracts.ComputeValidationSpec{
		RowID: "validate_manifest",
		Set:   "deploy_manifest",
		Into:  "computed.validation.deploy_manifest",
		Input: map[string]string{
			"source_ref":          "payload.source_ref",
			"manifest_source_ref": "payload.file_manifest.source_ref",
		},
		InputPaths: map[string]paths.Path{
			"source_ref":          paths.Parse("payload.source_ref"),
			"manifest_source_ref": paths.Parse("payload.file_manifest.source_ref"),
		},
	}
	handler := runtimecontracts.SystemNodeEventHandler{
		Rules: []runtimecontracts.HandlerRuleEntry{{
			ID:        "validate_manifest",
			PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindValidate, Validation: validation},
			Compute: &runtimecontracts.ComputeSpec{
				Operation:  runtimecontracts.ComputeOpValidate,
				StoreAs:    "computed.validation.deploy_manifest",
				Validation: validation,
			},
		}},
	}
	if includeConsumer {
		handler.Rules = append(handler.Rules, runtimecontracts.HandlerRuleEntry{
			ID:        "invalid_manifest",
			Condition: "computed.validation.deploy_manifest.valid == false",
			Emit:      runtimecontracts.EmitSpec{Event: emitEvent},
		})
	}
	return handler
}

func bootverifyValidationFindingContains(findings []Finding, contains string) bool {
	for _, finding := range findings {
		if finding.CheckID == policySheetValidationCheckID && strings.Contains(finding.Message, contains) {
			return true
		}
	}
	return false
}
