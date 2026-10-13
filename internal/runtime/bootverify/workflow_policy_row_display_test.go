package bootverify

import (
	"fmt"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPolicyValueRowDiagnosticsKeepPositionAndAdmittedKind(t *testing.T) {
	for _, tc := range []struct {
		kind   runtimecontracts.PolicySheetRowKind
		check  func(*checkerContext) []Finding
		id     string
		detail string
	}{
		{runtimecontracts.PolicySheetRowKindLookup, checkPolicySheetLookupValueRows, policySheetLookupCheckID, "lookup row must lower to compute-owned lookup operation"},
		{runtimecontracts.PolicySheetRowKindValidate, checkPolicySheetValidationValueRows, policySheetValidationCheckID, "validate row must lower to compute-owned validate operation"},
		{runtimecontracts.PolicySheetRowKindModule, checkComputeModuleValueRows, computeModuleCheckID, "compute_module row must lower to compute-owned module operation"},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			row := runtimecontracts.HandlerRuleEntry{PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: tc.kind}}
			bundle := &runtimecontracts.WorkflowContractBundle{Nodes: map[string]runtimecontracts.SystemNodeContract{
				"worker": {EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"request": {Rules: []runtimecontracts.HandlerRuleEntry{row, row}},
				}},
			}}
			findings := tc.check(newCheckerContext(t.Context(), semanticview.Wrap(bundle), Options{}))
			if len(findings) != 2 {
				t.Fatalf("value-row refusals = %#v, want both distinct positional rows", findings)
			}
			for i, finding := range findings {
				want := fmt.Sprintf("flow root node worker handler request %s row rules[%d]: %s: %s", tc.kind, i, tc.kind, tc.detail)
				if finding.CheckID != tc.id || finding.Severity != SeverityHardInvalidity || finding.Location != "worker" || finding.Message != want {
					t.Fatalf("value-row finding[%d] = %#v, want exact refusal %q", i, finding, want)
				}
			}
		})
	}
}

func TestDuplicateComputedBindingUsesBothPositionalDisplays(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{
		{PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindLookup}, Compute: &runtimecontracts.ComputeSpec{StoreAs: "computed.shared"}},
		{PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindModule}, Compute: &runtimecontracts.ComputeSpec{StoreAs: "computed.shared"}},
	}}
	findings := policySheetLookupDuplicateComputedBindings("worker", "request", handler)
	want := `node worker handler request computed binding "computed.shared" is written by both handler.rules[0]: lookup.compute and handler.rules[1]: compute_module.compute`
	if len(findings) != 1 || findings[0].Message != want || findings[0].Severity != SeverityHardInvalidity {
		t.Fatalf("duplicate binding refusal = %#v, want %q", findings, want)
	}
}

func TestInvalidResultConsumerDiagnosticRetainsRuleIndexAndPredicate(t *testing.T) {
	findings := bootverifyValidationFindings(bootverifyValidationHandler(true, "deploy.other_invalid"))
	want := `flow root node deploy_node handler deploy.requested validate row rules[0]: validate: invalid-result consumer row "rules[1]: when computed.validation.deploy_manifest.valid == false" emits "deploy.other_invalid", which is not declared as a rules.yaml class disposition`
	for _, finding := range findings {
		if finding.Message == want && finding.CheckID == policySheetValidationCheckID && finding.Severity == SeverityHardInvalidity {
			return
		}
	}
	t.Fatalf("missing exact invalid-result consumer refusal %q: %#v", want, findings)
}
