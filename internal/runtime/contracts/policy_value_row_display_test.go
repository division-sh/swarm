package contracts

import (
	"fmt"
	"testing"
)

func TestContractPolicyValueRowDiagnosticsKeepPositionAndKind(t *testing.T) {
	for _, tc := range []struct {
		kind   PolicySheetRowKind
		check  func(*WorkflowContractBundle) []error
		detail string
	}{
		{PolicySheetRowKindValidate, validatePolicySheetValidationRows, "validate row must lower to compute-owned validate operation"},
		{PolicySheetRowKindModule, validatePolicySheetComputeModuleRows, "compute_module row must lower to compute-owned module operation"},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			row := HandlerRuleEntry{PolicyRow: PolicySheetRowMetadata{Kind: tc.kind}}
			bundle := &WorkflowContractBundle{Nodes: map[string]SystemNodeContract{
				"worker": {EventHandlers: map[string]SystemNodeEventHandler{"request": {Rules: []HandlerRuleEntry{row, row}}}},
			}}
			errs := tc.check(bundle)
			if len(errs) != 2 {
				t.Fatalf("value-row refusals = %v, want both distinct positional rows", errs)
			}
			for i, err := range errs {
				want := fmt.Sprintf("%s: node worker handler request rules[%d]: %s %s", ErrInvalidField, i, tc.kind, tc.detail)
				if err.Error() != want {
					t.Fatalf("value-row refusal[%d] = %v, want %q", i, err, want)
				}
			}
		})
	}
}
