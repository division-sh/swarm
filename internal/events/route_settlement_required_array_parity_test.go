package events

import (
	"encoding/json"
	"testing"
)

// Required-array presence historically checks the exact JSON key, not the
// case-insensitive struct-field matching performed by encoding/json.
func TestRouteSettlementRequiredArrayKeyParity(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		plan      bool
		accept    bool
	}{
		{"plans_exact", `{"plans":[]}`, false, true},
		{"plans_escaped_exact", `{"pl\u0061ns":[]}`, false, true},
		{"plans_case_alias", `{"Plans":[]}`, false, false},
		{"plans_null_then_alias", `{"plans":null,"Plans":[]}`, false, false},
		{"plans_duplicate_null", `{"plans":[],"plans":null}`, false, false},
		{"plans_empty_then_null_alias", `{"plans":[],"Plans":null}`, false, true},
		{"plan_arrays_exact", `{"targets":[],"candidates":[]}`, true, true},
		{"targets_case_alias", `{"Targets":[],"candidates":[]}`, true, false},
		{"candidates_case_alias", `{"targets":[],"Candidates":[]}`, true, false},
		{"targets_null_then_alias", `{"targets":null,"Targets":[],"candidates":[]}`, true, false},
		{"candidates_null_then_alias", `{"targets":[],"candidates":null,"Candidates":[]}`, true, false},
		{"targets_empty_then_null_alias", `{"targets":[],"Targets":null,"candidates":[]}`, true, true},
		{"candidates_empty_then_null_alias", `{"targets":[],"candidates":[],"Candidates":null}`, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.plan {
				var wire settlementPlanWire
				err = json.Unmarshal([]byte(tc.raw), &wire)
			} else {
				var wire settlementLedgerWire
				err = json.Unmarshal([]byte(tc.raw), &wire)
			}
			if (err == nil) != tc.accept {
				t.Fatalf("required-array admission changed: raw=%s accept=%v err=%v", tc.raw, tc.accept, err)
			}
		})
	}
}
