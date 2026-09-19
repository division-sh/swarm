package events

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRouteSettlementRequiredArraysRetainStrictPresence(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		ledger    bool
		valid     bool
	}{
		{"plans empty", `{"plans":[]}`, true, true},
		{"plans omitted", `{}`, true, false},
		{"plans null", `{"plans": null}`, true, false},
		{"ledger null", `null`, true, false},
		{"plan null", `{"plans":[null]}`, true, false},
		{"plans wrong type", `{"plans":{}}`, true, false},
		{"ledger unknown", `{"plans":[],"extra":true}`, true, false},
		{"ledger trailing", `{"plans":[]} {}`, true, false},
		{"arrays empty", `{"targets":[],"candidates":[]}`, false, true},
		{"targets omitted", `{"candidates":[]}`, false, false},
		{"targets null", `{"targets":null,"candidates":[]}`, false, false},
		{"candidates omitted", `{"targets":[]}`, false, false},
		{"candidates null", `{"targets":[],"candidates": null}`, false, false},
		{"candidates wrong type", `{"targets":[],"candidates":{}}`, false, false},
		{"candidate unknown", `{"targets":[],"candidates":[{"extra":1}]}`, false, false},
		{"plan unknown", `{"targets":[],"candidates":[],"extra":1}`, false, false},
		{"plan trailing", `{"targets":[],"candidates":[]} {}`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("private_wire", func(t *testing.T) {
				var err error
				if tc.ledger {
					value := settlementLedgerWire{Plans: []settlementPlanWire{{}}}
					err = json.Unmarshal([]byte(tc.raw), &value)
					if err == nil && value.Plans == nil {
						t.Fatal("successful ledger decode lost explicit array")
					}
				} else {
					value := settlementPlanWire{Targets: []RouteIdentity{{FlowID: "old"}}, Candidates: []settlementCandidateWire{{Path: "old"}}}
					err = json.Unmarshal([]byte(tc.raw), &value)
					if err == nil && (value.Targets == nil || value.Candidates == nil) {
						t.Fatal("successful plan decode lost explicit arrays")
					}
				}
				if (err == nil) != tc.valid {
					t.Fatalf("decode err=%v, valid=%v", err, tc.valid)
				}
			})
			ledger := tc.raw
			if !tc.ledger {
				plan := `{"plan_sha256":"` + strings.Repeat("1", 64) + `","resolution":"no_registration",` + strings.TrimPrefix(tc.raw, "{")
				ledger = `{"plans":[` + plan + `]}`
			}
			raw := `{"write_class":"normal_publication","arm":"delivery","evaluation":` + ledger + `}`
			var value RouteSettlement
			err := json.Unmarshal([]byte(raw), &value)
			if (err == nil) != tc.valid {
				t.Fatalf("decode err=%v, valid=%v", err, tc.valid)
			}
			if err == nil && !value.Ledger().Present() {
				t.Fatal("successful decode lost explicit ledger")
			}
		})
	}
}
