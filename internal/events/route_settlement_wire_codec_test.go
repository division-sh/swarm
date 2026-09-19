package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Frozen pre-optimization decoders, only for differential tests and benchmarks.
// The ledger deliberately uses the reference plan, not the production decoder.
type settlementPlanWireBefore settlementPlanWire

type settlementLedgerWireBefore struct {
	Plans []settlementPlanWireBefore `json:"plans"`
}

func (w *settlementLedgerWireBefore) UnmarshalJSON(raw []byte) error {
	type wire settlementLedgerWireBefore
	var decoded wire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if err := settlementWireBeforeEOF(decoder); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	plans, ok := fields["plans"]
	if !ok || string(plans) == "null" {
		return fmt.Errorf("route settlement evaluation plans are required")
	}
	*w = settlementLedgerWireBefore(decoded)
	return nil
}

func (w *settlementPlanWireBefore) UnmarshalJSON(raw []byte) error {
	type wire settlementPlanWireBefore
	var decoded wire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if err := settlementWireBeforeEOF(decoder); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, field := range []string{"targets", "candidates"} {
		value, ok := fields[field]
		if !ok || string(value) == "null" {
			return fmt.Errorf("route settlement plan %s are required", field)
		}
	}
	*w = settlementPlanWireBefore(decoded)
	return nil
}

func settlementWireBeforeEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected trailing JSON value")
	}
	return nil
}

func settlementWireCanonical(t testing.TB, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func checkSettlementWireCodec(t testing.TB, raw []byte, ledger, wrapped bool) {
	t.Helper()
	seed := settlementPlanWire{
		PlanID: "old-plan", Resolution: "old-resolution",
		Targets:    []RouteIdentity{{FlowID: "old-flow", EntityID: "old-entity"}},
		Candidates: []settlementCandidateWire{{Receiver: "old-receiver", Path: "old-path"}},
	}
	var current, before json.Unmarshaler
	if ledger {
		current = &settlementLedgerWire{Plans: []settlementPlanWire{seed}}
		before = &settlementLedgerWireBefore{Plans: []settlementPlanWireBefore{settlementPlanWireBefore(seed)}}
	} else {
		current = &seed
		reference := settlementPlanWireBefore(seed)
		before = &reference
	}
	initial := settlementWireCanonical(t, current)
	decode := func(value json.Unmarshaler) error {
		if wrapped {
			return json.Unmarshal(raw, value)
		}
		return value.UnmarshalJSON(raw)
	}
	gotErr, wantErr := decode(current), decode(before)
	if (gotErr == nil) != (wantErr == nil) {
		t.Fatalf("admission changed (ledger=%v wrapped=%v) for %q: current=%v before=%v", ledger, wrapped, raw, gotErr, wantErr)
	}
	got, want := settlementWireCanonical(t, current), settlementWireCanonical(t, before)
	if !bytes.Equal(got, want) {
		t.Fatalf("canonical output changed for %q: current=%s before=%s", raw, got, want)
	}
	if gotErr != nil && (!bytes.Equal(got, initial) || !bytes.Equal(want, initial)) {
		t.Fatalf("error mutated populated receiver for %q: initial=%s current=%s before=%s", raw, initial, got, want)
	}
}

func TestRouteSettlementWireCodecDifferential(t *testing.T) {
	// Include malformed grammar and direct calls: json.Unmarshal would reject
	// trailing input itself, before either custom decoder is called.
	nonobjects := []string{"", " ", "null", " null ", "[]", "[{}]", "true", "1", `"object"`, "{}", "{", "}", "{]", `{"plans":[]`, "\xef\xbb\xbf{}"}
	for i, raw := range nonobjects {
		t.Run(fmt.Sprintf("nonobject_%d", i), func(t *testing.T) {
			for _, ledger := range []bool{false, true} {
				for _, wrapped := range []bool{false, true} {
					checkSettlementWireCodec(t, []byte(raw), ledger, wrapped)
				}
			}
		})
	}
	plan := `{"plan_sha256":"first","resolution":"resolved","targets":[{"flow_id":"flow","entity_id":"entity"}],"candidates":[{"receiver_sha256":"receiver","outcome":"accepted"}]}`
	cases := []struct {
		name, raw string
		ledger    bool
	}{
		{"plan", plan, false},
		{"ledger", `{"plans":[` + plan + `]}`, true},
		{"escaped_fields", `{"plan_\u0073ha256":"id","re\u0073olution":"resolved","tar\u0067ets":[],"candidate\u0073":[]}`, false},
		{"folded_strings", `{"PLAN_SHA256":"a","RESOLUTION":"b","targets":[],"candidates":[]}`, false},
		{"null_string_preserves_previous", `{"plan_sha256":"a","plan_sha256":null,"resolution":"b","RESOLUTION":null,"targets":[],"candidates":[]}`, false},
		{"duplicate_string_order", `{"plan_sha256":"a","PLAN_SHA256":"b","resolution":"a","Resolution":"b","targets":[],"candidates":[]}`, false},
		{"typed_error_then_valid", `{"plan_sha256":1,"plan_sha256":"a","targets":[],"candidates":[]}`, false},
		{"unknown_before", `{"unknown":0,"targets":[],"candidates":[]}`, false},
		{"unknown_after", `{"targets":[],"candidates":[],"unknown":0}`, false},
		{"unknown_target", `{"targets":[{"unknown":0}],"candidates":[]}`, false},
		{"unknown_candidate", `{"targets":[],"candidates":[{"unknown":0}]}`, false},
		{"unknown_agent", `{"targets":[],"candidates":[{"agent_plan":{"unknown":0}}]}`, false},
		{"unknown_agent_name", `{"targets":[],"candidates":[{"agent_plan":{"name":{"unknown":0}}}]}`, false},
		{"wrong_agent_route_type", `{"targets":[],"candidates":[{"agent_plan":{"route":{"scope_key":{}}}}]}`, false},
		{"duplicate_nested_object", `{"targets":[],"candidates":[{"agent_plan":{"name":{"agent_id":"a"}},"agent_plan":{"name":{"owner":"b"}}}]}`, false},
		{"null_array_elements", `{"targets":[null],"candidates":[null]}`, false},
		{"duplicate_nonempty_slices", `{"targets":[{"flow_id":"first","entity_id":"retained"}],"targets":[{"flow_id":"last"}],"candidates":[{"path":"retained"}],"CANDIDATES":[{"outcome":"last"}]}`, false},
		{"null_then_nonempty_slices", `{"targets":null,"targets":[{}],"candidates":null,"candidates":[{}]}`, false},
		{"unknown_ledger", `{"plans":[],"unknown":0}`, true},
		{"duplicate_nonempty_plans", `{"plans":[` + plan + `],"plans":[{"targets":[],"candidates":[]}]}`, true},
		{"multiple_plans_preserve_order", `{"plans":[` + plan + `,{"plan_sha256":"second","targets":[],"candidates":[]}]}`, true},
		{"empty_then_alias_plan", `{"plans":[],"PLANS":[` + plan + `]}`, true},
		{"alias_empty_after_plan", `{"plans":[` + plan + `],"PLANS":[]}`, true},
		{"duplicate_array_plan_reset", `{"plans":[` + plan + `,` + plan + `],"plans":[{"targets":[],"candidates":[]}]}`, true},
		{"duplicate_array_missing_field", `{"plans":[` + plan + `],"plans":[{"targets":[]}]}`, true},
		{"duplicate_array_null_plan", `{"plans":[` + plan + `],"plans":[null]}`, true},
		{"array_missing_comma", `{"plans":[` + plan + ` ` + plan + `]}`, true},
		{"array_trailing_comma", `{"plans":[` + plan + `,]}`, true},
		{"array_wrong_close", `{"plans":[` + plan + `}}`, true},
		{"array_nested_array", `{"plans":[[` + plan + `]]}`, true},
		{"nested_custom_error", `{"plans":[` + plan + `,{}]}`, true},
		{"nested_custom_null", `{"plans":[null]}`, true},
		{"nested_custom_error_then_valid", `{"plans":[{}],"plans":[]}`, true},
		{"nested_custom_error_after_valid", `{"plans":[],"plans":[{"targets":[]}]}`, true},
		{"nested_custom_unknown", `{"plans":[{"targets":[],"candidates":[],"unknown":0}]}`, true},
		{"nested_custom_typed_error", `{"plans":[{"targets":[],"candidates":[{"agent_plan":1}]}]}`, true},
	}
	// Enumerate last-exact-key presence independently of case-folded values,
	// including Unicode simple-fold aliases, escaped keys and both field orders.
	for _, field := range []string{"plans", "targets", "candidates"} {
		ledger := field == "plans"
		other := ""
		if field == "targets" {
			other = `"candidates":[],`
		} else if field == "candidates" {
			other = `"targets":[],`
		}
		keys := []string{field, strings.ToUpper(field), strings.ReplaceAll(field, "s", `\u017f`), strings.ReplaceAll(field, "a", `\u0061`)}
		values := []string{"[]", "null", " null ", "{}", "1", `"array"`}
		for ki, key := range keys {
			for vi, value := range values {
				cases = append(cases, struct {
					name, raw string
					ledger    bool
				}{fmt.Sprintf("%s_key%d_value%d", field, ki, vi), "{" + other + `"` + key + `":` + value + "}", ledger})
				for vj, second := range values[:3] {
					for _, reverse := range []bool{false, true} {
						left, right := `"`+field+`":`+value, `"`+key+`":`+second
						if reverse {
							left, right = right, left
						}
						cases = append(cases, struct {
							name, raw string
							ledger    bool
						}{fmt.Sprintf("%s_duplicate_%d_%d_%d_reverse%v", field, ki, vi, vj, reverse), "{" + other + left + "," + right + "}", ledger})
					}
				}
			}
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, wrapped := range []bool{false, true} {
				checkSettlementWireCodec(t, []byte(tc.raw), tc.ledger, wrapped)
				// Propagate every plan case through the ledger's custom decoder.
				if !tc.ledger {
					checkSettlementWireCodec(t, []byte(`{"plans":[`+tc.raw+`]}`), true, wrapped)
				}
			}
		})
	}
	for _, suffix := range []string{" ", "\n\t", " null", " {}", " []", " true", " 1", " garbage", ",", "\x00", "\xff"} {
		for _, ledger := range []bool{false, true} {
			raw := plan
			if ledger {
				raw = `{"plans":[` + plan + `]}`
			}
			for _, wrapped := range []bool{false, true} {
				checkSettlementWireCodec(t, []byte(raw+suffix), ledger, wrapped)
			}
		}
	}
	for _, ledger := range []bool{false, true} {
		raw := plan
		if ledger {
			raw = `{"plans":[` + plan + `]}`
		}
		for end := 0; end < len(raw); end++ {
			for _, wrapped := range []bool{false, true} {
				checkSettlementWireCodec(t, []byte(raw[:end]), ledger, wrapped)
			}
		}
	}
}

func FuzzRouteSettlementWireCodecDifferential(f *testing.F) {
	for _, raw := range []string{
		`{"plans":[]}`, `{"plans":[],"Plans":null}`, `{"plans":[{"targets":[],"candidates":[]}]}`,
		`{"targets":[],"candidates":[]}`, `{"targets":[],"TARGETS":null,"candidates":[]}`,
		`{"plans":[{}]}`, `null`, `{"targets":[],"candidates":[]} {}`,
	} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		for _, ledger := range []bool{false, true} {
			for _, wrapped := range []bool{false, true} {
				checkSettlementWireCodec(t, raw, ledger, wrapped)
			}
		}
	})
}

func BenchmarkRouteSettlementWireCodec(b *testing.B) {
	for _, size := range []int{0, 1, 18} {
		plan := settlementPlanWire{PlanID: strings.Repeat("1", 64), Resolution: "resolved", Targets: []RouteIdentity{}, Candidates: []settlementCandidateWire{}}
		for i := 0; i < size; i++ {
			plan.Targets = append(plan.Targets, RouteIdentity{FlowID: "account", FlowInstance: fmt.Sprintf("account/%02d", i), EntityID: fmt.Sprintf("entity-%02d", i)})
			plan.Candidates = append(plan.Candidates, settlementCandidateWire{Receiver: strings.Repeat("2", 64), RecipientKind: "node", RecipientID: "account/notify", Path: fmt.Sprintf("account/%02d", i), Outcome: "accepted"})
		}
		for _, ledger := range []bool{false, true} {
			raw := settlementWireCanonical(b, plan)
			if ledger {
				raw = settlementWireCanonical(b, settlementLedgerWire{Plans: []settlementPlanWire{plan}})
			}
			checkSettlementWireCodec(b, raw, ledger, false)
			for _, version := range []string{"before", "before_array", "after"} {
				var baseline json.Unmarshaler
				if ledger {
					baseline = &settlementLedgerWireBeforeArray{}
				} else {
					baseline = &settlementPlanWireBeforeArray{}
				}
				if err := baseline.UnmarshalJSON(raw); err != nil || !bytes.Equal(settlementWireCanonical(b, baseline), raw) {
					b.Fatalf("intermediate baseline fixture changed: %v", err)
				}
				b.Run(fmt.Sprintf("ledger%v/recipients%d/%s", ledger, size, version), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(raw)))
					for b.Loop() {
						var err error
						if ledger {
							switch version {
							case "before":
								var decoded settlementLedgerWireBefore
								err = decoded.UnmarshalJSON(raw)
							case "before_array":
								var decoded settlementLedgerWireBeforeArray
								err = decoded.UnmarshalJSON(raw)
							case "after":
								var decoded settlementLedgerWire
								err = decoded.UnmarshalJSON(raw)
							}
						} else {
							switch version {
							case "before":
								var decoded settlementPlanWireBefore
								err = decoded.UnmarshalJSON(raw)
							case "before_array":
								var decoded settlementPlanWireBeforeArray
								err = decoded.UnmarshalJSON(raw)
							case "after":
								var decoded settlementPlanWire
								err = decoded.UnmarshalJSON(raw)
							}
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
