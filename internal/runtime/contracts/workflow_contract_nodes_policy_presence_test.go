package contracts

import (
	"reflect"
	"strings"
	"testing"
)

func TestW4PolicyRowNestedFieldPresence(t *testing.T) {
	scalarOnly := [8]bool{false, false, false, true, false, false, false, false}
	optionalScalar := [8]bool{true, false, false, true, false, false, false, false}
	for _, tc := range []struct {
		kind, field, base, scalar, sequence, mapping string
		admit                                        [8]bool
		read                                         func(HandlerRuleEntry) any
		want                                         map[string]any
	}{
		{"case", "selector", "equals: service\n", "payload.kind", "[payload.kind]", "{path: payload.kind}", scalarOnly,
			func(r HandlerRuleEntry) any { return r.PolicyRow.Selectors }, map[string]any{"scalar": []string{"payload.kind"}}},
		{"case", "selectors", "equals: service\n", "payload.kind", "[payload.kind]", "{path: payload.kind}", [8]bool{false, false, false, true, false, true, false, false},
			func(r HandlerRuleEntry) any { return r.PolicyRow.Selectors }, map[string]any{"scalar": []string{"payload.kind"}, "sequence": []string{"payload.kind"}}},
		{"case", "equals", "selector: payload.kind\n", "service", "[service]", "{kind: service}", [8]bool{false, false, true, true, false, true, false, false},
			func(r HandlerRuleEntry) any { return r.PolicyRow.CaseValues }, map[string]any{"empty_scalar": []string{""}, "scalar": []string{"service"}, "sequence": []string{"service"}}},
		{"range", "value", "gte: 0\n", "payload.score", "[payload.score]", "{path: payload.score}", scalarOnly,
			func(r HandlerRuleEntry) any { return r.PolicyRow.RangeValue }, map[string]any{"scalar": "payload.score"}},
		{"range", "gt", "value: payload.score\nlt: 10\n", "2", "[2]", "{value: 2}", optionalScalar,
			func(r HandlerRuleEntry) any { return r.PolicyRow.RangeLower }, map[string]any{"missing": PolicySheetRangeBound{}, "scalar": PolicySheetRangeBound{Operator: ">", Value: "2", Kind: "literal"}}},
		{"range", "gte", "value: payload.score\nlt: 10\n", "2", "[2]", "{value: 2}", optionalScalar,
			func(r HandlerRuleEntry) any { return r.PolicyRow.RangeLower }, map[string]any{"missing": PolicySheetRangeBound{}, "scalar": PolicySheetRangeBound{Operator: ">=", Value: "2", Kind: "literal"}}},
		{"range", "lt", "value: payload.score\ngt: 0\n", "2", "[2]", "{value: 2}", optionalScalar,
			func(r HandlerRuleEntry) any { return r.PolicyRow.RangeUpper }, map[string]any{"missing": PolicySheetRangeBound{}, "scalar": PolicySheetRangeBound{Operator: "<", Value: "2", Kind: "literal"}}},
		{"range", "lte", "value: payload.score\ngt: 0\n", "2", "[2]", "{value: 2}", optionalScalar,
			func(r HandlerRuleEntry) any { return r.PolicyRow.RangeUpper }, map[string]any{"missing": PolicySheetRangeBound{}, "scalar": PolicySheetRangeBound{Operator: "<=", Value: "2", Kind: "literal"}}},
		{"range", "monotonicity", "value: payload.score\ngte: 0\n", "policy.low <= policy.high", "['policy.low <= policy.high']", "{constraint: ordered}", [8]bool{true, false, true, true, true, true, false, false},
			func(r HandlerRuleEntry) any { return r.PolicyRow.Monotonicity }, map[string]any{"missing": []string(nil), "empty_scalar": []string(nil), "empty_sequence": []string{}, "scalar": []string{"policy.low <= policy.high"}, "sequence": []string{"policy.low <= policy.high"}}},
		{"lookup", "on", "entries: [{key: service, value: chosen}]\ninto: computed.choice\n", "payload.kind", "[payload.kind]", "{path: payload.kind}", [8]bool{false, false, false, true, false, true, false, false},
			func(r HandlerRuleEntry) any { return r.Compute.Lookup.On }, map[string]any{"scalar": []string{"payload.kind"}, "sequence": []string{"payload.kind"}}},
		{"lookup", "into", "on: payload.kind\nentries: [{key: service, value: chosen}]\n", "computed.choice", "[computed.choice]", "{path: computed.choice}", scalarOnly,
			func(r HandlerRuleEntry) any { return r.Compute.StoreAs }, map[string]any{"scalar": "computed.choice"}},
		{"lookup", "entries", "on: payload.kind\ninto: computed.choice\n", "service", "[{key: service, value: chosen}]", "{key: service, value: chosen}", [8]bool{false, false, false, false, false, true, false, false},
			func(r HandlerRuleEntry) any { return r.Compute.Lookup.Entries[0].Value }, map[string]any{"sequence": "chosen"}},
		{"lookup", "default", "on: payload.kind\nentries: [{key: service, value: chosen}]\ninto: computed.choice\n", "fail", "[fail]", "{value: fail}", optionalScalar,
			func(r HandlerRuleEntry) any { return r.Compute.Lookup.DefaultDeclared }, map[string]any{"missing": false, "scalar": true}},
		{"validate", "set", "input: {document: payload.document}\ninto: computed.validation.manifest\n", "manifest", "[manifest]", "{name: manifest}", scalarOnly,
			func(r HandlerRuleEntry) any { return r.Compute.Validation.Set }, map[string]any{"scalar": "manifest"}},
		{"validate", "input", "set: manifest\ninto: computed.validation.manifest\n", "payload.document", "[payload.document]", "{document: payload.document}", [8]bool{false, false, false, false, false, false, false, true},
			func(r HandlerRuleEntry) any { return r.Compute.Validation.Input }, map[string]any{"mapping": map[string]string{"document": "payload.document"}}},
		{"validate", "into", "set: manifest\ninput: {document: payload.document}\n", "computed.validation.manifest", "[computed.validation.manifest]", "{path: computed.validation.manifest}", scalarOnly,
			func(r HandlerRuleEntry) any { return r.Compute.StoreAs }, map[string]any{"scalar": "computed.validation.manifest"}},
		{"compute_module", "module", "input: {document: payload.document}\ninto: computed.rendered\n", "render", "[render]", "{name: render}", scalarOnly,
			func(r HandlerRuleEntry) any { return r.Compute.Module.Module }, map[string]any{"scalar": "render"}},
		{"compute_module", "input", "module: render\ninto: computed.rendered\n", "payload.document", "[payload.document]", "{document: payload.document}", [8]bool{false, false, false, false, false, false, false, true},
			func(r HandlerRuleEntry) any { return r.Compute.Module.Input }, map[string]any{"mapping": map[string]string{"document": "payload.document"}}},
		{"compute_module", "into", "module: render\ninput: {document: payload.document}\n", "computed.rendered", "[computed.rendered]", "{path: computed.rendered}", scalarOnly,
			func(r HandlerRuleEntry) any { return r.Compute.StoreAs }, map[string]any{"scalar": "computed.rendered"}},
	} {
		t.Run(tc.kind+"."+tc.field, func(t *testing.T) {
			proveNodePresence(t, nodePresenceShapes(tc.scalar, tc.sequence, tc.mapping), tc.admit,
				func(shape nodePresenceShape) string {
					body := tc.base
					if shape.name != "missing" {
						body += tc.field + ": " + shape.value + "\n"
					}
					return "id: selected\n" + tc.kind + ":\n  " + strings.ReplaceAll(strings.TrimSpace(body), "\n", "\n  ") + "\n"
				}, decodeNodeTestYAML, func(t *testing.T, name string, row HandlerRuleEntry) {
					if !row.Authored() || !reflect.DeepEqual(tc.read(row), tc.want[name]) {
						t.Fatalf("typed %s.%s=%#v want %#v, authored=%t", tc.kind, tc.field, tc.read(row), tc.want[name], row.Authored())
					}
				})
		})
	}
}

func TestW4NestedExpressionFieldPresence(t *testing.T) {
	for _, slot := range []string{"activity.input", "emit.fields", "write.value", "write.key", "write.index"} {
		t.Run(slot, func(t *testing.T) {
			for _, shape := range nodePresenceShapes("7", "[7]", "{value: 7}") {
				t.Run(shape.name, func(t *testing.T) {
					body := ""
					var got ExpressionValue
					var err error
					if strings.HasPrefix(slot, "write.") {
						field := strings.TrimPrefix(slot, "write.")
						body = "op: append\ntarget: entity.rows\n"
						if field == "key" {
							body = "op: delete\ntarget: entity.rows\n"
						}
						if field == "index" {
							body = "op: update\ntarget: entity.rows\nvalue: replacement\n"
						}
						if shape.name != "missing" {
							body += field + ": " + shape.value + "\n"
						}
						var out WorkflowDataWrite
						err = decodeNodeTestYAML([]byte(body), &out)
						switch field {
						case "value":
							got = out.Value
						case "key":
							got = out.Key
						case "index":
							got = out.Index
						}
					} else {
						body = "event: task.done\nfields: {}\n"
						if slot == "activity.input" {
							body = "tool: notify\ninput: {}\n"
						}
						if shape.name != "missing" {
							body = strings.Replace(body, "{}", "{value: "+shape.value+"}", 1)
						}
						if slot == "activity.input" {
							var out ActivitySpec
							err = decodeNodeTestYAML([]byte(body), &out)
							got = out.Input["value"]
						} else {
							var out SystemNodeEventHandler
							err = decodeNodeTestYAML([]byte("emit:\n  "+strings.ReplaceAll(strings.TrimSpace(body), "\n", "\n  ")+"\n"), &out)
							got = out.Emit.Fields["value"]
						}
					}
					wantAdmit := shape.name != "missing" || !strings.HasPrefix(slot, "write.")
					if (err == nil) != wantAdmit {
						t.Fatalf("admission=%v want admitted=%t", err, wantAdmit)
					}
					if err != nil {
						return
					}
					if shape.name == "missing" {
						if !got.IsZero() {
							t.Fatalf("omitted expression became assigned: %#v", got)
						}
						return
					}
					want := map[string]any{"null": nil, "empty_scalar": "", "scalar": 7, "empty_sequence": []any{}, "sequence": []any{7}, "empty_mapping": map[string]any{}, "mapping": map[string]any{"value": 7}}[shape.name]
					if got.Kind != ExpressionKindLiteral || !reflect.DeepEqual(got.Literal, want) {
						t.Fatalf("literal=%#v want %#v; expression=%#v", got.Literal, want, got)
					}
				})
			}
		})
	}
}
