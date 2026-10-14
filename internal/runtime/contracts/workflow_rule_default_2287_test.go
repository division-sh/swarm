package contracts

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestRuleDefaultRetiredFieldsRejectOnPresence(t *testing.T) {
	for _, key := range []string{"else", "default"} {
		for _, value := range []string{"true", "false", "null", "''", "0", "[]", "{}"} {
			for _, shape := range []string{"direct", "alias", "merge"} {
				t.Run(key+"/"+value+"/"+shape, func(t *testing.T) {
					row := fmt.Sprintf("{%s: %s, emit: done}", key, value)
					source := "rules: [" + row + "]\n"
					if shape != "direct" {
						source = "_note: &row " + row + "\nrules: [*row]\n"
						if shape == "merge" {
							source = "_note: &row " + row + "\nrules: [{<<: *row}]\n"
						}
					}
					_, err := ruleTestHandler(source)
					diagnostic, ok := AsLoaderDiagnostic(err)
					if !ok || diagnostic.Code != "contract_loader.undefined_field" ||
						!strings.Contains(diagnostic.Problem, `field "`+key+`" is not supported`) ||
						!strings.Contains(diagnostic.Location.YAMLPath, `["rules"][0]["`+key+`"]`) ||
						!strings.Contains(err.Error(), "DIALECT-RULES-DEFAULT") ||
						!strings.Contains(err.Error(), "last row written without a predicate") ||
						slices.Contains(diagnostic.ValidOptions, key) {
						t.Fatalf("retired field must fail at its exact unknown-key boundary: %v", err)
					}
				})
			}
		}
	}
}

func TestRuleDefaultRejectsUnreachableAndEmptyRows(t *testing.T) {
	for _, tc := range []struct{ name, source, want string }{
		{"nonlast", "rules: [{emit: first}, {when: true, emit: second}]", "rules[0]"},
		{"two defaults", "rules: [{emit: first}, {emit: second}]", "unreachable"},
		{"before value row", "rules: [{emit: done}, {compute_module: {module: renderer, input: {}, into: computed.value}}]", "unreachable"},
		{"aliased nonlast", "_note: &row {emit: first}\nrules: [*row, {when: true}]", "unreachable"},
		{"merged nonlast", "_note: &row {emit: first}\nrules: [{<<: *row}, {when: true}]", "unreachable"},
		{"empty mapping", "rules: [{}]", "is empty"},
		{"empty emit", "rules: [{emit: null}]", "is empty"},
		{"empty writes", "rules: [{data_accumulation: {writes: []}}]", "is empty"},
		{"empty advance", "rules: [{advances_to: ''}]", "is empty"},
		{"blank advance", "rules: [{advances_to: '  '}]", "is empty"},
		{"empty compute", "rules: [{compute: {}}]", "is empty"},
		{"compute metadata only", "rules: [{compute: {description: inert}}]", "is empty"},
		{"activity name only", "rules: [{activity: {id: explicit_but_inert}}]", "is empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ruleTestHandler(tc.source)
			for _, want := range []string{"DIALECT-RULES-DEFAULT", "nodes.yaml:", tc.want} {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("default admission must name the exact cause %q: %v", want, err)
				}
			}
		})
	}
}

func TestRuleDefaultDerivesKindOnlyForTrailingSelection(t *testing.T) {
	for _, effect := range []string{
		"emit: done", "advances_to: done", "{advances_to: done, terminate: true}",
		"activity: {id: explicit_send, tool: send}",
		"data_accumulation: {writes: [{target_field: count, value: 1}]}",
		"compute: {operation: pick_or_average, keys: {numeric_keys: [score]}, store_as: computed.score}",
		"fan_out: {items_from: payload.items, as: element, identity: element, emit: done}",
	} {
		t.Run(effect, func(t *testing.T) {
			handler, err := ruleTestHandler("rules:\n  - when: false\n  - " + effect + "\n")
			if err != nil {
				t.Fatal(err)
			}
			row := handler.Rules[1]
			if row.Condition != "" || row.PolicyRow.Kind != PolicySheetRowKindDefault || row.DisplayLabel("rules", 1) != "rules[1]: default" {
				t.Fatalf("default must derive from trailing structure, never a CEL sentinel: %#v", row)
			}
		})
	}
	for _, source := range []string{
		"rules: [{when: false}]",
		"rules: [{when: false, emit: done}]",
		"rules: [{compute_module: {module: renderer, input: {text: payload.text}, into: computed.value}}]",
		"rules: [{lookup: {on: payload.kind, entries: [{key: a, value: found}], into: computed.value, default: fail}}]",
	} {
		handler, err := ruleTestHandler(source)
		if err != nil || len(handler.Rules) != 1 || handler.Rules[0].PolicyRow.Kind == PolicySheetRowKindDefault {
			t.Fatalf("predicate observers and value-only rows do not require or become defaults: %#v, %v", handler, err)
		}
	}
}

func TestRuleDefaultPreservesPolicySheetRequirement(t *testing.T) {
	for _, predicate := range []string{"case: {selector: payload.kind, equals: a}", "range: {value: payload.score, gte: 0, lt: 10}"} {
		_, err := ruleTestHandler("rules:\n  - " + predicate + "\n    emit: matched\n")
		if err == nil || !strings.Contains(err.Error(), "must end with a predicate-less default row") {
			t.Fatalf("policy-sheet default requirement was lost: %v", err)
		}
		handler, err := ruleTestHandler("rules:\n  - " + predicate + "\n    emit: matched\n  - emit: fallback\n")
		if err != nil || handler.Rules[1].PolicyRow.Kind != PolicySheetRowKindDefault {
			t.Fatalf("canonical policy-sheet default rejected: %#v, %v", handler, err)
		}
	}
	for _, predicate := range []string{"else", "ELSE", "Else"} {
		handler, err := ruleTestHandler("rules: [{when: " + predicate + ", emit: done}]")
		if err != nil || handler.Rules[0].Condition != predicate || handler.Rules[0].PolicyRow.Kind != PolicySheetRowKindWhen {
			t.Fatalf("source classification must leave ordinary CEL validation to its owner: %#v, %v", handler, err)
		}
	}
	handler, err := ruleTestHandler("on_complete: [{emit: first}, {condition: false, emit: second}]")
	if err != nil || len(handler.OnComplete) != 2 || handler.OnComplete[0].PolicyRow.Kind != "" {
		t.Fatalf("selection default rule must not change completion context: %#v, %v", handler, err)
	}
}
