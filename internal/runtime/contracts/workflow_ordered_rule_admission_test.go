package contracts

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestOrderedRuleCollectionsRejectAlternateShapes(t *testing.T) {
	for _, context := range []string{"rules", "on_complete"} {
		for _, tc := range []struct{ name, before, value string }{
			{"empty mapping", "", "{}"},
			{"singleton", "", "{emit: done}"},
			{"keyed", "", "{fallback: {emit: done}}"},
			{"field-word keys", "", "{emit: {emit: done}}"},
			{"null", "", "null"},
			{"scalar", "", "not_a_collection"},
			{"empty scalar", "", "''"},
			{"alias mapping", "_note: &rows {fallback: {emit: done}}\n", "*rows"},
			{"merge mapping", "_note: &rows {fallback: {emit: done}}\n", "{<<: *rows}"},
		} {
			t.Run(context+"/"+tc.name, func(t *testing.T) {
				_, err := ruleTestHandler(tc.before + context + ": " + tc.value + "\n")
				code := "DIALECT-RULES-ORDER"
				if context == "on_complete" {
					code = "DIALECT-OC-ORDER"
				}
				for _, want := range []string{code, context, "ordered list", "nodes.yaml:", `["` + context + `"]`} {
					if err == nil || !strings.Contains(err.Error(), want) {
						t.Fatalf("collection %s: %v, want %q", tc.value, err, want)
					}
				}
				if context == "rules" {
					for _, want := range []string{"evaluated top to bottom", "    - when: payload.kind == 'a'", "      emit: routed.a", "    - else: true", "rows are identified by position"} {
						if !strings.Contains(err.Error(), want) {
							t.Fatalf("collection teaching lost the ruled ordered example %q: %v", want, err)
						}
					}
				}
			})
		}
	}
}

func TestOrderedRuleRowsRejectAuthoredNamesOnPresence(t *testing.T) {
	for _, context := range []string{"rules", "on_complete"} {
		for _, key := range []string{"id", "description"} {
			for _, value := range []string{"name", "''", "'  '", "null", "[]", "{}", "&name 'display'", "*name"} {
				t.Run(context+"/"+key+"/"+value, func(t *testing.T) {
					before := "_note: &name display\n"
					_, err := ruleTestHandler(before + fmt.Sprintf("%s:\n  - %s: %s\n    emit: done\n", context, key, value))
					assertRetiredRuleNameDiagnostic(t, err, context, key)
				})
			}
			for _, shape := range []string{"alias", "merge"} {
				t.Run(context+"/"+key+"/"+shape, func(t *testing.T) {
					row := "*row"
					if shape == "merge" {
						row = "{<<: *row}"
					}
					_, err := ruleTestHandler(fmt.Sprintf("_note: &row {%s: null, emit: done}\n%s: [%s]\n", key, context, row))
					assertRetiredRuleNameDiagnostic(t, err, context, key)
				})
			}
		}
	}
}

func assertRetiredRuleNameDiagnostic(t *testing.T, err error, context, key string) {
	t.Helper()
	var diagnostic *LoaderDiagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "contract_loader.undefined_field" ||
		!strings.Contains(diagnostic.Problem, `field "`+key+`" is not supported`) ||
		!strings.Contains(diagnostic.Location.YAMLPath, `["`+key+`"]`) ||
		!slices.Contains(diagnostic.ValidOptions, "emit") || slices.Contains(diagnostic.ValidOptions, "id") || slices.Contains(diagnostic.ValidOptions, "description") {
		t.Fatalf("%s.%s must fail on presence at the vocabulary owner: %v", context, key, err)
	}
	for _, want := range []string{`["` + context + `"][0]`, "nodes.yaml:", "emit"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("%s.%s diagnostic lost owning path/source/vocabulary %q: %v", context, key, want, err)
		}
	}
}

func TestOrderedRuleCollectionsPreserveEmptyAliasesAndBankedRows(t *testing.T) {
	for _, context := range []string{"rules", "on_complete"} {
		for _, source := range []string{
			context + ": []\n",
			"_note: &rows []\n" + context + ": *rows\n",
		} {
			handler, err := ruleTestHandler(source)
			if err != nil || len(handler.Rules) != 0 || len(handler.OnComplete) != 0 {
				t.Fatalf("empty %s collection changed: %#v, %v", context, handler, err)
			}
		}
	}
	handler, err := ruleTestHandler("_note: &rows [{emit: first}, {default: true, emit: second}]\nrules: *rows\n")
	if err != nil || len(handler.Rules) != 2 || handler.Rules[0].Condition != "" || handler.Rules[0].DisplayLabel("rules", 0) != "rules[0]" ||
		handler.Rules[0].Emit.EventType() != "first" || handler.Rules[1].PolicyRow.Kind != PolicySheetRowKindDefault || handler.Rules[1].Emit.EventType() != "second" {
		t.Fatalf("collection cut changed predicate-less/default alias/order semantics: %#v, %v", handler, err)
	}
}
