package contracts

import (
	"fmt"
	"strings"
	"testing"
)

func TestScalar2556EveryPredicateRejectsDefaultSentinel(t *testing.T) {
	for _, tc := range []struct{ slot, body string }{
		{"rule.when", "rules:\n  - when: %s\n  - else: true\n"},
		{"on_complete.condition", "on_complete:\n  - condition: %s\n    emit: done\n"},
		{"guard.check", "guard:\n  check: %s\n"},
		{"guard.checks.check", "guard:\n  checks:\n    - check: %s\n"},
		{"query.filter", "query:\n  entities: account\n  filter: %s\n"},
		{"filter.condition", "filter:\n  items_from: payload.items\n  condition: %s\n"},
		{"count.condition", "count:\n  items_from: payload.items\n  condition: %s\n"},
	} {
		t.Run(tc.slot, func(t *testing.T) {
			for _, sentinel := range []string{"else", "ELSE", "eLsE"} {
				body := fmt.Sprintf(tc.body, sentinel)
				for _, source := range []string{body, "_note: &marker " + sentinel + "\n" + fmt.Sprintf(tc.body, "*marker"), "<<:\n  " + strings.ReplaceAll(strings.TrimSuffix(body, "\n"), "\n", "\n  ") + "\n"} {
					var handler SystemNodeEventHandler
					err := decodeNodeTestYAML([]byte(source), &handler)
					if err == nil || !strings.Contains(err.Error(), tc.slot+" must be a CEL predicate") || !strings.Contains(err.Error(), "nodes.yaml:") {
						t.Fatalf("sentinel admitted or wrong diagnostic for %q: %v", source, err)
					}
				}
			}
			for _, native := range []string{"true", "false"} {
				var handler SystemNodeEventHandler
				if err := decodeNodeTestYAML([]byte(fmt.Sprintf(tc.body, native)), &handler); err != nil {
					t.Fatalf("native boolean rejected: %v", err)
				}
			}
		})
	}
}

func TestScalar2556TypedDefaultsAndCompletionOmissionPreserved(t *testing.T) {
	for _, field := range []string{"else", "default"} {
		var handler SystemNodeEventHandler
		if err := decodeNodeTestYAML([]byte("rules:\n  - "+field+": true\n    emit: done\non_complete:\n  - emit: completed\n"), &handler); err != nil {
			t.Fatal(err)
		}
		if len(handler.Rules) != 1 || handler.Rules[0].PolicyRow.Kind != PolicySheetRowKindDefault || handler.Rules[0].Condition != "else" || handler.OnComplete[0].Condition != "" {
			t.Fatalf("internal default or omitted completion changed: %#v", handler)
		}
	}
}
