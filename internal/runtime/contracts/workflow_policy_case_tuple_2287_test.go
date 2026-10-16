package contracts

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestPolicyCaseRepeatedSelectorWithinTupleIsRefused2287(t *testing.T) {
	for _, entrance := range []string{"projector", "handler", "bundle"} {
		t.Run(entrance, func(t *testing.T) {
			for _, repeated := range []bool{false, true} {
				second := "payload.target"
				if repeated {
					second = "payload.mode"
				}
				row := "selectors: [payload.mode, " + second + "], equals: [gold, gold]"
				var err error
				switch entrance {
				case "projector":
					snapshot, loadErr := yamlsource.Load([]byte("{" + row + "}\n"))
					if loadErr != nil {
						t.Fatal(loadErr)
					}
					_, _, err = projectNodePolicyCaseValue(snapshot.Document("nodes.yaml").Root())
				case "handler":
					var handler SystemNodeEventHandler
					err = decodeNodeTestYAML([]byte("rules:\n  - case: {"+row+"}\n    emit: task.done\n  - emit: task.other\n"), &handler)
				case "bundle":
					root := t.TempDir()
					writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: case_tuple\nstages: {done: {final: true}}\n")
					writeFixtureFile(t, filepath.Join(root, "events.yaml"), "task.ready:\n  mode: text\n  target: text\ntask.done:\ntask.other:\n")
					writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  event_handlers:\n    task.ready:\n      rules:\n        - case: {"+row+"}\n          emit: task.done\n        - emit: task.other\n")
					repo := repoRootForContractsTest(t)
					_, err = LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
				}
				if !repeated {
					if err != nil {
						t.Fatal("unique-selector positive control refused", err)
					}
					continue
				}
				if err == nil || !strings.Contains(err.Error(), "duplicate case pair at positions 1 and 2") || !strings.Contains(err.Error(), ":") {
					t.Fatalf("repeated-selector tuple admitted instead of duplicate refusal: %v", err)
				}
			}
		})
	}
}

func TestPolicyCaseCanonicalShapesAndRefusals2287(t *testing.T) {
	for _, tc := range []struct {
		name, row, condition, refusal string
	}{
		{"singular", "selector: payload.mode, equals: gold", `payload.mode == "gold"`, ""},
		{"plural", "selectors: [payload.mode, payload.target], equals: [gold, blue]", `payload.mode == "gold" && payload.target == "blue"`, ""},
		{"typed", `selectors: [payload.ready, payload.count, payload.mode], equals: [true, 1, "1"]`, `payload.ready == true && payload.count == 1 && payload.mode == "1"`, ""},
		{"empty_text_literal", `selector: payload.mode, equals: ""`, `payload.mode == ""`, ""},
		{"different_literals", "selectors: [payload.mode, payload.mode], equals: [gold, blue]", `payload.mode == "gold" && payload.mode == "blue"`, ""},
		{"different_selectors", "selectors: [payload.mode, payload.target], equals: [gold, gold]", `payload.mode == "gold" && payload.target == "gold"`, ""},
		{"text_boolean", `selectors: [payload.mode, payload.mode], equals: ["true", true]`, `payload.mode == "true" && payload.mode == true`, ""},
		{"text_number", `selectors: [payload.mode, payload.mode], equals: ["1", 1]`, `payload.mode == "1" && payload.mode == 1`, ""},
		{"numeric_spelling", "selectors: [payload.count, payload.count], equals: [1, 1.0]", "payload.count == 1 && payload.count == 1.0", ""},
		{"order", "selectors: [payload.target, payload.mode], equals: [blue, gold]", `payload.target == "blue" && payload.mode == "gold"`, ""},
		{"singleton", "selectors: [payload.mode], equals: [gold]", "", "one selector: write `selector:` and a scalar `equals:`"},
		{"singular_list", "selector: payload.mode, equals: [gold]", "", "case is either singular (selector/equals) or plural (selectors/equals), not mixed"},
		{"plural_scalar", "selectors: [payload.mode, payload.target], equals: gold", "", "case is either singular (selector/equals) or plural (selectors/equals), not mixed"},
		{"scalar_selectors", "selectors: payload.mode, equals: gold", "", "`selectors` is a list; for one selector write `selector`"},
		{"blank_member", `selectors: [payload.mode, " ", payload.target], equals: [gold, blue]`, "", "must be non-empty"},
		{"both_keys", "selector: payload.mode, selectors: [payload.target, payload.mode], equals: [gold, blue]", "", "declares both selector and selectors"},
		{"missing_selector", "equals: gold", "", "requires selector or selectors"},
		{"missing_equals", "selector: payload.mode", "", "requires equals"},
		{"empty_lists", "selectors: [], equals: []", "", "requires a non-empty list"},
		{"null_selector", "selector: null, equals: gold", "", "must be a scalar"},
		{"null_selectors", "selectors: null, equals: [gold, blue]", "", "sequence"},
		{"null_equals", "selector: payload.mode, equals: null", "", "must be a scalar"},
		{"null_member", "selectors: [payload.mode, payload.target], equals: [gold, null]", "", "must be a scalar"},
		{"mapping_member", "selectors: [payload.mode, {path: payload.target}], equals: [gold, blue]", "", "must be a scalar"},
		{"mapping_equals", "selector: payload.mode, equals: {value: gold}", "", "must be a scalar"},
		{"arity", "selectors: [payload.mode, payload.target], equals: [gold]", "", "selector count 2 does not match equals count 1"},
		{"invalid_path", "selector: computed.mode, equals: gold", "", "unsupported root"},
		{"expression_path", "selector: payload.mode(), equals: gold", "", "simple dotted path"},
		{"adjacent_pair", "selectors: [payload.mode, payload.mode], equals: [gold, gold]", "", "duplicate case pair at positions 1 and 2"},
		{"nonadjacent_pair", "selectors: [payload.mode, payload.target, payload.mode], equals: [gold, blue, gold]", "", "duplicate case pair at positions 1 and 3"},
		{"normalized_pair", `selectors: [payload.mode, " payload.mode "], equals: [gold, gold]`, "", "duplicate case pair at positions 1 and 2"},
		{"duplicate_key", "selector: payload.mode, selector: payload.target, equals: gold", "", "duplicate"},
		{"missing_alias", "selector: *missing, equals: gold", "", "alias"},
		{"invalid_merge", "<<: 1, selector: payload.mode, equals: gold", "", "merge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte("{" + tc.row + "}\n"))
			var condition string
			if err == nil {
				condition, _, err = projectNodePolicyCaseValue(snapshot.Document("nodes.yaml").Root())
			}
			if tc.refusal != "" {
				if err == nil || !strings.Contains(err.Error(), tc.refusal) || !strings.Contains(err.Error(), ":") {
					t.Fatalf("want located refusal %q, got %v", tc.refusal, err)
				}
				return
			}
			if err != nil || condition != tc.condition {
				t.Fatalf("condition=%q want %q, error=%v", condition, tc.condition, err)
			}
		})
	}
}

func TestPolicyCaseBundleShapeAdmission2287(t *testing.T) {
	for _, tc := range []struct {
		name, row, refusal string
	}{
		{"singular", "selector: payload.mode, equals: gold", ""},
		{"plural", "selectors: [payload.mode, payload.target], equals: [gold, blue]", ""},
		{"typed", "selectors: [payload.mode, payload.ready, payload.count], equals: [gold, true, 1]", ""},
		{"different_values", "selectors: [payload.mode, payload.mode], equals: [gold, blue]", ""},
		{"singleton", "selectors: [payload.mode], equals: [gold]", "one selector: write `selector:` and a scalar `equals:`"},
		{"mixed", "selector: payload.mode, equals: [gold]", "case is either singular (selector/equals) or plural (selectors/equals), not mixed"},
		{"plural_scalar", "selectors: [payload.mode, payload.target], equals: gold", "case is either singular (selector/equals) or plural (selectors/equals), not mixed"},
		{"scalar_selectors", "selectors: payload.mode, equals: gold", "`selectors` is a list; for one selector write `selector`"},
		{"duplicate", "selectors: [payload.mode, payload.mode], equals: [gold, gold]", "duplicate case pair"},
		{"blank", `selectors: [payload.mode, ""], equals: [gold, blue]`, "must be non-empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: case_shape\nstages: {done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, "events.yaml"), "task.ready:\n  mode: text\n  target: text\n  ready: boolean\n  count: integer\ntask.done:\ntask.other:\n")
			writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  event_handlers:\n    task.ready:\n      rules:\n        - case: {"+tc.row+"}\n          emit: task.done\n        - emit: task.other\n")
			repo := repoRootForContractsTest(t)
			_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if tc.refusal != "" {
				if err == nil || !strings.Contains(err.Error(), tc.refusal) || !strings.Contains(err.Error(), "nodes.yaml:") {
					t.Fatalf("want located refusal %q, got %v", tc.refusal, err)
				}
			} else if err != nil {
				t.Fatalf("canonical typed bundle refused: %v", err)
			}
		})
	}
}

func TestPolicyCaseTupleOrderAndSiblingLists2287(t *testing.T) {
	var handler SystemNodeEventHandler
	err := decodeNodeTestYAML([]byte(`rules:
  - case: {selectors: [payload.mode, payload.target], equals: [gold, blue]}
    emit: task.done
  - case: {selectors: [payload.target, payload.mode], equals: [blue, gold]}
    emit: task.other
  - emit: task.fallback
`), &handler)
	if err != nil || len(handler.Rules) != 3 {
		t.Fatalf("ordered tuples refused: %v", err)
	}
	if !reflect.DeepEqual(handler.Rules[1].PolicyRow.Selectors, []string{"payload.target", "payload.mode"}) ||
		!reflect.DeepEqual(handler.Rules[1].PolicyRow.CaseValues, []string{"blue", "gold"}) ||
		handler.Rules[1].Condition != `payload.target == "blue" && payload.mode == "gold"` ||
		!strings.HasPrefix(handler.Rules[1].DisplayLabel(), "rules[1]: case") {
		t.Fatalf("tuple order, identity or presentation changed: %#v", handler.Rules)
	}
	for _, spelling := range []string{"policy.low <= policy.high", "['policy.low <= policy.high']"} {
		var row HandlerRuleEntry
		err := decodeNodeTestYAML([]byte("range: {value: payload.score, gte: 0, monotonicity: "+spelling+"}\n"), &row)
		if err != nil || !reflect.DeepEqual(row.PolicyRow.Monotonicity, []string{"policy.low <= policy.high"}) {
			t.Fatalf("range sibling changed: %#v %v", row.PolicyRow, err)
		}
	}
	for _, spelling := range []string{"payload.mode", "[payload.mode]"} {
		var row HandlerRuleEntry
		err := decodeNodeTestYAML([]byte("lookup: {on: "+spelling+", entries: [{key: gold, value: chosen}], into: computed.choice, default: fail}\n"), &row)
		if err != nil || !reflect.DeepEqual(row.Compute.Lookup.On, []string{"payload.mode"}) {
			t.Fatalf("lookup sibling changed: %#v %v", row.Compute, err)
		}
	}
}
