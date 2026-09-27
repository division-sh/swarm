package contracts

import (
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestHandlerRulesConditionRetiredInEveryAuthoredShape(t *testing.T) {
	cases := map[string]string{
		"list":           "rules:\n  - condition: payload.ready\n",
		"empty":          "rules:\n  - condition: ''\n",
		"null":           "rules:\n  - condition: null\n",
		"default":        "rules:\n  - condition: else\n",
		"singleton":      "rules:\n  condition: payload.ready\n",
		"singleton_list": "rules:\n  condition: []\n",
		"keyed":          "rules:\n  ready:\n    condition: payload.ready\n",
		"mixed":          "rules:\n  - when: payload.ready\n    condition: payload.other\n  - else: true\n",
		"duplicate":      "rules:\n  - condition: payload.ready\n    condition: payload.other\n",
		"alias":          "template: &old {condition: payload.ready}\nrules:\n  - *old\n",
		"merge":          "rules:\n  - <<: &old {condition: payload.ready}\n",
	}
	for name, source := range cases {
		t.Run(name, func(t *testing.T) {
			var document yaml.Node
			if err := yaml.Unmarshal([]byte(source), &document); err != nil {
				t.Fatal(err)
			}
			root := document.Content[0]
			var rules *yaml.Node
			for i := 0; i+1 < len(root.Content); i += 2 {
				if root.Content[i].Value == "rules" {
					rules = root.Content[i+1]
					break
				}
			}
			if rules == nil {
				t.Fatal("missing rules test fixture")
			}
			_, err := decodeHandlerRuleEntriesNode(rules, handlerRuleDecodeContextRules)
			if err == nil || !strings.Contains(err.Error(), "RETIRED-POLICY-SHEET-ROW") || !strings.Contains(err.Error(), "use when") {
				t.Fatalf("condition admission error = %v, want teaching retirement", err)
			}
		})
	}
}

func TestHandlerRulesConditionCanBeKeyedDisplayLabel(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte("rules:\n  condition:\n    when: payload.ready\n  fallback:\n    else: true\n"), &handler); err != nil {
		t.Fatal(err)
	}
	if len(handler.Rules) != 2 || handler.Rules[0].ID != "condition" || handler.Rules[0].PolicyRow.Kind != PolicySheetRowKindWhen {
		t.Fatalf("keyed rule label changed: %#v", handler.Rules)
	}
	var document yaml.Node
	if err := yaml.Unmarshal([]byte("rules:\n  condition: {when: payload.ready}\n  malformed: []\n"), &document); err != nil {
		t.Fatal(err)
	}
	_, err := decodeHandlerRuleEntriesNode(document.Content[0].Content[1], handlerRuleDecodeContextRules)
	if err == nil || strings.Contains(err.Error(), "RETIRED-POLICY-SHEET-ROW") {
		t.Fatalf("keyed display label was mistaken for predicate: %v", err)
	}
}

func TestHandlerRulesWhenOwnsPolicyPredicateAndFallback(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte("rules:\n  - when: payload.ready\n    emit: work.ready\n  - else: true\n    emit: work.skipped\n"), &handler); err != nil {
		t.Fatal(err)
	}
	if len(handler.Rules) != 2 || handler.Rules[0].Condition != "payload.ready" || handler.Rules[0].PolicyRow.Kind != PolicySheetRowKindWhen || handler.Rules[1].Condition != "else" || handler.Rules[1].PolicyRow.Kind != PolicySheetRowKindDefault {
		t.Fatalf("compiled policy rows = %#v", handler.Rules)
	}
	if err := yaml.Unmarshal([]byte("rules:\n  - when: payload.ready\n    emit: work.ready\n"), &handler); err == nil || !strings.Contains(err.Error(), "require an else/default row") {
		t.Fatalf("missing typed fallback error = %v", err)
	}
}

func TestHandlerRulesWhenRejectsDefaultSentinel(t *testing.T) {
	cases := map[string]string{
		"list_lower_no_fallback":        "rules:\n  - when: else\n    emit: work.ready\n",
		"list_upper_with_fallback":      "rules:\n  - when: ELSE\n    emit: work.ready\n  - else: true\n",
		"list_whitespace_with_fallback": "rules:\n  - when: '  Else  '\n    emit: work.ready\n  - else: true\n",
		"singleton":                     "rules:\n  when: else\n",
		"keyed":                         "rules:\n  bad:\n    when: else\n  fallback:\n    else: true\n",
		"alias":                         "bad: &bad {when: else}\nrules:\n  - *bad\n  - else: true\n",
		"merge":                         "bad: &bad {when: else}\nrules:\n  - <<: *bad\n  - else: true\n",
	}
	for name, source := range cases {
		t.Run(name, func(t *testing.T) {
			var document yaml.Node
			if err := yaml.Unmarshal([]byte(source), &document); err != nil {
				t.Fatal(err)
			}
			root := document.Content[0]
			var rules *yaml.Node
			for i := 0; i+1 < len(root.Content); i += 2 {
				if root.Content[i].Value == "rules" {
					rules = root.Content[i+1]
					break
				}
			}
			if rules == nil {
				t.Fatal("missing rules test fixture")
			}
			_, err := decodeHandlerRuleEntriesNode(rules, handlerRuleDecodeContextRules)
			if err == nil {
				t.Fatal("when default sentinel was admitted")
			}
			if name != "merge" && !strings.Contains(err.Error(), "when must be a CEL predicate") {
				t.Fatalf("when sentinel error = %v, want typed predicate rejection", err)
			}
		})
	}
}

func TestPolicySheetFallbackRequiresDefaultKind(t *testing.T) {
	rules := []HandlerRuleEntry{
		{Condition: "false", PolicyRow: PolicySheetRowMetadata{Kind: PolicySheetRowKindWhen}},
		{Condition: "else"},
	}
	if err := validatePolicySheetRows(rules, handlerRuleDecodeContextRules); err == nil || !strings.Contains(err.Error(), "require an else/default row") {
		t.Fatalf("untyped sentinel counted as fallback: %v", err)
	}
	rules[1].PolicyRow.Kind = PolicySheetRowKindDefault
	if err := validatePolicySheetRows(rules, handlerRuleDecodeContextRules); err != nil {
		t.Fatalf("typed default rejected: %v", err)
	}
}

func TestBundleAdmissionRejectsWhenDefaultSentinel(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: rule-sentinel-proof\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "proof.requested: {}\n")
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  execution_type: system_node\n  event_handlers:\n    proof.requested:\n      rules:\n        - when: else\n        - else: true\n")
	repoRoot := contractRepoRoot(t)
	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "when must be a CEL predicate") {
		t.Fatalf("sentinel bundle admission = %v, want predicate teaching rejection", err)
	}
}

func TestRulePredicateContextsRemainDistinct(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte("on_complete:\n  - condition: payload.ready\n    advances_to: done\n"), &handler); err != nil {
		t.Fatalf("ordinary completion condition: %v", err)
	}
	if len(handler.OnComplete) != 1 || handler.OnComplete[0].Condition != "payload.ready" || handler.OnComplete[0].PolicyRow.Kind != "" {
		t.Fatalf("completion rules = %#v", handler.OnComplete)
	}
	if err := yaml.Unmarshal([]byte("on_complete:\n  - when: payload.ready\n    advances_to: done\n"), &handler); err == nil || !strings.Contains(err.Error(), "only supported under handler.rules") {
		t.Fatalf("on_complete.when error = %v", err)
	}
	for _, spelling := range []string{"condition", "when"} {
		t.Run("join-"+spelling, func(t *testing.T) {
			var join JoinSpec
			err := yaml.Unmarshal([]byte("on_complete:\n  "+spelling+": payload.ready\n"), &join)
			if err == nil || !strings.Contains(err.Error(), spelling) {
				t.Fatalf("join %s error = %v", spelling, err)
			}
		})
		t.Run("join-timeout-"+spelling, func(t *testing.T) {
			var join JoinSpec
			err := yaml.Unmarshal([]byte("timeout:\n  after: 1h\n  "+spelling+": payload.ready\n"), &join)
			if err == nil || !strings.Contains(err.Error(), spelling) {
				t.Fatalf("join timeout %s error = %v", spelling, err)
			}
		})
	}
}

func TestBundleAdmissionRejectsRetiredRulePredicateAndCompilesWhen(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: rule-predicate-proof\n")
	writeFixtureFile(t, filepath.Join(root, "entities.yaml"), "proof: {}\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "proof.requested: {}\n")
	nodes := filepath.Join(root, "nodes.yaml")
	writeFixtureFile(t, nodes, "worker:\n  execution_type: system_node\n  event_handlers:\n    proof.requested:\n      rules:\n        - id: selected\n          condition: 'true'\n        - else: true\n")
	repoRoot := contractRepoRoot(t)
	if _, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot)); err == nil || !strings.Contains(err.Error(), "RETIRED-POLICY-SHEET-ROW") {
		t.Fatalf("retired bundle admission = %v", err)
	}
	writeFixtureFile(t, nodes, "worker:\n  execution_type: system_node\n  event_handlers:\n    proof.requested:\n      rules:\n        - id: selected\n          when: 'true'\n        - else: true\n")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	handler := bundle.Nodes["worker"].EventHandlers["proof.requested"]
	if len(handler.Rules) != 2 || handler.Rules[0].PolicyRow.Kind != PolicySheetRowKindWhen || handler.Rules[1].PolicyRow.Kind != PolicySheetRowKindDefault {
		t.Fatalf("typed bundle rule plan = %#v", handler.Rules)
	}
}
