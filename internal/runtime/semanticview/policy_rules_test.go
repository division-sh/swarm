package semanticview

import (
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

func r3PolicyRulesBundle() *c.WorkflowContractBundle {
	root := &c.FlowContractView{Path: "root", Paths: c.FlowContractPaths{FlowPath: "root"}, Policy: c.PolicyDocument{Values: map[string]c.PolicyValue{
		"config": {Value: map[string]any{"parent_only": true, "nested": []any{map[string]any{"value": 1}}}}, "": {Value: false}, " spaced ": {Value: 0},
	}}, Rules: c.RulesDocument{"review": {Criteria: &c.PolicyCriteriaSet{Rules: []c.PolicyCriteriaRule{{ID: "parent"}}}}}, Children: []c.FlowContractView{
		{Path: "root/child", Paths: c.FlowContractPaths{FlowPath: "child"}, Policy: c.PolicyDocument{Values: map[string]c.PolicyValue{"config": {Value: map[string]any{"child_only": true}}}}, Rules: c.RulesDocument{"review": {Validation: &c.PolicyValidationSet{Rules: []c.PolicyValidationRule{{ID: "child"}}}}}},
		{Path: "root/sibling", Paths: c.FlowContractPaths{FlowPath: "sibling"}},
	}}
	return &c.WorkflowContractBundle{FlowTree: c.FlowTree{Root: root, ByID: map[string]*c.FlowContractView{"root": root, "child": &root.Children[0], "sibling": &root.Children[1]}}}
}

func TestR3PolicyWholeObjectShadowOwner(t *testing.T) {
	for _, replacement := range []any{nil, false, 0, map[string]any{}, map[string]any{"child_only": true}} {
		bundle := r3PolicyRulesBundle()
		bundle.FlowTree.Root.Children[0].Policy.Values["config"] = c.PolicyValue{Value: replacement}
		source := Wrap(bundle)
		if _, ok := PolicyValueForFlow(source, "child", "config.parent_only"); ok {
			t.Fatal("ordinary lookup resurrected parent")
		}
		if _, ok := PolicyValueForFlowWithOwner(source, "child", "config.parent_only"); ok {
			t.Fatal("owner lookup resurrected parent")
		}
		resolved, ok := PolicyValueForFlowWithOwner(source, "child", "config")
		if !ok || resolved.OwnerKey != "flow:child" {
			t.Fatalf("child root owner=%#v %t", resolved, ok)
		}
		resolved, ok = PolicyValueForFlowWithOwner(source, "sibling", "config.parent_only")
		if !ok || resolved.Value.Value != true || resolved.OwnerKey != "flow:root" {
			t.Fatalf("sibling owner=%#v %t", resolved, ok)
		}
	}
}

func TestR3PolicyImmutableCloneAndPreview(t *testing.T) {
	bundle := r3PolicyRulesBundle()
	source := Wrap(bundle)
	resolved := source.ResolvedPolicyForFlow("sibling")
	resolved.Values["config"].Value.(map[string]any)["nested"].([]any)[0].(map[string]any)["value"] = 2
	if bundle.FlowTree.Root.Policy.Values["config"].Value.(map[string]any)["nested"].([]any)[0].(map[string]any)["value"] != 1 {
		t.Fatal("resolved view aliases original")
	}
	owned, ok := PolicyValueForFlowWithOwner(source, "sibling", "config")
	if !ok {
		t.Fatal("owner lookup failed")
	}
	owned.Value.Value.(map[string]any)["nested"].([]any)[0].(map[string]any)["value"] = 9
	if bundle.FlowTree.Root.Policy.Values["config"].Value.(map[string]any)["nested"].([]any)[0].(map[string]any)["value"] != 1 {
		t.Fatal("owner-bearing read aliases original")
	}
	scope, ok := source.FlowScopeByID("root")
	if !ok {
		t.Fatal("scope lookup failed")
	}
	scope.Rules["review"].Criteria.Rules[0].ID = "changed"
	if bundle.FlowTree.Root.Rules["review"].Criteria.Rules[0].ID != "parent" {
		t.Fatal("scope rule aliases declaration")
	}
	override := map[string]any{"": map[string]any{"nested": []any{map[string]any{"value": 3}}}, " spaced ": false}
	preview := CloneBundleForPreview(bundle, override)
	override[""].(map[string]any)["nested"].([]any)[0].(map[string]any)["value"] = 4
	previewValue := Wrap(preview).ResolvedPolicyForFlow("child")
	if previewValue.Values[""].Value.(map[string]any)["nested"].([]any)[0].(map[string]any)["value"] != 3 {
		t.Fatal("caller-owned override aliases preview")
	}
	if previewValue.Values[" spaced "].Value != false || bundle.FlowTree.Root.Policy.Values[" spaced "].Value != 0 {
		t.Fatal("exact key or original value changed")
	}
	preview.FlowTree.Root.Rules["review"].Criteria.Rules[0].ID = "changed"
	if bundle.FlowTree.Root.Rules["review"].Criteria.Rules[0].ID != "parent" {
		t.Fatal("preview rule aliases original")
	}
}

func TestR3RuleLocalDuplicateAndWrongKindShadow(t *testing.T) {
	bundle := r3PolicyRulesBundle()
	source := Wrap(bundle)
	rules := source.ResolvedRulesForFlow("child")
	if _, ok := rules.Criteria("review"); ok {
		t.Fatal("nearest wrong-kind bypassed through ancestor")
	}
	if set, ok := rules.Validation("review"); !ok || set.Rules[0].ID != "child" {
		t.Fatal("nearest machine lost")
	}
	if set, ok := source.ResolvedRulesForFlow("sibling").Criteria("review"); !ok || set.Rules[0].ID != "parent" {
		t.Fatal("sibling selection changed")
	}
	rules["review"].Validation.Rules[0].ID = "changed"
	if bundle.FlowTree.Root.Children[0].Rules["review"].Validation.Rules[0].ID != "child" {
		t.Fatal("resolved rules alias declaration")
	}
}
