package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"gopkg.in/yaml.v3"
)

func TestPlatformSpecPolicyRowsUseCanonicalElementIdentity(t *testing.T) {
	repo := handlerRuleIdentityGuardRepoRoot(t)
	raw, err := os.ReadFile(DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		HandlerSpecification struct {
			OnCompleteVsRules struct {
				OnComplete string `yaml:"on_complete"`
			} `yaml:"on_complete_vs_rules"`
			AuthoredRuleElementIdentity struct {
				Grammar string `yaml:"grammar"`
			} `yaml:"authored_rule_element_identity"`
			HandlerFields struct {
				Rules struct {
					Type                     string `yaml:"type"`
					PolicySheetSelectionRows struct {
						StableIdentity string `yaml:"stable_identity"`
					} `yaml:"policy_sheet_selection_rows"`
				} `yaml:"rules"`
			} `yaml:"handler_fields"`
		} `yaml:"handler_specification"`
		Engine struct {
			BootVerification struct {
				Checks []struct {
					ID      string `yaml:"id"`
					Trigger string `yaml:"trigger"`
				} `yaml:"checks"`
			} `yaml:"boot_verification"`
		} `yaml:"engine"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	rules := spec.HandlerSpecification.HandlerFields.Rules
	for field, text := range map[string]string{"rules.type": rules.Type, "policy_sheet_selection_rows.stable_identity": rules.PolicySheetSelectionRows.StableIdentity} {
		if !strings.Contains(text, "element_id") || !strings.Contains(text, "non-authoritative display") {
			t.Fatalf("%s = %q, want canonical element_id and non-authoritative display contract", field, text)
		}
		if strings.Contains(text, "stable `id`") || strings.Contains(text, "selected row ID") {
			t.Fatalf("%s retains local-id identity authority: %q", field, text)
		}
	}
	if text := spec.HandlerSpecification.OnCompleteVsRules.OnComplete; !strings.Contains(text, "Ordered list") || strings.Contains(text, "keyed map") {
		t.Fatalf("handler_specification.on_complete = %q, want ordered-list-only contract", text)
	}
	grammar := spec.HandlerSpecification.AuthoredRuleElementIdentity.Grammar
	for _, required := range []string{"Empty authored rows are invalid", "handler.on_complete admits only an ordered sequence", "Aliases are resolved before rule-shape classification", "recursive aliases fail closed", "reuse across authored row occurrences fails closed", "Explicit null collections and duplicate normalized keys are invalid"} {
		if !strings.Contains(grammar, required) {
			t.Fatalf("authored_rule_element_identity.grammar = %q, want %q", grammar, required)
		}
	}
	for _, check := range spec.Engine.BootVerification.Checks {
		if check.ID != "dialect_compliance" {
			continue
		}
		if !strings.Contains(check.Trigger, "on_complete as a map") || !strings.Contains(check.Trigger, "structurally ambiguous") {
			t.Fatalf("dialect_compliance trigger = %q, want list-only completion and rules ambiguity rejection", check.Trigger)
		}
		return
	}
	t.Fatal("dialect_compliance spec check is missing")
}

func TestHandlerRuleIdentityIgnoresKeyLabelAndPosition(t *testing.T) {
	const elementID = "00000000-0000-4000-8000-000000000301"
	decode := func(raw string) HandlerRuleEntry {
		t.Helper()
		var handler SystemNodeEventHandler
		if err := yaml.Unmarshal([]byte(raw), &handler); err != nil {
			t.Fatal(err)
		}
		node, err := runtimeidentity.AdmitExecutableNodeDeclaration("flows/scout", "scout", "router")
		if err != nil {
			t.Fatal(err)
		}
		handler, err = QualifySystemNodeHandlerRuleRefs(node, handler)
		if err != nil {
			t.Fatal(err)
		}
		return handler.Rules[0]
	}
	before := decode(`rules:
  original-key:
    element_id: ` + elementID + `
    id: original-label
    condition: else
`)
	after := decode(`rules:
  renamed-key:
    element_id: ` + elementID + `
    id: renamed-label
    condition: else
`)
	beforeRef, beforeOK := before.ContractElementRef()
	afterRef, afterOK := after.ContractElementRef()
	if !beforeOK || !afterOK || !beforeRef.Equal(afterRef) {
		t.Fatalf("label rename changed typed identity: before=%#v after=%#v", beforeRef, afterRef)
	}
	if before.ID == after.ID {
		t.Fatal("hostile fixture did not actually rename the display label")
	}
}

func TestHandlerRuleIdentityRequiresAuthoredElementIDButNotForSyntheticEntries(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte("rules:\n  selected: {condition: else}\n"), &handler); err != nil {
		t.Fatal(err)
	}
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", "", "router")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := QualifySystemNodeHandlerRuleRefs(node, handler); err == nil || !strings.Contains(err.Error(), "swarm mint-element-ids") {
		t.Fatalf("missing element identity error = %v", err)
	}

	synthetic := SystemNodeEventHandler{Rules: []HandlerRuleEntry{{ID: "runtime-only", Condition: "else"}}}
	qualified, err := QualifySystemNodeHandlerRuleRefs(node, synthetic)
	if err != nil {
		t.Fatalf("synthetic rule qualification: %v", err)
	}
	if _, ok := qualified.Rules[0].ContractElementRef(); ok {
		t.Fatal("synthetic rule fabricated an authored contract element reference")
	}
}

func TestPolicyValueRowsCarryIdentityWithoutBecomingExecutableSelectionRows(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`rules:
  lookup-choice:
    element_id: 00000000-0000-4000-8000-000000000302
    lookup:
      on: payload.kind
      entries:
        - key: service
          value: selected
      into: computed.choice
      default: fail
  selected:
    element_id: 00000000-0000-4000-8000-000000000303
    condition: else
`), &handler); err != nil {
		t.Fatal(err)
	}
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration("flows/scout", "scout", "router")
	if err != nil {
		t.Fatal(err)
	}
	qualified, err := QualifySystemNodeHandlerRuleRefs(node, handler)
	if err != nil {
		t.Fatal(err)
	}
	if qualified.Rules[0].PolicyRow.Kind != PolicySheetRowKindLookup {
		t.Fatalf("first row kind = %q", qualified.Rules[0].PolicyRow.Kind)
	}
	for index, rule := range qualified.Rules {
		if ref, ok := rule.ContractElementRef(); !ok || !ref.Valid() {
			t.Fatalf("rule %d missing typed identity: %#v", index, rule)
		}
	}
}

func TestHandlerRuleSemanticCarriersPreserveTypedReference(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`rules:
  selected:
    element_id: 00000000-0000-4000-8000-000000000304
    condition: else
    advances_to: done
    emit: completed
    activity: {tool: notify}
`), &handler); err != nil {
		t.Fatal(err)
	}
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration("flows/scout", "scout", "router")
	if err != nil {
		t.Fatal(err)
	}
	handler, err = QualifySystemNodeHandlerRuleRefs(node, handler)
	if err != nil {
		t.Fatal(err)
	}
	want, ok := handler.Rules[0].ContractElementRef()
	if !ok {
		t.Fatal("qualified rule ref is unavailable")
	}
	emitSites := HandlerDeclarativeEmitSites(handler)
	advanceCarriers := HandlerAdvanceCarriers(handler)
	activitySites := ActivitySitesForNode(node, map[string]SystemNodeEventHandler{"started": handler})
	if len(emitSites) != 1 || !emitSites[0].RuleRef.Equal(want) {
		t.Fatalf("emit sites = %#v", emitSites)
	}
	if len(advanceCarriers) != 1 || !advanceCarriers[0].RuleRef.Equal(want) {
		t.Fatalf("advance carriers = %#v", advanceCarriers)
	}
	if len(activitySites) != 1 || !activitySites[0].RuleRef.Equal(want) {
		t.Fatalf("activity sites = %#v", activitySites)
	}
}

func TestLoadWorkflowContractBundleQualifiesElementIdentityByPackage(t *testing.T) {
	const sharedElementID = "00000000-0000-4000-8000-000000000305"
	repo := repoRootForContractsTest(t)
	root := writeHandlerRuleIdentityPackageFixture(t, sharedElementID, false)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load package-qualified rule identity fixture: %v", err)
	}
	rootNode, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", "", "root-handler")
	if err != nil {
		t.Fatal(err)
	}
	childNode, err := runtimeidentity.AdmitExecutableNodeDeclaration("flows/child", "child", "child-handler")
	if err != nil {
		t.Fatal(err)
	}
	rootRule := bundle.Semantics.NodeHandlers[rootNode.Key()]["root.started"].Rules[0]
	childRule := bundle.Semantics.NodeHandlers[childNode.Key()]["child.started"].Rules[0]
	rootRef, rootOK := rootRule.ContractElementRef()
	childRef, childOK := childRule.ContractElementRef()
	if !rootOK || !childOK || rootRef.Equal(childRef) {
		t.Fatalf("package-qualified refs collapsed: root=%#v child=%#v", rootRef, childRef)
	}
	if rootRef.ElementID().String() != sharedElementID || childRef.ElementID().String() != sharedElementID {
		t.Fatalf("element axes changed: root=%#v child=%#v", rootRef, childRef)
	}
}

func TestLoadWorkflowContractBundleRejectsDuplicateElementIdentityWithinPackage(t *testing.T) {
	const sharedElementID = "00000000-0000-4000-8000-000000000306"
	repo := repoRootForContractsTest(t)
	root := writeHandlerRuleIdentityPackageFixture(t, sharedElementID, true)

	_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err == nil || !strings.Contains(err.Error(), "contract element_id "+sharedElementID+" is duplicated in package .") {
		t.Fatalf("duplicate package-local element identity error = %v", err)
	}
}

func writeHandlerRuleIdentityPackageFixture(t *testing.T, sharedElementID string, duplicateRoot bool) string {
	t.Helper()
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: handler-rule-identity-package-proof
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
`)
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: handler-rule-identity-package-proof
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  inputs:
    events: [root.started]
`)
	writeFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "root.started: {entity_id: string}\n")
	rootRules := ""
	if duplicateRoot {
		rootRules = `
        duplicate:
          element_id: ` + sharedElementID + `
          condition: else
          advances_to: done`
	}
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
root-handler:
  id: root-handler
  execution_type: system_node
  subscribes_to: [root.started]
  event_handlers:
    root.started:
      rules:
        selected:
          element_id: `+sharedElementID+`
          condition: else
          advances_to: done`+rootRules+`
`)
	childRoot := filepath.Join(root, "flows", "child")
	writeFixtureFile(t, filepath.Join(childRoot, "package.yaml"), "name: child\n")
	writeFixtureFile(t, filepath.Join(childRoot, "schema.yaml"), `
name: child
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  inputs:
    events: [child.started]
`)
	writeFixtureFile(t, filepath.Join(childRoot, "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(childRoot, "agents.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(childRoot, "events.yaml"), "child.started: {entity_id: string}\n")
	writeFixtureFile(t, filepath.Join(childRoot, "nodes.yaml"), `
child-handler:
  id: child-handler
  execution_type: system_node
  subscribes_to: [child.started]
  event_handlers:
    child.started:
      rules:
        selected:
          element_id: `+sharedElementID+`
          condition: else
          advances_to: done
`)
	return root
}
