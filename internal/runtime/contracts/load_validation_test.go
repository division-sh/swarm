package contracts

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateWorkflowContractBundleLoadConstraintsRejectsOnCompleteAndRules(t *testing.T) {
	bundle, err := LoadWorkflowContractBundle(contractRepoRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle: %v", err)
	}

	nodeID, eventType, handler, ok := firstLoadedWorkflowHandler(bundle)
	if !ok {
		t.Fatal("expected workflow handler")
	}
	handler.OnComplete = []HandlerRuleEntry{{Condition: "true"}}
	handler.Rules = []HandlerRuleEntry{{Condition: "else"}}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node

	err = validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !errors.Is(err, ErrConflictingCompletion) {
		t.Fatalf("unexpected load validation error: %v", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRejectsDeprecatedGuardFallback(t *testing.T) {
	bundle, err := LoadWorkflowContractBundle(contractRepoRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle: %v", err)
	}

	nodeID, eventType, handler, ok := firstLoadedWorkflowHandler(bundle)
	if !ok {
		t.Fatal("expected workflow handler")
	}
	handler.Guard = &GuardSpec{ID: "legacy_guard_only"}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node

	err = validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !errors.Is(err, ErrDeprecatedGuardFallback) {
		t.Fatalf("unexpected load validation error: %v", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRejectsMultipleAuthoritativeOwners(t *testing.T) {
	bundle, err := LoadWorkflowContractBundle(contractRepoRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle: %v", err)
	}

	bundle.Semantics.EventOwners["task.completed"] = []string{"node-a", "node-b"}

	err = validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !errors.Is(err, ErrMultipleAuthoritativeOwners) {
		t.Fatalf("unexpected load validation error: %v", err)
	}
}

func contractRepoRoot(t *testing.T) string {
	t.Helper()
	return strings.TrimSpace(repoRootForContractsTest(t))
}

func firstLoadedWorkflowHandler(bundle *WorkflowContractBundle) (string, string, SystemNodeEventHandler, bool) {
	for nodeID, node := range bundle.Nodes {
		for eventType, handler := range node.EventHandlers {
			return nodeID, eventType, handler, true
		}
	}
	return "", "", SystemNodeEventHandler{}, false
}

func TestLoadWorkflowContractBundleRejectsTier8DialectFixtures(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml")
	cases := []struct {
		name     string
		fixture  string
		contains string
	}{
		{name: "advances_to list", fixture: "test-boot-advances-to-list", contains: "DIALECT-ADV-LIST"},
		{name: "guard scalar", fixture: "test-boot-dialect-guard", contains: "DIALECT-GUARD"},
		{name: "on_complete dict", fixture: "test-boot-on-complete-dict", contains: "DIALECT-OC-ORDER"},
		{name: "undefined handler field", fixture: "test-boot-handler-field-undefined", contains: "UNDEFINED-FIELD"},
		{name: "deprecated handler field", fixture: "test-boot-deprecated-field", contains: "DEPRECATED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", tc.fixture)
			_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err == nil || !contractErrorContains(err, tc.contains) {
				t.Fatalf("expected load error containing %q, got %v", tc.contains, err)
			}
		})
	}
}

func contractErrorContains(err error, substr string) bool {
	if err == nil || strings.TrimSpace(substr) == "" {
		return false
	}
	var verr *LoadValidationError
	if errors.As(err, &verr) {
		for _, item := range verr.Items {
			if item != nil && strings.Contains(item.Error(), substr) {
				return true
			}
		}
	}
	text := err.Error()
	return strings.Contains(text, substr)
}
