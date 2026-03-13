package contracts

import (
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
	if err == nil || !strings.Contains(err.Error(), "declares both on_complete and rules") {
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
	if err == nil || !strings.Contains(err.Error(), "deprecated id-only guard") {
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
	if err == nil || !strings.Contains(err.Error(), "multiple authoritative system node owners") {
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
