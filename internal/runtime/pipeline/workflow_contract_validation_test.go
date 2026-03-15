package pipeline

import (
	"strings"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/semanticview"
)

func TestValidateWorkflowContractsRejectsOnCompleteAndRulesInSameHandler(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(contractComplianceRepoRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle: %v", err)
	}

	nodeID, eventType, handler, ok := firstWorkflowHandler(bundle)
	if !ok {
		t.Fatal("expected at least one workflow handler")
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{Condition: "true"}}
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{Condition: "true"}}

	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node

	err = validateWorkflowContracts(semanticview.Wrap(bundle))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !workflowValidationErrorContains(err, "declares both on_complete and rules") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateWorkflowContractsRejectsUnknownHandlerAction(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(contractComplianceRepoRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle: %v", err)
	}

	nodeID, eventType, handler, ok := firstWorkflowHandler(bundle)
	if !ok {
		t.Fatal("expected at least one workflow handler")
	}
	handler.Action = runtimecontracts.ActionSpec{ID: "missing.handler.action"}

	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node

	err = validateWorkflowContracts(semanticview.Wrap(bundle))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !workflowValidationErrorContains(err, "action missing.handler.action is not executable") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateWorkflowContractsRejectsUnsupportedGuardOnFail(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(contractComplianceRepoRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle: %v", err)
	}

	nodeID, eventType, handler, ok := firstWorkflowHandler(bundle)
	if !ok {
		t.Fatal("expected at least one workflow handler")
	}
	if handler.Guard == nil {
		handler.Guard = &runtimecontracts.GuardSpec{Check: "true"}
	}
	handler.Guard.OnFail = "escalate:"

	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node

	err = validateWorkflowContracts(semanticview.Wrap(bundle))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !workflowValidationErrorContains(err, "on_fail escalate requires event type") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func firstWorkflowHandler(bundle *runtimecontracts.WorkflowContractBundle) (string, string, runtimecontracts.SystemNodeEventHandler, bool) {
	for nodeID, node := range bundle.Nodes {
		for eventType, handler := range node.EventHandlers {
			return nodeID, eventType, handler, true
		}
	}
	return "", "", runtimecontracts.SystemNodeEventHandler{}, false
}

func workflowValidationErrorContains(err error, substr string) bool {
	if err == nil || strings.TrimSpace(substr) == "" {
		return false
	}
	text := err.Error()
	return strings.Contains(text, substr)
}
