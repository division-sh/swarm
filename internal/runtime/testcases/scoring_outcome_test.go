package testcases

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestGenericBundle_ScoringOutcomePatterns(t *testing.T) {
	bundle := loadGenericSwarmBundle(t)
	handler := mustHandler(t, bundle, "processing-node", "item.review_requested")
	if len(handler.Rules) != 2 {
		t.Fatalf("expected approval and default rejection rows, got %+v", handler.Rules)
	}
	approved := previewHandler(t, bundle, "processing-node", "item.review_requested", map[string]any{
		"entity_id":    "item-123",
		"score":        90.0,
		"quality":      90.0,
		"completeness": 90.0,
		"risk":         80.0,
		"priority":     "urgent",
	}, runtimepipeline.WorkflowState{
		EntityID: "item-123",
		Stage:    runtimepipeline.NormalizeWorkflowStateID("ready"),
		Status:   "ready",
		Metadata: map[string]any{},
	}, nil)
	approveRef, approveDeclared := handler.Rules[0].DeclarationIdentity()
	approveFact, err := approved.RuleSelection.ResolvedFact()
	if !approveDeclared || err != nil || approveFact.Context() != handlerselection.ContextRules || approveFact.Disposition() != handlerselection.DispositionSelected || !approveFact.Ref().Equal(approveRef) || approved.RuleID != "rules[0]: when payload.score >= 80.0" || approveFact.DisplayLabel() != approved.RuleID || approved.Stage != runtimepipeline.NormalizeWorkflowStateID("approved") {
		t.Fatalf("expected exact approval row execution, got %+v (selection error: %v)", approved, err)
	}
	if !hasAll(approved.Emits, "processing/item.completed") {
		t.Fatalf("expected approval emission, got %v", approved.Emits)
	}
	if got := approved.Computed["score"]; got == nil {
		t.Fatalf("expected computed score to be stored, got %+v", approved)
	}

	rejected := previewHandler(t, bundle, "processing-node", "item.review_requested", map[string]any{
		"entity_id":    "item-456",
		"score":        45.0,
		"quality":      45.0,
		"completeness": 55.0,
		"risk":         40.0,
	}, runtimepipeline.WorkflowState{
		EntityID: "item-456",
		Stage:    runtimepipeline.NormalizeWorkflowStateID("ready"),
		Status:   "ready",
		Metadata: map[string]any{},
	}, nil)
	rejectRef, rejectDeclared := handler.Rules[1].DeclarationIdentity()
	rejectFact, err := rejected.RuleSelection.ResolvedFact()
	if !rejectDeclared || err != nil || rejectFact.Context() != handlerselection.ContextRules || rejectFact.Disposition() != handlerselection.DispositionSelected || !rejectFact.Ref().Equal(rejectRef) || rejected.RuleID != "rules[1]: default" || rejectFact.DisplayLabel() != rejected.RuleID || rejected.Stage != runtimepipeline.NormalizeWorkflowStateID("rejected") {
		t.Fatalf("expected exact default rejection row execution, got %+v (selection error: %v)", rejected, err)
	}
	if !hasAll(rejected.Emits, "processing/item.rejected") {
		t.Fatalf("expected rejection emission, got %v", rejected.Emits)
	}
}
