package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestLoopValidationAcceptsBoundedDisjointRevisionLoop(t *testing.T) {
	if findings := loopValidationFindings(loopValidationBundle()); len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestLoopValidationRejectsMissingAdmissionOnCorrelatedHandler(t *testing.T) {
	bundle := loopValidationBundle()
	handler := bundle.Nodes["controller"].EventHandlers["draft.ready"]
	handler.Loop = nil
	bundle.Nodes["controller"].EventHandlers["draft.ready"] = handler
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "consumes loop revision field revision_id but omits loop operation") {
		t.Fatalf("findings = %#v, want missing admission", findings)
	}
}

func TestLoopValidationRejectsEscapeInsideRegion(t *testing.T) {
	bundle := loopValidationBundle()
	bundle.Semantics.Loops[0].Escape.AdvancesTo = "drafting"
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "escape target drafting does not leave the loop region") {
		t.Fatalf("findings = %#v, want invalid escape", findings)
	}
}

func TestLoopValidationRejectsRecurringTimerConnectedToRegion(t *testing.T) {
	bundle := loopValidationBundle()
	bundle.Semantics.Timers = []runtimecontracts.WorkflowTimerContract{{
		ID: "review.poll", Stage: "review", StageOwned: true, Event: "review.poll", StartOn: "state:review", Recurring: true,
	}}
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "recurring timer review.poll is connected to the loop region") {
		t.Fatalf("findings = %#v, want recurring timer rejection", findings)
	}
}

func TestLoopValidationRejectsTimerThatBypassesLoopClose(t *testing.T) {
	bundle := loopValidationBundle()
	bundle.Semantics.Timers = []runtimecontracts.WorkflowTimerContract{{
		ID: "review.expire", Stage: "review", StageOwned: true, Event: "platform.stage_timer_fired",
		StartOn: "state:review", AdvancesTo: "exhausted",
	}}
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "timer review.expire advances_to exhausted leaves the loop region") {
		t.Fatalf("findings = %#v, want timer lifecycle bypass rejection", findings)
	}
}

func TestLoopValidationRejectsMissingOrNonIntegerAttemptPolicy(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		set   bool
	}{
		{name: "missing"},
		{name: "text", value: "three", set: true},
		{name: "fractional", value: 2.5, set: true},
		{name: "zero", value: 0, set: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loopValidationBundle()
			bundle.Semantics.Loops[0].MaxAttempts = runtimecontracts.LoopAttemptLimit{PolicyRef: "inner_revision_max"}
			if tc.set {
				bundle.Policy.Values = map[string]runtimecontracts.PolicyValue{"inner_revision_max": {Value: tc.value}}
			}
			if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "max_attempts policy inner_revision_max") {
				t.Fatalf("findings = %#v, want invalid attempt policy", findings)
			}
		})
	}
}

func TestLoopValidationRejectsReservedControlBucketAsNodeID(t *testing.T) {
	bundle := loopValidationBundle()
	bundle.Nodes["handler_loops"] = runtimecontracts.SystemNodeContract{}
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "node id handler_loops is reserved") {
		t.Fatalf("findings = %#v, want reserved loop state rejection", findings)
	}
}

func loopValidationFindings(bundle *runtimecontracts.WorkflowContractBundle) []Finding {
	return checkLoopValidation(newCheckerContext(context.Background(), semanticview.Wrap(bundle), Options{}))
}

func loopValidationBundle() *runtimecontracts.WorkflowContractBundle {
	revisionField := "revision_id"
	emit := func(event string) runtimecontracts.EmitSpec {
		return runtimecontracts.EmitSpec{Event: event, Fields: map[string]runtimecontracts.ExpressionValue{revisionField: runtimecontracts.RefExpression("loop.revision_id")}}
	}
	handlers := map[string]runtimecontracts.SystemNodeEventHandler{
		"research.done": {
			Loop: &runtimecontracts.LoopOperationSpec{Start: "revision", From: "research"}, AdvancesTo: "drafting", Emit: emit("draft.requested"),
		},
		"draft.ready": {
			Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "drafting"}, AdvancesTo: "review", Emit: emit("review.requested"),
		},
		"review.issues": {
			Loop: &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "review"}, AdvancesTo: "drafting", Emit: emit("draft.requested"),
		},
		"review.passed": {
			Loop: &runtimecontracts.LoopOperationSpec{Close: "revision", From: "review"}, AdvancesTo: "approved",
		},
	}
	operations := []runtimecontracts.WorkflowLoopOperationPlan{
		{NodeID: "controller", HandlerEvent: "research.done", Kind: runtimecontracts.LoopOperationStart, LoopID: "revision", From: "research", AdvancesTo: "drafting", Emit: emit("draft.requested")},
		{NodeID: "controller", HandlerEvent: "draft.ready", Kind: runtimecontracts.LoopOperationAdmit, LoopID: "revision", From: "drafting", AdvancesTo: "review", Emit: emit("review.requested")},
		{NodeID: "controller", HandlerEvent: "review.issues", Kind: runtimecontracts.LoopOperationRepeat, LoopID: "revision", From: "review", AdvancesTo: "drafting", Emit: emit("draft.requested")},
		{NodeID: "controller", HandlerEvent: "review.passed", Kind: runtimecontracts.LoopOperationClose, LoopID: "revision", From: "review", AdvancesTo: "approved"},
	}
	events := map[string]runtimecontracts.EventCatalogEntry{}
	for _, eventType := range []string{"draft.ready", "review.issues", "review.passed", "draft.requested", "review.requested"} {
		events[eventType] = loopRevisionEvent(revisionField)
	}
	events["research.done"] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{}}}
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{
			{ID: "research", Initial: true}, {ID: "drafting"}, {ID: "review"}, {ID: "exhausted", Terminal: true}, {ID: "approved", Terminal: true},
		}}},
		Events: events,
		Nodes:  map[string]runtimecontracts.SystemNodeContract{"controller": {EventHandlers: handlers}},
		Semantics: runtimecontracts.WorkflowSemanticView{
			InitialStage:   "research",
			Stages:         []runtimecontracts.WorkflowStageContract{{ID: "research"}, {ID: "drafting"}, {ID: "review"}, {ID: "exhausted"}, {ID: "approved"}},
			TerminalStages: []string{"exhausted", "approved"},
			Loops:          []runtimecontracts.WorkflowLoopPlan{{ID: "revision", RevisionField: revisionField, MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 3}, Escape: runtimecontracts.LoopEscapeSpec{AdvancesTo: "exhausted"}, EntryStage: "drafting", RegionStages: []string{"drafting", "review"}, Operations: operations}},
		},
	}
}

func loopRevisionEvent(field string) runtimecontracts.EventCatalogEntry {
	return runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{
		Properties: map[string]runtimecontracts.EventFieldSpec{field: {Type: "text"}}, Required: []string{field},
	}}
}

func loopFindingContains(findings []Finding, want string) bool {
	for _, finding := range findings {
		if strings.Contains(finding.Message, want) {
			return true
		}
	}
	return false
}
