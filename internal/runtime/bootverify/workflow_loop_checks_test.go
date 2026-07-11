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
	refreshLoopValidationTopology(bundle)
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "may execute in the loop region but omits loop operation") {
		t.Fatalf("findings = %#v, want missing admission", findings)
	}
}

func TestLoopValidationRejectsMissingAdmissionAndRevisionField(t *testing.T) {
	bundle := loopValidationBundle()
	handler := bundle.Nodes["controller"].EventHandlers["draft.ready"]
	handler.Loop = nil
	bundle.Nodes["controller"].EventHandlers["draft.ready"] = handler
	bundle.Events["draft.ready"] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{}}}
	refreshLoopValidationTopology(bundle)
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "omits both loop operation and required text revision field revision_id") {
		t.Fatalf("findings = %#v, want dual omission rejection", findings)
	}
}

func TestLoopValidationRejectsEscapeInsideRegion(t *testing.T) {
	bundle := loopValidationBundle()
	bundle.Semantics.Loops[0].Escape.AdvancesTo = "drafting"
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "escape target drafting does not leave the loop SCC") {
		t.Fatalf("findings = %#v, want invalid escape", findings)
	}
}

func TestLoopValidationRejectsCloseBackIntoCanonicalRegion(t *testing.T) {
	bundle := loopValidationBundle()
	handler := bundle.Nodes["controller"].EventHandlers["review.passed"]
	handler.AdvancesTo = "drafting"
	bundle.Nodes["controller"].EventHandlers["review.passed"] = handler
	for idx := range bundle.Semantics.Loops[0].Operations {
		if bundle.Semantics.Loops[0].Operations[idx].Kind == runtimecontracts.LoopOperationClose {
			bundle.Semantics.Loops[0].Operations[idx].AdvancesTo = "drafting"
		}
	}
	refreshLoopValidationTopology(bundle)
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "close target drafting does not leave the loop SCC") {
		t.Fatalf("findings = %#v, want close exit rejection", findings)
	}
}

func TestLoopValidationRejectsRepeatAwayFromEntry(t *testing.T) {
	bundle := loopValidationBundle()
	handler := bundle.Nodes["controller"].EventHandlers["review.issues"]
	handler.AdvancesTo = "review"
	bundle.Nodes["controller"].EventHandlers["review.issues"] = handler
	for idx := range bundle.Semantics.Loops[0].Operations {
		if bundle.Semantics.Loops[0].Operations[idx].Kind == runtimecontracts.LoopOperationRepeat {
			bundle.Semantics.Loops[0].Operations[idx].AdvancesTo = "review"
		}
	}
	refreshLoopValidationTopology(bundle)
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "must return to entry stage drafting") {
		t.Fatalf("findings = %#v, want repeat entry rejection", findings)
	}
}

func TestLoopValidationRejectsEscapeTargetThatReconnectsToRegion(t *testing.T) {
	bundle := loopValidationBundle()
	bundle.Semantics.TerminalStages = []string{"approved"}
	bundle.Nodes["controller"].EventHandlers["escalation.reopened"] = runtimecontracts.SystemNodeEventHandler{AdvancesTo: "drafting"}
	bundle.Events["escalation.reopened"] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{}}}
	refreshLoopValidationTopology(bundle)
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "escape target exhausted does not leave the loop SCC") {
		t.Fatalf("findings = %#v, want reconnecting escape rejection", findings)
	}
}

func TestLoopValidationCanonicalRegionExcludesStartSource(t *testing.T) {
	bundle := loopValidationBundle()
	region := stringSet(bundle.Semantics.Loops[0].RegionStages)
	if _, ok := region["research"]; ok {
		t.Fatalf("loop region = %#v, start source must not be loop-owned", bundle.Semantics.Loops[0].RegionStages)
	}
}

func TestLoopValidationRejectsUnownedEscapeEmit(t *testing.T) {
	bundle := loopValidationBundle()
	bundle.Semantics.Loops[0].Escape.Emit = runtimecontracts.EmitSpec{Event: "review.escalated", Fields: map[string]runtimecontracts.ExpressionValue{
		"revision_id": runtimecontracts.RefExpression("loop.revision_id"),
	}}
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "escape.emit event review.escalated has no typed event schema") {
		t.Fatalf("findings = %#v, want unknown escape event rejection", findings)
	}
}

func TestLoopValidationRejectsIncompleteEscapeEmitPayload(t *testing.T) {
	bundle := loopValidationBundle()
	bundle.Events["review.escalated"] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{
		Properties: map[string]runtimecontracts.EventFieldSpec{"revision_id": {Type: "text"}, "reason": {Type: "text"}},
		Required:   []string{"revision_id", "reason"},
	}}
	bundle.Semantics.Loops[0].Escape.Emit = runtimecontracts.EmitSpec{Event: "review.escalated", Fields: map[string]runtimecontracts.ExpressionValue{
		"revision_id": runtimecontracts.RefExpression("loop.revision_id"),
	}}
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "omits required payload field reason") {
		t.Fatalf("findings = %#v, want incomplete escape payload rejection", findings)
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
	bundle := &runtimecontracts.WorkflowContractBundle{
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
	refreshLoopValidationTopology(bundle)
	return bundle
}

func refreshLoopValidationTopology(bundle *runtimecontracts.WorkflowContractBundle) {
	transitions := make([]runtimecontracts.HandlerTransitionSemantic, 0)
	for eventType, handler := range bundle.Nodes["controller"].EventHandlers {
		transitions = append(transitions, runtimecontracts.HandlerTransitionSemantic{
			ID: eventType, NodeID: "controller", EventType: eventType, CreateEntity: handler.CreateEntity,
			AdvancesTo: handler.AdvancesTo, Emit: handler.Emit, Loop: handler.Loop, OnComplete: handler.OnComplete,
			Rules: handler.Rules, Accumulate: handler.Accumulate, Join: handler.Join,
		})
	}
	bundle.Semantics.HandlerTransitions = transitions
	topology := runtimecontracts.BuildWorkflowStageTopology("", bundle.Semantics.InitialStage,
		[]string{"research", "drafting", "review", "exhausted", "approved"}, bundle.Semantics.TerminalStages,
		transitions, bundle.Semantics.Timers, bundle.Semantics.Loops)
	bundle.Semantics.StageTopologies = map[string]runtimecontracts.WorkflowStageTopology{"": topology}
	bundle.Semantics.Loops = runtimecontracts.BindWorkflowLoopRegions(bundle.Semantics.Loops, bundle.Semantics.StageTopologies)
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
