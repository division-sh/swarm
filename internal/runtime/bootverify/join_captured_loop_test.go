package bootverify

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestJoinCapturedLoopOutcomeValidation(t *testing.T) {
	for _, outcome := range []string{"on_complete", "timeout"} {
		for _, field := range []string{"id", "activation_id", "revision_id", "attempt", "max_attempts"} {
			for _, ref := range []bool{false, true} {
				expr := runtimecontracts.CELExpression("loop." + field)
				if ref {
					expr = runtimecontracts.RefExpression("loop." + field)
				}
				rule := runtimecontracts.HandlerRuleEntry{
					Emit:             runtimecontracts.EmitSpec{Event: "result", Fields: map[string]runtimecontracts.ExpressionValue{"captured": expr}},
					DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{TargetField: "captured", Value: expr}}},
				}
				if findings := validateJoinOutcome("test", ".", "join", "arrival", outcome, rule, []string{"waiting"}, runtimecontracts.CatalogTypeReference{}, false); len(findings) != 0 {
					t.Fatalf("%s %s ref=%v: %#v", outcome, field, ref, findings)
				}
			}
		}
		for _, expression := range []string{"loop.flow_id", "loop.revision_field", "loop.unknown", "_loop.revision_id", "payload.revision_id"} {
			rule := runtimecontracts.HandlerRuleEntry{DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{TargetField: "captured", Value: runtimecontracts.RefExpression(expression)}},
			}}
			if findings := validateJoinOutcome("test", ".", "join", "arrival", outcome, rule, nil, runtimecontracts.CatalogTypeReference{}, false); len(findings) == 0 {
				t.Fatalf("%s accepted %s", outcome, expression)
			}
		}
		if findings := validateJoinOutcome("test", ".", "join", "arrival", outcome, runtimecontracts.HandlerRuleEntry{AdvancesTo: "missing"}, []string{"waiting"}, runtimecontracts.CatalogTypeReference{}, false); len(findings) == 0 {
			t.Fatalf("%s lost actual target validation", outcome)
		}
	}
}

func TestLoopAdmissionDelegatesEmptyTargetOnlyToJoinOutcomes(t *testing.T) {
	bundle := loopValidationBundle()
	handler := bundle.Nodes["controller"].EventHandlers["draft.ready"]
	handler.Join = &runtimecontracts.JoinSpec{
		ID: "review", Stage: "drafting", OnCompleteFound: true, TimeoutFound: true,
		OnComplete: runtimecontracts.HandlerRuleEntry{AdvancesTo: handler.AdvancesTo, Emit: handler.Emit},
		Timeout:    runtimecontracts.JoinTimeoutSpec{After: "1h", Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "review"}},
	}
	handler.AdvancesTo, handler.Emit = "", runtimecontracts.EmitSpec{}
	bundle.Nodes["controller"].EventHandlers["draft.ready"] = handler
	bundle.Semantics.Loops[0].Operations[1].AdvancesTo = ""
	bundle.Semantics.Loops[0].Operations[1].Emit = runtimecontracts.EmitSpec{}
	refreshLoopValidationTopology(bundle)
	if findings := loopValidationFindings(bundle); len(findings) != 0 {
		t.Fatalf("delegated join target rejected: %#v", findings)
	}
	handler.Join.Timeout.Outcome.AdvancesTo = "approved"
	if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "leaves the loop region") {
		t.Fatalf("timeout escaped loop region: %#v", findings)
	}
	for index, event := range []string{"research.done", "draft.ready", "review.issues", "review.passed"} {
		t.Run(event, func(t *testing.T) {
			bundle := loopValidationBundle()
			handler := bundle.Nodes["controller"].EventHandlers[event]
			handler.AdvancesTo = ""
			bundle.Nodes["controller"].EventHandlers[event] = handler
			bundle.Semantics.Loops[0].Operations[index].AdvancesTo = ""
			refreshLoopValidationTopology(bundle)
			if findings := loopValidationFindings(bundle); !loopFindingContains(findings, "requires advances_to") {
				t.Fatalf("missing direct target accepted: %#v", findings)
			}
		})
	}
}
