package engine

import (
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"gopkg.in/yaml.v3"
)

func TestActivityLoopLineageRetainsHistoryWithoutReadmission(t *testing.T) {
	now := time.Now().UTC()
	activation, err := loopruntime.New("run", "entity", "validation", "revision", "revision_id", "start", "working", 3, now)
	if err != nil {
		t.Fatal(err)
	}
	g := activation.Generation()
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Loops: []runtimecontracts.WorkflowLoopPlan{{FlowID: "validation", ID: "revision", RevisionField: "revision_id"}},
	}})
	handler := runtimecontracts.SystemNodeEventHandler{
		Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "working"}, AdvancesTo: "executing",
	}
	parent := loopTestRequest(t, testStateSnapshot("working", nil, nil, nil), handler, uuidForLoopCarrier("lineage", 1), map[string]any{"revision_id": g.RevisionID}).Event
	intent := ActivityIntent{ExecutionFlowID: identity.NormalizeFlowID("validation"), Generation: g, LoopStage: "executing"}
	selection := handlerselection.NotApplicable()
	check := func(t *testing.T, candidate ActivityIntent, state []loopruntime.Activation, want bool) {
		t.Helper()
		err := ValidateActivityLoopLineage(candidate, parent, source, handler, selection, state)
		if (err == nil) != want {
			t.Fatalf("historical activity lineage: %v, want valid=%t", err, want)
		}
	}
	check(t, intent, []loopruntime.Activation{activation}, true)
	if err := activation.AdvanceWithin("result", "result", now); err != nil {
		t.Fatal(err)
	}
	check(t, intent, []loopruntime.Activation{activation}, true)
	if _, err := activation.Repeat("working", "repeat", now); err != nil {
		t.Fatal(err)
	}
	check(t, intent, []loopruntime.Activation{activation}, true)
	if err := activation.Close("complete", "close", now); err != nil {
		t.Fatal(err)
	}
	check(t, intent, []loopruntime.Activation{activation}, true)
	if activation.Admit(g.RevisionID, "working") != loopruntime.AdmissionStale {
		t.Fatal("proof inadvertently readmitted history")
	}
	for _, test := range []struct {
		name   string
		mutate func(*ActivityIntent)
	}{
		{"missing", func(i *ActivityIntent) { i.Generation = attemptgeneration.Generation{} }},
		{"flow", func(i *ActivityIntent) { i.Generation.FlowID = "foreign" }},
		{"loop", func(i *ActivityIntent) { i.Generation.LoopID = "foreign" }},
		{"revision_field", func(i *ActivityIntent) { i.Generation.RevisionField = "foreign" }},
		{"activation", func(i *ActivityIntent) { i.Generation.ActivationID = "foreign" }},
		{"parent_revision", func(i *ActivityIntent) {
			i.Generation.RevisionID = activation.RevisionID
			i.Generation.Attempt = activation.Attempt
		}},
		{"attempt_mismatch", func(i *ActivityIntent) { i.Generation.Attempt = 2 }},
		{"future_attempt", func(i *ActivityIntent) { i.Generation.Attempt = 3 }},
		{"malformed_attempt", func(i *ActivityIntent) { i.Generation.Attempt = 0 }},
		{"stage_before", func(i *ActivityIntent) { i.LoopStage = "working" }},
		{"stage_after", func(i *ActivityIntent) { i.LoopStage = "complete" }},
		{"stage_missing", func(i *ActivityIntent) { i.LoopStage = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := intent
			test.mutate(&candidate)
			check(t, candidate, []loopruntime.Activation{activation}, false)
		})
	}
	check(t, intent, nil, false)
	// The selected rule, not another reachable rule or the loop's present stage,
	// owns the transition before the activity dispatch.
	if err := yaml.Unmarshal([]byte("loop: {admit: revision, from: working}\nadvances_to: executing\nrules:\n  - condition: else\n    advances_to: selected\n  - condition: else\n    advances_to: unselected\n"), &handler); err != nil {
		t.Fatal(err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration("validation", "validator")
	if err != nil {
		t.Fatal(err)
	}
	handler, err = runtimecontracts.QualifySystemNodeHandlerRuleRefsForEvent(node, "work.requested", handler)
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := handler.Rules[0].DeclarationIdentity()
	selection, err = handlerselection.Selected(handlerselection.ContextRules, ref, "")
	if err != nil {
		t.Fatal(err)
	}
	intent.LoopStage = "selected"
	check(t, intent, []loopruntime.Activation{activation}, true)
	intent.LoopStage = "unselected"
	check(t, intent, []loopruntime.Activation{activation}, false)
}
