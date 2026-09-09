package semanticview

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

// These tests exercise declaration compilation, not storage admission of R or
// generation ownership. Those boundaries must supply their own execution proof.
func loopCarriageBundle(t *testing.T) (*contracts.WorkflowContractBundle, identity.ExecutableNode) {
	t.Helper()
	node, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
	if err != nil {
		t.Fatal(err)
	}
	emit := contracts.EmitSpec{Event: "review.requested", Fields: map[string]contracts.ExpressionValue{"revision_id": contracts.RefExpression("loop.revision_id")}}
	entry := contracts.EventCatalogEntry{Payload: contracts.EventPayloadSpec{
		Properties: map[string]contracts.EventFieldSpec{"revision_id": {Type: "text"}}, Required: []string{"revision_id"},
	}}
	bundle := &contracts.WorkflowContractBundle{
		Events: map[string]contracts.EventCatalogEntry{"start": {}, "review.requested": entry, "review.passed": entry},
		Nodes: map[string]contracts.SystemNodeContract{"controller": {EventHandlers: map[string]contracts.SystemNodeEventHandler{
			"start":         {Loop: &contracts.LoopOperationSpec{Start: "revision", From: "queued"}, Emit: emit},
			"review.passed": {Loop: &contracts.LoopOperationSpec{Close: "revision", From: "review"}},
		}}},
		Semantics: contracts.WorkflowSemanticView{Loops: []contracts.WorkflowLoopPlan{{
			FlowID: ".", ID: "revision", RevisionField: "revision_id", MaxAttempts: contracts.LoopAttemptLimit{Literal: 3},
			Operations: []contracts.WorkflowLoopOperationPlan{
				{Node: node, HandlerEvent: "start", Kind: contracts.LoopOperationStart, LoopID: "revision", Emit: emit},
				{Node: node, HandlerEvent: "review.passed", Kind: contracts.LoopOperationClose, LoopID: "revision"},
			},
		}}},
	}
	return bundle, node
}

func TestOriginalLoopCarriageDeclaration(t *testing.T) {
	for _, tc := range []struct {
		name, event, handler string
		producer, want       bool
	}{
		{"original_output", "review.requested", "start", true, true},
		{"input_without_producer", "review.passed", "", false, true},
		{"unrelated_input", "start", "", false, false},
		{"output_needs_producer", "review.requested", "", false, false},
		{"no_name_heuristic", "other.revision_id", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle, node := loopCarriageBundle(t)
			owner, err := compileLoopCarriage(Wrap(bundle))
			if err != nil {
				t.Fatal(err)
			}
			scope := LoopEventScope{FlowID: ".", EventType: tc.event, HandlerEvent: tc.handler}
			if tc.producer {
				scope.Producer = node
			}
			role, found, err := owner.resolve(scope)
			if err != nil || found != tc.want {
				t.Fatalf("resolve = %+v, %v, %v", role, found, err)
			}
			if found && (role.FlowID() != "." || role.LoopID() != "revision" || role.RevisionField() != "revision_id") {
				t.Fatalf("wrong role: %+v", role)
			}
		})
	}
}

func TestOriginalLoopCarriageRejectsDeclarationContradictions(t *testing.T) {
	for _, name := range []string{"optional_input", "numeric_input", "business_emit", "duplicate_operation", "foreign_operation", "different_loop", "missing_output_schema"} {
		t.Run(name, func(t *testing.T) {
			bundle, _ := loopCarriageBundle(t)
			switch name {
			case "optional_input":
				entry := bundle.Events["review.passed"]
				entry.Payload.Required = nil
				bundle.Events["review.passed"] = entry
			case "numeric_input":
				bundle.Events["review.passed"].Payload.Properties["revision_id"] = contracts.EventFieldSpec{Type: "integer"}
			case "business_emit":
				bundle.Nodes["controller"].EventHandlers["start"].Emit.Fields["revision_id"] = contracts.RefExpression("payload.revision_id")
			case "duplicate_operation":
				p := &bundle.Semantics.Loops[0]
				p.Operations = append(p.Operations, p.Operations[0])
			case "foreign_operation":
				node, err := identity.AdmitExecutableNodeDeclaration("child", "controller")
				if err != nil {
					t.Fatal(err)
				}
				bundle.Semantics.Loops[0].Operations[0].Node = node
			case "different_loop":
				bundle.Semantics.Loops[0].Operations[0].LoopID = "other"
			case "missing_output_schema":
				delete(bundle.Events, "review.requested")
			}
			if _, err := compileLoopCarriage(Wrap(bundle)); err == nil {
				t.Fatal("contradictory declaration accepted")
			}
		})
	}
}

func TestOriginalLoopCarriageUnanimousSites(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, conflict := range []bool{false, true} {
			bundle, node := loopCarriageBundle(t)
			handler := bundle.Nodes["controller"].EventHandlers["start"]
			ref := handler.Emit
			cel := contracts.EmitSpec{Event: ref.Event, Fields: map[string]contracts.ExpressionValue{
				"revision_id": {Kind: contracts.ExpressionKindCEL, CEL: "loop.revision_id"},
			}}
			if conflict {
				cel.Fields["revision_id"] = contracts.RefExpression("payload.revision_id")
			}
			handler.Emit = contracts.EmitSpec{}
			handler.Rules = []contracts.HandlerRuleEntry{{ID: "first", Emit: ref}, {ID: "second", Emit: cel}}
			if reverse {
				handler.Rules[0], handler.Rules[1] = handler.Rules[1], handler.Rules[0]
			}
			bundle.Nodes["controller"].EventHandlers["start"] = handler
			owner, err := compileLoopCarriage(Wrap(bundle))
			if conflict {
				if err == nil {
					t.Fatalf("business alternative accepted, reverse=%v", reverse)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			role, found, err := owner.resolve(LoopEventScope{FlowID: ".", EventType: "review.requested", Producer: node, HandlerEvent: "start"})
			if err != nil || !found || role.LoopID() != "revision" {
				t.Fatalf("unanimous sites: %+v %v %v", role, found, err)
			}
		}
	}
}

func TestOriginalLoopCarriageConflictingInputAndOutput(t *testing.T) {
	bundle, node := loopCarriageBundle(t)
	bundle.Nodes["controller"].EventHandlers["business"] = contracts.SystemNodeEventHandler{Emit: contracts.EmitSpec{Event: "review.passed"}}
	owner, err := compileLoopCarriage(Wrap(bundle))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := owner.resolve(LoopEventScope{FlowID: ".", EventType: "review.passed", Producer: node, HandlerEvent: "business"}); err == nil {
		t.Fatal("affirmative business output overrides input role")
	}
	if _, found, err := owner.resolve(LoopEventScope{FlowID: ".", EventType: "review.passed"}); err != nil || !found {
		t.Fatalf("unambiguous external/agent input requires an invented producer: %v %v", found, err)
	}
}

func TestOriginalLoopCarriageDetached(t *testing.T) {
	bundle, node := loopCarriageBundle(t)
	owner, err := compileLoopCarriage(Wrap(bundle))
	if err != nil {
		t.Fatal(err)
	}
	bundle.Semantics.Loops[0].ID = "selected-loop"
	bundle.Semantics.Loops[0].RevisionField = "selected-field"
	bundle.Nodes["controller"].EventHandlers["start"].Emit.Fields["revision_id"] = contracts.RefExpression("payload.revision_id")
	delete(bundle.Events, "review.passed")
	for _, scope := range []LoopEventScope{
		{FlowID: ".", EventType: "review.requested", Producer: node, HandlerEvent: "start"},
		{FlowID: ".", EventType: "review.passed"},
	} {
		role, found, err := owner.resolve(scope)
		if err != nil || !found || role.LoopID() != "revision" || role.RevisionField() != "revision_id" {
			t.Fatalf("source edits changed admitted meaning: %+v %v %v", role, found, err)
		}
	}
}

func TestOriginalLoopCarriageRequiresAdmittedArtifact(t *testing.T) {
	bundle, node := loopCarriageBundle(t)
	if _, err := CompileOriginalLoopCarriage(Wrap(bundle)); err == nil {
		t.Fatal("unbound source accepted")
	}
	if _, err := CompileOriginalLoopCarriage(nil); err == nil {
		t.Fatal("nil source accepted")
	}
	var owner OriginalLoopCarriage
	if err := owner.RequireSource(""); err == nil {
		t.Fatal("zero artifact binding accepted")
	}
	if _, _, err := owner.Resolve(LoopEventScope{FlowID: ".", EventType: "review.passed"}); err == nil {
		t.Fatal("zero owner resolved role")
	}
	if _, _, err := owner.ResolveActivity(node, "start", "write", "write"); err == nil {
		t.Fatal("zero owner resolved activity")
	}
}
