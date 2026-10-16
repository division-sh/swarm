package semanticview

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestOriginalActivityApprovalRequiresExactDetachedDeclaration(t *testing.T) {
	for _, decision := range []string{"", "approve-write"} {
		t.Run("decision_"+decision, func(t *testing.T) {
			bundle, node := loopCarriageBundle(t)
			handler := bundle.Nodes["controller"].EventHandlers["start"]
			handler.Activity = contracts.ActivitySpec{ID: "write", Tool: "send"}
			if decision != "" {
				handler.Activity.Approval = &contracts.ActivityApprovalSpec{Decision: decision}
			}
			bundle.Nodes["controller"].EventHandlers["start"] = handler
			state, err := compileLoopCarriage(Wrap(bundle))
			if err != nil {
				t.Fatal(err)
			}
			// This tests detached declaration compilation, not artifact storage
			// admission or durable approval/execution authority.
			state.bundleHash = "original"
			owner := OriginalLoopCarriage{state: state}
			got, err := owner.ResolveActivityApproval(node, "start", "write", "send")
			if err != nil || got != decision {
				t.Fatalf("original approval=%q want=%q err=%v", got, decision, err)
			}
			if _, loop, err := owner.ResolveActivity(node, "start", "write", "send"); err != nil || !loop {
				t.Fatalf("approval evidence changed original loop role: loop=%t err=%v", loop, err)
			}
			handler.Activity.Approval = &contracts.ActivityApprovalSpec{Decision: "replacement"}
			bundle.Nodes["controller"].EventHandlers["start"] = handler
			got, err = owner.ResolveActivityApproval(node, "start", "write", "send")
			if err != nil || got != decision {
				t.Fatalf("replacement source rewrote captured approval=%q want=%q err=%v", got, decision, err)
			}
			foreign, err := identity.AdmitExecutableNodeDeclaration("other", "controller")
			if err != nil {
				t.Fatal(err)
			}
			for _, scope := range []loopActivityScope{
				{foreign, "start", "write", "send"},
				{node, "other", "write", "send"},
				{node, "start", "other", "send"},
				{node, "start", "write", "other"},
			} {
				if got, err := owner.ResolveActivityApproval(scope.node, scope.handler, scope.activity, scope.tool); err == nil || got != "" {
					t.Fatalf("foreign declaration became unapproved evidence: %+v decision=%q err=%v", scope, got, err)
				}
			}
		})
	}
}

func TestOriginalActivityApprovalRejectsAbsentOrIncompleteEvidence(t *testing.T) {
	bundle, node := loopCarriageBundle(t)
	if _, err := (OriginalLoopCarriage{}).ResolveActivityApproval(node, "start", "write", "send"); err == nil {
		t.Fatal("missing original artifact became an unapproved declaration")
	}
	handler := bundle.Nodes["controller"].EventHandlers["start"]
	handler.Activity = contracts.ActivitySpec{ID: "write", Tool: "send", Approval: &contracts.ActivityApprovalSpec{Decision: " "}}
	bundle.Nodes["controller"].EventHandlers["start"] = handler
	if _, err := compileLoopCarriage(Wrap(bundle)); err == nil {
		t.Fatal("incomplete approval became an unapproved declaration")
	}
}
