package contracts

import (
	"strings"
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"gopkg.in/yaml.v3"
)

func TestHandlerRuleIdentityDerivesFromCanonicalDeclarationSite(t *testing.T) {
	decode := func(raw string) HandlerRuleEntry {
		t.Helper()
		var handler SystemNodeEventHandler
		if err := yaml.Unmarshal([]byte(raw), &handler); err != nil {
			t.Fatal(err)
		}
		node, err := runtimeidentity.AdmitExecutableNodeDeclaration("scout", "router")
		if err != nil {
			t.Fatal(err)
		}
		handler, err = QualifySystemNodeHandlerRuleRefsForEvent(node, "work.requested", handler)
		if err != nil {
			t.Fatal(err)
		}
		return handler.Rules[0]
	}

	before := decode("rules:\n  - id: original-label\n    condition: else\n")
	after := decode("rules:\n  - id: renamed-label\n    condition: else\n")
	beforeRef, beforeOK := before.DeclarationIdentity()
	afterRef, afterOK := after.DeclarationIdentity()
	if !beforeOK || !afterOK || !beforeRef.Equal(afterRef) {
		t.Fatalf("display-label rename changed declaration identity: before=%#v after=%#v", beforeRef, afterRef)
	}
	if before.ID == after.ID {
		t.Fatal("fixture did not rename the display label")
	}
	if beforeRef.Flow().String() != "scout" || beforeRef.Family() != "handler_rule" || !strings.Contains(beforeRef.SemanticPath(), "handlers[\"work.requested\"].rules[0]") {
		t.Fatalf("derived declaration identity = %#v", beforeRef)
	}
}

func TestHandlerRuleReorderIsDeleteAndAdd(t *testing.T) {
	decode := func(raw string) map[string]runtimeidentity.DeclarationIdentity {
		t.Helper()
		var handler SystemNodeEventHandler
		if err := yaml.Unmarshal([]byte(raw), &handler); err != nil {
			t.Fatal(err)
		}
		node, err := runtimeidentity.AdmitExecutableNodeDeclaration("scout", "router")
		if err != nil {
			t.Fatal(err)
		}
		handler, err = QualifySystemNodeHandlerRuleRefsForEvent(node, "work.requested", handler)
		if err != nil {
			t.Fatal(err)
		}
		out := make(map[string]runtimeidentity.DeclarationIdentity, len(handler.Rules))
		for _, rule := range handler.Rules {
			identity, ok := rule.DeclarationIdentity()
			if !ok {
				t.Fatalf("rule %q lacks declaration identity", rule.ID)
			}
			out[rule.ID] = identity
		}
		return out
	}

	before := decode("rules:\n  - {id: first, condition: payload.ready}\n  - {id: second, condition: else}\n")
	after := decode("rules:\n  - {id: second, condition: else}\n  - {id: first, condition: payload.ready}\n")
	if before["first"].Equal(after["first"]) || before["second"].Equal(after["second"]) {
		t.Fatalf("reordered rows retained positional identity: before=%#v after=%#v", before, after)
	}
}

func TestHandlerRuleIdentitySeparatesFlowsAndEvents(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte("rules:\n  - {condition: else}\n"), &handler); err != nil {
		t.Fatal(err)
	}
	qualify := func(flowPath, event string) runtimeidentity.DeclarationIdentity {
		t.Helper()
		node, err := runtimeidentity.AdmitExecutableNodeDeclaration(flowPath, "router")
		if err != nil {
			t.Fatal(err)
		}
		qualified, err := QualifySystemNodeHandlerRuleRefsForEvent(node, event, handler)
		if err != nil {
			t.Fatal(err)
		}
		identity, ok := qualified.Rules[0].DeclarationIdentity()
		if !ok {
			t.Fatal("qualified rule lacks declaration identity")
		}
		return identity
	}

	first := qualify("first", "work.requested")
	second := qualify("second", "work.requested")
	otherEvent := qualify("first", "work.retried")
	if first.Equal(second) || first.Equal(otherEvent) {
		t.Fatalf("distinct declaration sites collapsed: first=%#v second=%#v event=%#v", first, second, otherEvent)
	}
}

func TestAuthoredElementIDIsRetired(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte("rules:\n  - element_id: 00000000-0000-4000-8000-000000000001\n    condition: else\n"), &handler)
	if err == nil || !strings.Contains(err.Error(), "RETIRED: rule.element_id") {
		t.Fatalf("retired element_id error = %v", err)
	}
}
