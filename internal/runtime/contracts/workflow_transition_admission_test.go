package contracts

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"gopkg.in/yaml.v3"
)

func TestCompiledTransitionPreservesDistinctRuleCarriers(t *testing.T) {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration("scout", "router")
	if err != nil {
		t.Fatal(err)
	}
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte("rules:\n  - {id: first, condition: payload.ready, advances_to: done}\n  - {id: second, condition: else, advances_to: done}\n"), &handler); err != nil {
		t.Fatal(err)
	}
	handler, err = QualifySystemNodeHandlerRuleRefsForEvent(node, "work.requested", handler)
	if err != nil {
		t.Fatal(err)
	}
	graph := BuildWorkflowStageTopology("scout", "ready", []string{"ready", "done"}, []string{"done"}, []HandlerTransitionSemantic{{Node: node, EventType: "work.requested", Rules: handler.Rules}}, nil, nil)
	if len(graph.Edges) != 2 {
		t.Fatalf("distinct qualified rules collapsed: %#v", graph.Edges)
	}
	for _, carrier := range HandlerAdvanceCarriers(handler) {
		site := WorkflowTransitionSite{Node: node, HandlerEvent: "work.requested", AdvanceCarrier: carrier.Kind, RuleRef: carrier.RuleRef}
		admitted, err := graph.AdmitTransition(site, "ready", "done")
		if err != nil {
			t.Fatal(err)
		}
		if !admitted.Edge().RuleRef.Equal(carrier.RuleRef) {
			t.Fatal("admission substituted a sibling rule")
		}
		site.RuleRef = runtimeidentity.DeclarationIdentity{}
		if _, err := graph.AdmitTransition(site, "ready", "done"); err == nil {
			t.Fatal("missing selected rule identity admitted")
		}
	}
}

func TestCompiledTransitionRejectsForeignCarrierAndStages(t *testing.T) {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", "router")
	if err != nil {
		t.Fatal(err)
	}
	child, err := runtimeidentity.AdmitExecutableNodeDeclaration("child", "router")
	if err != nil {
		t.Fatal(err)
	}
	graph := BuildWorkflowStageTopology(".", "ready", []string{"ready", "working", "done"}, []string{"done"}, []HandlerTransitionSemantic{{Node: node, EventType: "work.requested", AdvancesTo: "done"}}, nil, nil)
	site := WorkflowTransitionSite{Node: node, HandlerEvent: "work.requested", AdvanceCarrier: HandlerAdvanceCarrierHandler}
	for _, stage := range []string{"ready", "working"} {
		if _, err := graph.AdmitTransition(site, stage, "done"); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name, from, to string
		site           WorkflowTransitionSite
	}{
		{"foreign_node", "ready", "done", WorkflowTransitionSite{Node: child, HandlerEvent: site.HandlerEvent, AdvanceCarrier: site.AdvanceCarrier}},
		{"foreign_handler", "ready", "done", WorkflowTransitionSite{Node: node, HandlerEvent: "another.event", AdvanceCarrier: site.AdvanceCarrier}},
		{"wrong_carrier", "ready", "done", WorkflowTransitionSite{Node: node, HandlerEvent: site.HandlerEvent, AdvanceCarrier: HandlerAdvanceCarrierRules}},
		{"unknown_source", "missing", "done", site},
		{"unknown_target", "ready", "missing", site},
		{"terminal_source", "done", "ready", site},
		{"arbitrary_seed", "", "done", site},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := graph.AdmitTransition(tc.site, tc.from, tc.to); err == nil {
				t.Fatal("invalid transition admitted")
			}
		})
	}
}
