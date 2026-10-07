package contracts

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestTerminateBelongsToExactAdvanceCarrier(t *testing.T) {
	node, err := identity.AdmitExecutableNodeDeclaration(".", "router")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		root, rule bool
	}{
		{"root_only", true, false}, {"rule_only", false, true}, {"both", true, true}, {"neither", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var handler SystemNodeEventHandler
			if err := decodeNodeTestYAML([]byte(fmt.Sprintf("advances_to: done\nterminate: %t\nrules:\n  - id: override\n    else: true\n    advances_to: other\n    terminate: %t\n", tc.root, tc.rule)), &handler); err != nil {
				t.Fatal(err)
			}
			handler, err = QualifySystemNodeHandlerRuleRefsForEvent(node, "work.finished", handler)
			if err != nil {
				t.Fatal(err)
			}
			semantic := HandlerTransitionSemantic{Node: node, EventType: "work.finished", AdvancesTo: handler.AdvancesTo, Terminate: handler.Terminate, Rules: handler.Rules}
			graph := BuildWorkflowStageTopology(".", "ready", []string{"ready", "other", "done"}, []string{"done"}, []HandlerTransitionSemantic{semantic}, nil, nil)
			for _, carrier := range HandlerAdvanceCarriers(handler) {
				want := tc.root
				if carrier.Kind == HandlerAdvanceCarrierRules {
					want = tc.rule
				}
				if carrier.Terminate != want {
					t.Fatalf("carrier %s borrowed/lost cancellation: got=%t want=%t", carrier.Kind, carrier.Terminate, want)
				}
				compiled, err := graph.AdmitTransition(WorkflowTransitionSite{Node: node, HandlerEvent: "work.finished", AdvanceCarrier: carrier.Kind, RuleRef: carrier.RuleRef}, "ready", carrier.AdvancesTo)
				if err != nil {
					t.Fatal(err)
				}
				if compiled.Edge().Terminate != want {
					t.Fatalf("compiled %s borrowed/lost termination: got=%t want=%t", carrier.Kind, compiled.Edge().Terminate, want)
				}
				raw, err := json.Marshal(compiled)
				if err != nil {
					t.Fatal(err)
				}
				var restored CompiledTransition
				if err := json.Unmarshal(raw, &restored); err != nil || restored != compiled {
					t.Fatalf("termination evidence failed round trip: %s err=%v", raw, err)
				}
			}
		})
	}
}
