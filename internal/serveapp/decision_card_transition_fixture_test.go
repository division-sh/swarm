package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Store-seeded mailbox/control fixtures still freeze real compiled carriers.
// Source-loaded lifecycle journeys do not use this helper.
func servedDecisionCardTransitionFixture(t *testing.T, source semanticview.Source, outcomes map[string]contracts.WorkflowGateOutcomePlan) map[string]contracts.CompiledTransition {
	t.Helper()
	graph, ok := semanticview.WorkflowStageTopology(source, ".")
	if !ok {
		t.Fatal("served decision fixture has no compiled root lifecycle")
	}
	result := make(map[string]contracts.CompiledTransition, len(outcomes))
	for verdict, outcome := range outcomes {
		transition, err := graph.AdmitTransition(contracts.WorkflowTransitionSite{DecisionID: "launch_review", Verdict: verdict}, "awaiting_review", outcome.AdvancesTo)
		if err != nil {
			t.Fatal(err)
		}
		result[verdict] = transition
	}
	return result
}
