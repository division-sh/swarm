package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

// Store-seeded mailbox/control fixtures still freeze real compiled carriers.
// Source-loaded lifecycle journeys do not use this helper.
func servedDecisionCardTransitionFixture(t *testing.T, outcomes map[string]contracts.WorkflowGateOutcomePlan) map[string]contracts.CompiledTransition {
	t.Helper()
	graph := contracts.BuildWorkflowStageTopology(".", "awaiting_review", []string{"awaiting_review", "done", "rework"}, []string{"done"}, nil, nil, nil,
		[]contracts.WorkflowGatePlan{{FlowID: ".", Stage: "awaiting_review", Decision: "launch_review", Outcomes: outcomes}})
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
