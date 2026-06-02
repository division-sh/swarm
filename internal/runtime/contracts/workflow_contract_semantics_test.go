package contracts

import "testing"

func TestWorkflowSemanticsRuleActionUsesHandlerAdvancesToFallback(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Nodes: map[string]SystemNodeContract{
			"review_node": {
				EventHandlers: map[string]SystemNodeEventHandler{
					"expense.submitted": {
						AdvancesTo: "awaiting_review",
						Rules: []HandlerRuleEntry{
							{
								ID:        "needs-human",
								Condition: "payload.amount > 100",
								Action:    ActionSpec{ID: "request_review"},
							},
							{
								ID:        "auto-approve",
								Condition: "else",
							},
						},
					},
				},
			},
		},
	}

	populateWorkflowSemantics(bundle)

	var found bool
	for _, transition := range bundle.WorkflowTransitions() {
		if transition.ID != "needs-human" {
			continue
		}
		found = true
		if transition.To != "awaiting_review" {
			t.Fatalf("rule transition To = %q, want handler advances_to fallback", transition.To)
		}
		if got, want := transition.Actions, []string{"request_review"}; len(got) != len(want) || got[0] != want[0] {
			t.Fatalf("rule transition Actions = %#v, want %#v", got, want)
		}
	}
	if !found {
		t.Fatalf("missing rule transition for rule-level action using handler advances_to fallback")
	}

	for _, transition := range bundle.WorkflowTransitions() {
		if transition.ID == "auto-approve" {
			t.Fatalf("derived fallback transition for rule without action: %#v", transition)
		}
	}
}
