package contracts

import (
	"reflect"
	"testing"
)

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

func TestWorkflowSemanticsDerivesTopLevelAndAccumulateCompletionTransitions(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Nodes: map[string]SystemNodeContract{
			"fan-in-node": {
				EventHandlers: map[string]SystemNodeEventHandler{
					"component.scaffolded": {
						OnComplete: []HandlerRuleEntry{{
							ID:         "top-complete",
							Condition:  "true",
							AdvancesTo: "top_review",
						}},
						Accumulate: &AccumulateSpec{
							OnComplete: []HandlerRuleEntry{{
								ID:         "accumulate-complete",
								Condition:  "accumulated.count >= 3",
								AdvancesTo: "launch_review",
							}},
						},
					},
				},
			},
		},
	}

	populateWorkflowSemantics(bundle)

	transitions := map[string]WorkflowTransitionContract{}
	for _, transition := range bundle.WorkflowTransitions() {
		transitions[transition.ID] = transition
	}
	if got := transitions["top-complete"].To; got != "top_review" {
		t.Fatalf("top-level on_complete transition To = %q, want top_review; transitions=%#v", got, transitions)
	}
	if got := transitions["accumulate-complete"].To; got != "launch_review" {
		t.Fatalf("accumulate.on_complete transition To = %q, want launch_review; transitions=%#v", got, transitions)
	}
}

func TestWorkflowSemanticsDerivesEffectiveSystemNodeFacts(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Nodes: map[string]SystemNodeContract{
			"worker": {
				EventHandlers: map[string]SystemNodeEventHandler{
					"task.start": {
						DataAccumulation: WorkflowDataAccumulation{
							Writes: []WorkflowDataWrite{{Field: "status"}},
						},
						Emit: EmitSpec{Event: "task.done"},
					},
					"task.review": {
						Rules: []HandlerRuleEntry{{Emit: EmitSpec{Event: "task.approved"}}},
					},
					"task.timeout": {
						Accumulate: &AccumulateSpec{
							OnTimeout: &HandlerRuleEntry{Emit: EmitSpec{Event: "task.expired"}},
						},
					},
					"task.rules": {
						Rules: []HandlerRuleEntry{
							{
								ID:        "priority",
								Condition: "payload.priority == 'urgent'",
								Emit:      EmitSpec{Event: "task.rules.then"},
							},
							{
								ID:        "fallback",
								Condition: "else",
								Emit:      EmitSpec{Event: "task.rules.else"},
							},
						},
						FanOut: &FanOutSpec{Emit: EmitSpec{Event: "task.child"}},
					},
				},
			},
		},
	}

	populateWorkflowSemantics(bundle)

	node := bundle.Nodes["worker"]
	if node.ID != "worker" {
		t.Fatalf("normalized node ID = %q, want worker", node.ID)
	}
	if node.ExecutionType != SystemNodeExecutionType {
		t.Fatalf("normalized execution_type = %q, want %q", node.ExecutionType, SystemNodeExecutionType)
	}
	effective, ok := bundle.NodeEffectiveSemantics("worker")
	if !ok {
		t.Fatal("missing effective node semantics")
	}
	if got, want := effective.ID, "worker"; got != want {
		t.Fatalf("effective ID = %q, want %q", got, want)
	}
	if got, want := effective.ExecutionType, SystemNodeExecutionType; got != want {
		t.Fatalf("effective execution type = %q, want %q", got, want)
	}
	if got, want := effective.RuntimeSubscriptions, []string{"accumulate.timeout", "task.review", "task.rules", "task.start", "task.timeout"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("effective subscriptions = %#v, want %#v", got, want)
	}
	if got, want := effective.Produces, []string{"task.approved", "task.child", "task.done", "task.expired", "task.rules.else", "task.rules.then"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("effective produces = %#v, want %#v", got, want)
	}
	handler, ok := bundle.NodeEventHandler("worker", "task.start")
	if !ok {
		t.Fatal("missing task.start handler")
	}
	if got, want := handler.DataAccumulation.SourceEvent, "task.start"; got != want {
		t.Fatalf("effective source_event = %q, want %q", got, want)
	}
	transition, ok := bundle.DerivedHandlerTransition("worker", "task.start")
	if !ok {
		t.Fatal("missing task.start transition")
	}
	if got, want := transition.DataAccumulation.SourceEvent, "task.start"; got != want {
		t.Fatalf("transition source_event = %q, want %q", got, want)
	}
}
