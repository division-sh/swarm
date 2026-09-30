package semanticview_test

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestWorkflowJoinViewPreservesCompilerOwnedMembership(t *testing.T) {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration("collector", "join-node")
	if err != nil {
		t.Fatal(err)
	}
	count := 2
	spec := runtimecontracts.JoinSpec{ID: "collected", Stage: "waiting",
		Members:  runtimecontracts.JoinMembersSpec{Count: &count, By: "payload.member_id"},
		Deadline: &runtimecontracts.JoinDeadlineSpec{After: "1h", From: runtimecontracts.JoinDeadlineFromStageEntry}}
	bundle := &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{Joins: []runtimecontracts.WorkflowJoinPlan{
		{Node: node, HandlerEvent: "item.completed", Mode: runtimecontracts.WorkflowJoinModeArrival, Spec: spec},
	}}}
	effective := requireSingleJoinPlan(t, semanticview.Wrap(bundle).WorkflowJoins())
	if effective.Spec.Members.By != "payload.member_id" || *effective.Spec.Members.Count != 2 {
		t.Fatalf("view replaced compiler membership: %#v", effective)
	}
	*effective.Spec.Members.Count = 7
	effective.Spec.Deadline.After = "9h"
	if raw := requireSingleJoinPlan(t, bundle.WorkflowJoins()); *raw.Spec.Members.Count != 2 || raw.Spec.Deadline.After != "1h" {
		t.Fatalf("view mutated compiler-owned declaration: %#v", raw)
	}
}

func TestWorkflowJoinPlanForRefDistinguishesRootAndSameLeafFlowDeclarations(t *testing.T) {
	spec := runtimecontracts.JoinSpec{ID: "awaiting", Stage: "awaiting"}
	root, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", "join-node")
	if err != nil {
		t.Fatal(err)
	}
	orders, err := runtimeidentity.AdmitExecutableNodeDeclaration("orders", "join-node")
	if err != nil {
		t.Fatal(err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{Joins: []runtimecontracts.WorkflowJoinPlan{
		{Node: root, HandlerEvent: "item.completed", Mode: runtimecontracts.WorkflowJoinModeArrival, Spec: spec},
		{Node: orders, HandlerEvent: "item.completed", Mode: runtimecontracts.WorkflowJoinModeArrival, Spec: spec},
	}}}
	source := semanticview.Wrap(bundle)
	for _, node := range []runtimeidentity.ExecutableNode{root, orders} {
		ref, err := timeridentity.NewJoinRef(node, "item.completed", "awaiting", "awaiting")
		if err != nil {
			t.Fatal(err)
		}
		plan, ok := semanticview.WorkflowJoinPlanForRef(source, ref)
		if !ok || !plan.Node.Equal(node) {
			t.Fatalf("plan for node %#v = %#v, ok=%v", node, plan, ok)
		}
	}
	hostileNode, err := runtimeidentity.AdmitExecutableNodeDeclaration("unrelated", "join-node")
	if err != nil {
		t.Fatal(err)
	}
	hostile, err := timeridentity.NewJoinRef(hostileNode, "item.completed", "awaiting", "awaiting")
	if err != nil {
		t.Fatal(err)
	}
	if plan, ok := semanticview.WorkflowJoinPlanForRef(source, hostile); ok {
		t.Fatalf("unrelated same-leaf declaration resolved: %#v", plan)
	}
}

func TestWorkflowJoinPlanForRefPreservesDistinctFlowDeclarationsInEitherOrder(t *testing.T) {
	spec := runtimecontracts.JoinSpec{ID: "awaiting", Stage: "awaiting"}
	first, err := runtimeidentity.AdmitExecutableNodeDeclaration("orders/a", "shared")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtimeidentity.AdmitExecutableNodeDeclaration("orders/b", "shared")
	if err != nil {
		t.Fatal(err)
	}
	for _, plans := range [][]runtimecontracts.WorkflowJoinPlan{
		{{Node: first, HandlerEvent: "item.completed", Mode: runtimecontracts.WorkflowJoinModeArrival, Spec: spec}, {Node: second, HandlerEvent: "item.completed", Mode: runtimecontracts.WorkflowJoinModeArrival, Spec: spec}},
		{{Node: second, HandlerEvent: "item.completed", Mode: runtimecontracts.WorkflowJoinModeArrival, Spec: spec}, {Node: first, HandlerEvent: "item.completed", Mode: runtimecontracts.WorkflowJoinModeArrival, Spec: spec}},
	} {
		source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{Joins: plans}})
		for _, node := range []runtimeidentity.ExecutableNode{first, second} {
			ref, err := timeridentity.NewJoinRef(node, "item.completed", "awaiting", "awaiting")
			if err != nil {
				t.Fatal(err)
			}
			plan, ok := semanticview.WorkflowJoinPlanForRef(source, ref)
			if !ok || !plan.Node.Equal(node) {
				t.Fatalf("plan for %s = %#v, ok=%v", node.Key(), plan, ok)
			}
		}
	}
}

func requireSingleJoinPlan(t *testing.T, plans []runtimecontracts.WorkflowJoinPlan) runtimecontracts.WorkflowJoinPlan {
	t.Helper()
	if len(plans) != 1 {
		t.Fatalf("join plans = %#v, want exactly one", plans)
	}
	return plans[0]
}
