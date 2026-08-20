package semanticview_test

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestFanInBarrierContractDerivesEffectiveJoinPlan(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical fan-in barrier: %v", err)
	}
	raw := requireSingleJoinPlan(t, bundle.WorkflowJoins())
	if raw.Spec.Members.By != "" || raw.Spec.Window == nil || raw.Spec.Window.By != "" {
		t.Fatalf("authored join unexpectedly contains derived fields: %#v", raw.Spec)
	}

	effective := requireSingleJoinPlan(t, semanticview.Wrap(bundle).WorkflowJoins())
	if effective.Spec.Members.By != "payload.operating_id" || effective.Derivation.MembersByFrom != "resolution.dedup_by" {
		t.Fatalf("effective member derivation = %#v", effective)
	}
	if effective.Spec.Window == nil || effective.Spec.Window.By != "payload.period_id" || effective.Derivation.WindowByFrom != "resolution.window" {
		t.Fatalf("effective window derivation = %#v", effective)
	}
	if effective.Derivation.FanInPin != "operating_reported" {
		t.Fatalf("effective fan-in pin = %q", effective.Derivation.FanInPin)
	}

	rawAfter := requireSingleJoinPlan(t, bundle.WorkflowJoins())
	if rawAfter.Spec.Members.By != "" || rawAfter.Spec.Window == nil || rawAfter.Spec.Window.By != "" {
		t.Fatalf("effective lowering mutated authored join: %#v", rawAfter.Spec)
	}
}

func TestWorkflowJoinPlanForRefDistinguishesRootAndSameLeafFlowDeclarations(t *testing.T) {
	spec := runtimecontracts.JoinSpec{ID: "awaiting", Stage: "awaiting"}
	root, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", "", "join-node")
	if err != nil {
		t.Fatal(err)
	}
	orders, err := runtimeidentity.AdmitExecutableNodeDeclaration("flows/orders", "orders", "join-node")
	if err != nil {
		t.Fatal(err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{Joins: []runtimecontracts.WorkflowJoinPlan{
		{Node: root, HandlerEvent: "item.completed", Spec: spec},
		{Node: orders, HandlerEvent: "item.completed", Spec: spec},
	}}}
	source := semanticview.Wrap(bundle)
	for _, node := range []runtimeidentity.ExecutableNode{root, orders} {
		ref, err := timeridentity.NewJoinRef(node, "item.completed", "awaiting", "awaiting", "")
		if err != nil {
			t.Fatal(err)
		}
		plan, ok := semanticview.WorkflowJoinPlanForRef(source, ref)
		if !ok || !plan.Node.Equal(node) {
			t.Fatalf("plan for node %#v = %#v, ok=%v", node, plan, ok)
		}
	}
	hostileNode, err := runtimeidentity.AdmitExecutableNodeDeclaration("flows/unrelated", "unrelated", "join-node")
	if err != nil {
		t.Fatal(err)
	}
	hostile, err := timeridentity.NewJoinRef(hostileNode, "item.completed", "awaiting", "awaiting", "")
	if err != nil {
		t.Fatal(err)
	}
	if plan, ok := semanticview.WorkflowJoinPlanForRef(source, hostile); ok {
		t.Fatalf("unrelated same-leaf declaration resolved: %#v", plan)
	}
}

func TestWorkflowJoinPlanForRefPreservesSameFlowPackageDeclarationsInEitherOrder(t *testing.T) {
	spec := runtimecontracts.JoinSpec{ID: "awaiting", Stage: "awaiting"}
	first, err := runtimeidentity.AdmitExecutableNodeDeclaration("packages/a", "orders", "shared")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtimeidentity.AdmitExecutableNodeDeclaration("packages/b", "orders", "shared")
	if err != nil {
		t.Fatal(err)
	}
	for _, plans := range [][]runtimecontracts.WorkflowJoinPlan{
		{{Node: first, HandlerEvent: "item.completed", Spec: spec}, {Node: second, HandlerEvent: "item.completed", Spec: spec}},
		{{Node: second, HandlerEvent: "item.completed", Spec: spec}, {Node: first, HandlerEvent: "item.completed", Spec: spec}},
	} {
		source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{Joins: plans}})
		for _, node := range []runtimeidentity.ExecutableNode{first, second} {
			ref, err := timeridentity.NewJoinRef(node, "item.completed", "awaiting", "awaiting", "")
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
