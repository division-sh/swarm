package engine

import (
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func a2DeadlineHandler(t *testing.T) (identity.ExecutableNode, c.SystemNodeEventHandler) {
	t.Helper()
	node := testRootExecutableNode(t, "collector")
	count := 2
	handler, err := completeSemanticFixtureHandlerRuleIdentity(node, "arrived", c.SystemNodeEventHandler{Join: &c.JoinSpec{
		Stage: "waiting", Members: c.JoinMembersSpec{Count: &count, By: "payload.member"}, Output: "payload.result",
		Deadline:        &c.JoinDeadlineSpec{After: "1h", From: c.JoinDeadlineFromStageEntry},
		OnCompleteFound: true, OnComplete: c.HandlerRuleEntry{AdvancesTo: "completed"},
		OnDeadlineFound: true, OnDeadline: c.HandlerRuleEntry{AdvancesTo: "expired", DataAccumulation: c.WorkflowDataAccumulation{Writes: []c.WorkflowDataWrite{{TargetRef: "entity.expired", Value: c.LiteralExpression(true)}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return node, handler
}

func TestA2CommittedActivityRuleConsumesExactDeadlineOutcome(t *testing.T) {
	_, handler := a2DeadlineHandler(t)
	deadlineRef, _ := handler.Join.OnDeadline.DeclarationIdentity()
	selection, err := handlerselection.Selected(handlerselection.ContextJoinTimeout, deadlineRef, "")
	if err != nil {
		t.Fatal(err)
	}
	got, err := committedHandlerRule(handler, selection)
	if err != nil || got == nil || got.AdvancesTo != "expired" {
		t.Fatalf("committed deadline outcome: %#v %v", got, err)
	}
	completeRef, _ := handler.Join.OnComplete.DeclarationIdentity()
	foreign, err := handlerselection.Selected(handlerselection.ContextJoinTimeout, completeRef, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := committedHandlerRule(handler, foreign); err == nil {
		t.Fatal("deadline selection borrowed completion rule")
	}
	for _, without := range []func(*c.JoinSpec){func(j *c.JoinSpec) { j.Deadline = nil }, func(j *c.JoinSpec) { j.OnDeadlineFound = false }} {
		bad := handler
		join := *handler.Join
		without(&join)
		bad.Join = &join
		if _, err := committedHandlerRule(bad, selection); err == nil {
			t.Fatal("undeclared deadline produced committed activity rule")
		}
	}
}

func TestA2EntityAssignmentDeadlineCarrierRequiresDeclaredOutcome(t *testing.T) {
	_, handler := a2DeadlineHandler(t)
	if entityAssignmentSelectionStep(c.HandlerAdvanceCarrierJoinOnDeadline) != StepJoin {
		t.Fatal("deadline carrier assigned to wrong execution step")
	}
	count := func(handler c.SystemNodeEventHandler) int {
		n := 0
		for _, outcome := range entityAssignmentOutcomes(handler) {
			if outcome.kind != c.HandlerAdvanceCarrierJoinOnDeadline {
				continue
			}
			n++
			ref, _ := handler.Join.OnDeadline.DeclarationIdentity()
			if outcome.ref != ref || outcome.rule == nil || outcome.rule.AdvancesTo != "expired" || len(outcome.rule.DataAccumulation.Writes) != 1 {
				t.Fatal("assignment analysis lost exact deadline outcome")
			}
		}
		return n
	}
	if count(handler) != 1 {
		t.Fatal("declared deadline missing in assignment outcomes")
	}
	for _, without := range []func(*c.JoinSpec){func(j *c.JoinSpec) { j.Deadline = nil }, func(j *c.JoinSpec) { j.OnDeadlineFound = false }} {
		bad := handler
		join := *handler.Join
		without(&join)
		bad.Join = &join
		if count(bad) != 0 {
			t.Fatal("absence of deadline manufactured an assignment outcome")
		}
	}
}

func TestA2CompiledDeadlineTransitionConsumesExactCarrier(t *testing.T) {
	node, handler := a2DeadlineHandler(t)
	graph := c.BuildWorkflowStageTopology(".", "waiting", []string{"waiting", "completed", "expired"}, []string{"completed", "expired"}, []c.HandlerTransitionSemantic{{Node: node, EventType: "arrived", Join: handler.Join}}, nil, nil)
	exec := &Executor{deps: RuntimeDependencies{Source: semanticview.Wrap(&c.WorkflowContractBundle{Semantics: c.WorkflowSemanticView{StageTopologies: map[string]c.WorkflowStageTopology{".": graph}}})}}
	makeFrame := func() executionFrame {
		ref, _ := handler.Join.OnDeadline.DeclarationIdentity()
		fact, err := handlerselection.Selected(handlerselection.ContextJoinTimeout, ref, "")
		if err != nil {
			t.Fatal(err)
		}
		return executionFrame{req: ExecutionRequest{Node: node, ExecutionFlowID: identity.NormalizeFlowID("."), HandlerEventKey: "arrived", Handler: handler}, result: ExecutionResult{CurrentState: "waiting", HandlerRuleSelection: handlerselection.Resolved(fact)}, rule: &handler.Join.OnDeadline, ruleSource: handlerRuleSourceJoinTimeout}
	}
	frame := makeFrame()
	if err := exec.admitSelectedTransition(&frame, "expired"); err != nil {
		t.Fatal(err)
	}
	compiled, ok := frame.result.StateMutation.Transition.Compiled()
	if !ok || compiled.Edge().AdvanceCarrier != c.HandlerAdvanceCarrierJoinOnDeadline || compiled.Edge().EventType != "platform.join_timeout" || compiled.Edge().TimerID != handler.Join.EffectiveID() {
		t.Fatalf("wrong deadline edge: %#v", compiled.Edge())
	}
	for _, change := range []func(*executionFrame){
		func(f *executionFrame) { join := *f.req.Handler.Join; join.Deadline = nil; f.req.Handler.Join = &join },
		func(f *executionFrame) {
			join := *f.req.Handler.Join
			join.OnDeadlineFound = false
			f.req.Handler.Join = &join
		},
		func(f *executionFrame) { f.rule = &handler.Join.OnComplete },
	} {
		bad := makeFrame()
		change(&bad)
		if err := exec.admitSelectedTransition(&bad, "expired"); err == nil || bad.result.StateMutation.Transition != nil {
			t.Fatal("deadline transition borrowed missing/foreign carrier")
		}
	}
}
