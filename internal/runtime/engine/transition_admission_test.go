package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type transitionMutationRecorder struct {
	mutations []EngineMutation
}

func (r *transitionMutationRecorder) CommitEngineMutation(_ context.Context, mutation EngineMutation) (CommittedEngineMutation, error) {
	if err := mutation.ValidateTransitionEvidence(); err != nil {
		return CommittedEngineMutation{}, err
	}
	r.mutations = append(r.mutations, mutation)
	return CommittedEngineMutation{EmitIntents: mutation.EmitIntents}, nil
}

func transitionTestRequest(t testing.TB, node identity.ExecutableNode, event string, handler contracts.SystemNodeEventHandler, state string) ExecutionRequest {
	t.Helper()
	return ExecutionRequest{
		EntityID: "entity-1", Node: node, ExecutionFlowID: identity.NormalizeFlowID(node.FlowPath()), HandlerEventKey: event,
		Route:   flowidentity.DeriveRoute(node.FlowPath(), semanticExecutionFixtureRunID),
		Handler: handler, State: testStateSnapshot(state, map[string]any{}, nil, nil),
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("transition-"+event), events.EventType(event), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
	}
}

func transitionTestExecutor(t testing.TB, source semanticview.Source) (*Executor, *transitionMutationRecorder) {
	t.Helper()
	recorder := &transitionMutationRecorder{}
	exec, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: stubStateRepo{}, MutationOwner: recorder, Locker: stubLocker{}, Dispatcher: stubDispatcher{}, WorkflowLifecycle: &testWorkflowLifecycleOwner{}}, stubEvaluator{bools: map[string]bool{"true": true, "false": false}})
	if err != nil {
		t.Fatal(err)
	}
	return exec, recorder
}

func TestExecutorCompiledTransitionRejectsMissingExactCarrier(t *testing.T) {
	node := testFlowExecutableNode(t, "orders", "worker")
	handler := contracts.SystemNodeEventHandler{AdvancesTo: "done"}
	graph := contracts.BuildWorkflowStageTopology("orders", "ready", []string{"ready", "working", "done"}, []string{"done"},
		[]contracts.HandlerTransitionSemantic{{Node: node, EventType: "work.requested", AdvancesTo: "done"}}, nil, nil)
	sibling := contracts.BuildWorkflowStageTopology("sibling", "ready", []string{"ready", "foreign", "done"}, []string{"ready"},
		[]contracts.HandlerTransitionSemantic{{Node: testFlowExecutableNode(t, "sibling", "worker"), EventType: "work.requested", AdvancesTo: "foreign"}}, nil, nil)
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{
		StageTopologies: map[string]contracts.WorkflowStageTopology{"orders": graph, "sibling": sibling},
	}})
	for _, tc := range []struct {
		name   string
		change func(*ExecutionRequest)
	}{
		{"unknown_source", func(r *ExecutionRequest) { r.State.CurrentState = "unknown" }},
		{"empty_source", func(r *ExecutionRequest) { r.State.CurrentState = "" }},
		{"unknown_target", func(r *ExecutionRequest) { r.Handler.AdvancesTo = "unknown" }},
		{"sibling_only_target", func(r *ExecutionRequest) { r.Handler.AdvancesTo = "foreign" }},
		{"local_terminal_exit", func(r *ExecutionRequest) { r.State.CurrentState = "done"; r.Handler.AdvancesTo = "working" }},
		{"different_handler_same_pair", func(r *ExecutionRequest) { r.HandlerEventKey = "work.other" }},
		{"different_node_same_pair", func(r *ExecutionRequest) { r.Node = testFlowExecutableNode(t, "orders", "other") }},
		{"foreign_flow_same_pair", func(r *ExecutionRequest) { r.ExecutionFlowID = identity.NormalizeFlowID("sibling") }},
		{"missing_flow_graph", func(r *ExecutionRequest) { r.ExecutionFlowID = identity.NormalizeFlowID("missing") }},
		{"rule_cannot_borrow_handler_pair", func(r *ExecutionRequest) {
			r.Handler = contracts.SystemNodeEventHandler{Rules: []contracts.HandlerRuleEntry{{Condition: "else", AdvancesTo: "done"}}}
		}},
		{"self_target_requires_carrier", func(r *ExecutionRequest) { r.Handler.AdvancesTo = "ready" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, recorder := transitionTestExecutor(t, source)
			req := transitionTestRequest(t, node, "work.requested", handler, "ready")
			tc.change(&req)
			result, err := exec.ExecuteSemanticFixture(context.Background(), req)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("error = %v, want exact graph admission rejection", err)
			}
			if len(recorder.mutations) != 0 || result.StateMutation.Transition != nil {
				t.Fatalf("rejected carrier committed mutation or transition: %#v / %#v", recorder.mutations, result.StateMutation.Transition)
			}
		})
	}
	// A sibling terminal with the same name must not forbid this flow's source.
	for _, state := range []string{"ready", "working"} {
		exec, recorder := transitionTestExecutor(t, source)
		result, err := exec.ExecuteSemanticFixture(context.Background(), transitionTestRequest(t, node, "work.requested", handler, state))
		if err != nil || len(recorder.mutations) != 1 || result.StateMutation.Transition == nil {
			t.Fatalf("legal ordinary source %s: result=%#v mutations=%d err=%v", state, result, len(recorder.mutations), err)
		}
		if err := result.StateMutation.Transition.ValidateAgainst(graph); err != nil {
			t.Fatal(err)
		}
	}
}

func TestExecutorCompiledOrdinaryTransitionRetainsSelectedCarrier(t *testing.T) {
	node := testFlowExecutableNode(t, "orders", "worker")
	for _, tc := range []struct {
		name          string
		handler       contracts.SystemNodeEventHandler
		carrier       contracts.HandlerAdvanceCarrierKind
		selectedIndex int
	}{
		{"direct", contracts.SystemNodeEventHandler{AdvancesTo: "done"}, contracts.HandlerAdvanceCarrierHandler, -1},
		{"inherited", contracts.SystemNodeEventHandler{AdvancesTo: "done", Rules: []contracts.HandlerRuleEntry{{ID: "selected", Condition: "else", Emit: contracts.EmitSpec{Event: "work.accepted"}}}}, contracts.HandlerAdvanceCarrierHandler, 0},
		{"first_same_pair", contracts.SystemNodeEventHandler{Rules: []contracts.HandlerRuleEntry{{ID: "first", Condition: "true", AdvancesTo: "done"}, {ID: "second", Condition: "else", AdvancesTo: "done"}}}, contracts.HandlerAdvanceCarrierRules, 0},
		{"second_same_pair", contracts.SystemNodeEventHandler{Rules: []contracts.HandlerRuleEntry{{ID: "first", Condition: "false", AdvancesTo: "done"}, {ID: "second", Condition: "else", AdvancesTo: "done"}}}, contracts.HandlerAdvanceCarrierRules, 1},
		{"completion", contracts.SystemNodeEventHandler{OnComplete: []contracts.HandlerRuleEntry{{ID: "complete", Condition: "else", AdvancesTo: "done"}}}, contracts.HandlerAdvanceCarrierOnComplete, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, err := completeSemanticFixtureHandlerRuleIdentity(node, "work.requested", tc.handler)
			if err != nil {
				t.Fatal(err)
			}
			graph := contracts.BuildWorkflowStageTopology("orders", "ready", []string{"ready", "working", "done"}, []string{"done"},
				[]contracts.HandlerTransitionSemantic{{Node: node, EventType: "work.requested", AdvancesTo: handler.AdvancesTo, Rules: handler.Rules, OnComplete: handler.OnComplete}}, nil, nil)
			source := semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{StageTopologies: map[string]contracts.WorkflowStageTopology{"orders": graph}}})
			for _, state := range []string{"ready", "working"} {
				exec, recorder := transitionTestExecutor(t, source)
				result, err := exec.ExecuteSemanticFixture(context.Background(), transitionTestRequest(t, node, "work.requested", handler, state))
				if err != nil {
					t.Fatal(err)
				}
				cause := result.StateMutation.Transition
				if cause == nil || len(recorder.mutations) != 1 || !reflect.DeepEqual(recorder.mutations[0].State.Transition, cause) {
					t.Fatalf("missing atomic cause: %#v", result)
				}
				compiled, ok := cause.Compiled()
				if !ok || compiled.Edge().AdvanceCarrier != tc.carrier || compiled.Edge().Node != node || compiled.Edge().HandlerEvent != "work.requested" || compiled.Edge().From != state {
					t.Fatalf("wrong carrier: %#v", compiled.Edge())
				}
				if tc.selectedIndex < 0 {
					if cause.RuleSelection().Disposition() != handlerselection.DispositionNotApplicable {
						t.Fatalf("direct handler invented selection: %#v", cause.RuleSelection())
					}
					continue
				}
				rules := handler.Rules
				if tc.carrier == contracts.HandlerAdvanceCarrierOnComplete {
					rules = handler.OnComplete
				}
				ref, _ := rules[tc.selectedIndex].DeclarationIdentity()
				if !cause.RuleSelection().Ref().Equal(ref) {
					t.Fatalf("selection = %#v, want %s", cause.RuleSelection(), ref.Key())
				}
				if tc.carrier == contracts.HandlerAdvanceCarrierHandler {
					if compiled.Edge().RuleRef.Valid() {
						t.Fatal("inherited handler target manufactured a rule-owned advance")
					}
				} else if !compiled.Edge().RuleRef.Equal(ref) {
					t.Fatalf("carrier selected another same-pair rule: %#v", compiled.Edge())
				}
			}
		})
	}
}

func TestExecutorCompiledTransitionNoOpHasNoCause(t *testing.T) {
	node := testFlowExecutableNode(t, "orders", "worker")
	for _, tc := range []struct {
		name    string
		handler contracts.SystemNodeEventHandler
	}{
		{"self", contracts.SystemNodeEventHandler{AdvancesTo: "ready"}},
		{"no_advance", contracts.SystemNodeEventHandler{}},
		{"emit_only", contracts.SystemNodeEventHandler{Emit: contracts.EmitSpec{Event: "work.noted"}}},
		{"unmatched_rule", contracts.SystemNodeEventHandler{Rules: []contracts.HandlerRuleEntry{{Condition: "false", AdvancesTo: "done"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := completeSemanticFixtureHandlerRuleIdentity(node, "work.requested", tc.handler)
			if err != nil {
				t.Fatal(err)
			}
			graph := contracts.BuildWorkflowStageTopology("orders", "ready", []string{"ready", "done"}, nil, []contracts.HandlerTransitionSemantic{{Node: node, EventType: "work.requested", AdvancesTo: h.AdvancesTo, Rules: h.Rules}}, nil, nil)
			exec, recorder := transitionTestExecutor(t, semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{StageTopologies: map[string]contracts.WorkflowStageTopology{"orders": graph}}}))
			result, err := exec.ExecuteSemanticFixture(context.Background(), transitionTestRequest(t, node, "work.requested", h, "ready"))
			if err != nil {
				t.Fatal(err)
			}
			if result.StateMutation.Transition != nil || result.NextState != "ready" {
				t.Fatalf("no-op invented stage entry: %#v", result)
			}
			for _, mutation := range recorder.mutations {
				if mutation.State.Transition != nil {
					t.Fatal("no-op committed transition")
				}
			}
			if tc.name == "emit_only" && len(result.EmitIntents) != 1 {
				t.Fatalf("emit-only lost publication: %#v", result.EmitIntents)
			}
		})
	}
}

func TestExecutorCompiledGuardDispositionHasExplicitCause(t *testing.T) {
	node := testFlowExecutableNode(t, "orders", "worker")
	graph := contracts.BuildWorkflowStageTopology("orders", "ready", []string{"ready", "done", "killed"}, []string{"killed"}, nil, nil, nil)
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{
		FlowStates: map[string][]string{"orders": {"ready", "done", "killed"}}, FlowTerminal: map[string][]string{"orders": {"killed"}},
		StageTopologies: map[string]contracts.WorkflowStageTopology{"orders": graph},
	}})
	for _, disposition := range []string{"kill", "reject", "discard"} {
		t.Run(disposition, func(t *testing.T) {
			exec, recorder := transitionTestExecutor(t, source)
			handler := contracts.SystemNodeEventHandler{Guard: &contracts.GuardSpec{Check: "false", OnFail: disposition}, AdvancesTo: "done", Emit: contracts.EmitSpec{Event: "must.not.emit"}}
			result, err := exec.ExecuteSemanticFixture(context.Background(), transitionTestRequest(t, node, "work.requested", handler, "ready"))
			if err != nil {
				t.Fatal(err)
			}
			if len(result.EmitIntents) != 0 {
				t.Fatal("failed guard emitted ordinary work")
			}
			cause := result.StateMutation.Transition
			if disposition != "kill" {
				if cause != nil || result.NextState != "ready" {
					t.Fatalf("%s invented transition: %#v", disposition, result)
				}
				return
			}
			if cause == nil || cause.FlowID() != "orders" || cause.From() != "ready" || cause.To() != "killed" || len(recorder.mutations) != 1 {
				t.Fatalf("guard cause = %#v, mutations=%d", cause, len(recorder.mutations))
			}
			if _, compiled := cause.Compiled(); compiled {
				t.Fatal("guard kill masqueraded as authored edge")
			}
			if !reflect.DeepEqual(cause.GuardsEvaluated(), result.GuardsEvaluated) || len(cause.GuardsEvaluated()) == 0 {
				t.Fatalf("lost evaluated guard: %#v", cause.GuardsEvaluated())
			}
			if err := cause.ValidateAgainst(graph); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExecutorCompiledCreateCarrierRequiresCanonicalInitialStage(t *testing.T) {
	node := testFlowExecutableNode(t, "orders", "creator")
	handler := contracts.SystemNodeEventHandler{CreateEntity: true, AdvancesTo: "working"}
	graph := contracts.BuildWorkflowStageTopology("orders", "ready", []string{"ready", "working", "other"}, nil,
		[]contracts.HandlerTransitionSemantic{{Node: node, EventType: "work.created", CreateEntity: true, AdvancesTo: "working"}}, nil, nil)
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{StageTopologies: map[string]contracts.WorkflowStageTopology{"orders": graph}}})
	for _, state := range []string{"ready", "", "other"} {
		t.Run("source_"+state, func(t *testing.T) {
			exec, recorder := transitionTestExecutor(t, source)
			result, err := exec.ExecuteSemanticFixture(context.Background(), transitionTestRequest(t, node, "work.created", handler, state))
			if state != "ready" {
				if !errors.Is(err, ErrInvalidTransition) || len(recorder.mutations) != 0 {
					t.Fatalf("arbitrary seed %q: err=%v mutations=%d", state, err, len(recorder.mutations))
				}
				return
			}
			if err != nil || result.StateMutation.Transition == nil || result.StateMutation.Transition.From() != "ready" || len(recorder.mutations) != 1 {
				t.Fatalf("canonical initial source: result=%#v err=%v", result, err)
			}
		})
	}
}

func TestSemanticFixtureCannotInferStageMembership(t *testing.T) {
	for _, flowID := range []string{"orders", "."} {
		t.Run(flowID, func(t *testing.T) {
			node := testFlowExecutableNode(t, flowID, "worker")
			original := stubSource()
			source := sourceWithFixtureStages(original, flowID, "ready", "ready")
			exec, recorder := transitionTestExecutor(t, source)
			req := transitionTestRequest(t, node, "work.requested", contracts.SystemNodeEventHandler{AdvancesTo: "undeclared"}, "ready")
			req.ExecutionFlowID = identity.NormalizeFlowID(flowID)
			result, err := exec.ExecuteSemanticFixture(context.Background(), req)
			if !errors.Is(err, ErrInvalidTransition) || len(recorder.mutations) != 0 || result.StateMutation.Transition != nil {
				t.Fatalf("isolated fixture invented target membership: result=%#v mutations=%d err=%v", result, len(recorder.mutations), err)
			}
			if _, exists := semanticview.WorkflowStageTopology(source, flowID); exists || !reflect.DeepEqual(source.FlowStates(flowID), []string{"ready"}) {
				t.Fatal("isolated execution mutated its shared fixture source")
			}
			if len(original.FlowStates(flowID)) != 0 {
				t.Fatal("fixture declaration mutated the original empty source")
			}
		})
	}
}
