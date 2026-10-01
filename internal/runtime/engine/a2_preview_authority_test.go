package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestA2PreviewEvaluatesTransitionWithoutDurableOccurrence(t *testing.T) {
	node := testFlowExecutableNode(t, "orders", "worker")
	for _, rule := range []bool{false, true} {
		handler := contracts.SystemNodeEventHandler{AdvancesTo: "done"}
		if rule {
			handler = contracts.SystemNodeEventHandler{Rules: []contracts.HandlerRuleEntry{{ID: "chosen", Condition: "else", AdvancesTo: "done"}}}
		}
		var err error
		handler, err = completeSemanticFixtureHandlerRuleIdentity(node, "work.requested", handler)
		if err != nil {
			t.Fatal(err)
		}
		graph := contracts.BuildWorkflowStageTopology("orders", "ready", []string{"ready", "done"}, []string{"done"},
			[]contracts.HandlerTransitionSemantic{{Node: node, EventType: "work.requested", AdvancesTo: handler.AdvancesTo, Rules: handler.Rules}}, nil, nil)
		source := semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{
			StageTopologies: map[string]contracts.WorkflowStageTopology{"orders": graph},
		}})
		executor, recorder := transitionTestExecutor(t, source)
		request := transitionTestRequest(t, node, "work.requested", handler, "ready")
		request.Preview = true
		executor.deps.StateRepo = sparseSnapshotRepo{snapshot: request.State}
		before := request.State
		// Call the production entry without the semantic fixture's invented claim.
		result, err := executor.Execute(context.Background(), request)
		if err != nil || result.Committed || len(recorder.mutations) != 0 || result.StateMutation.NextState != "done" || result.StateMutation.Transition == nil {
			t.Fatalf("rule=%v preview=%#v mutations=%d err=%v", rule, result, len(recorder.mutations), err)
		}
		if err := result.StateMutation.Transition.ValidateAgainst(graph); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(request.State, before) {
			t.Fatal("preview mutated its input snapshot")
		}
	}
}

func TestA2PreviewEvaluatesUnkeyedArrivalWithoutDeliveryEvidence(t *testing.T) {
	executor, repo := a2AccumulatorExecutor(t)
	request := a2AccumulatorRequest(t, "", `{"id":"item","marker":"hypothetical"}`, "preview-only")
	request.Preview = true
	before := repo.snapshot
	result, err := executor.ExecuteSemanticFixture(context.Background(), request)
	if err != nil || result.Committed || result.Status == OutcomeDiscarded {
		t.Fatalf("preview=%#v err=%v", result, err)
	}
	preview := StateSnapshot{StateCarrier: result.StateMutation.StateCarrier}
	acc, found := loadAccumulator(preview, request.Node, events.EventType(request.HandlerEventKey))
	if !found || len(acc.Items) != 1 || len(acc.Deliveries) != 0 || len(acc.Received) != 0 || acc.Items[0]["marker"] != "hypothetical" {
		t.Fatalf("preview invented or omitted arrival evidence: %#v", acc)
	}
	if !reflect.DeepEqual(repo.snapshot, before) {
		t.Fatal("preview changed the persisted snapshot")
	}
	request.Preview = false
	_, err = executor.ExecuteSemanticFixture(context.Background(), request)
	requireAccumulatorFailure(t, err, failures.ClassLifecycleConflict)
	if !reflect.DeepEqual(repo.snapshot, before) {
		t.Fatal("unclaimed execution changed the persisted snapshot")
	}
}
