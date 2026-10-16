package engine

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func Test2293Q1HandlerSelectionSeesUnconditionalWrites(t *testing.T) {
	for _, selection := range []string{"rules", "on_complete"} {
		t.Run(selection, func(t *testing.T) {
			state := testStateSnapshot("pending", map[string]any{"branch_target": "prior"}, nil, nil)
			executor, err := NewExecutor(RuntimeDependencies{
				Source: rootHandlerOrderSource(map[string]runtimecontracts.EntityFieldDecl{
					"branch_target": {Type: "text"}, "selected": {Type: "boolean"},
				}, runtimecontracts.EventCatalogEntry{}),
				StateRepo: sparseSnapshotRepo{snapshot: state}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{},
			}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
				`entity.branch_target == "handler"`: func(base BaseContext) (bool, error) {
					return base.Entity.Raw()["branch_target"] == "handler", nil
				},
			}})
			if err != nil {
				t.Fatal(err)
			}
			handler := runtimecontracts.SystemNodeEventHandler{
				DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
					{TargetField: "branch_target", Value: runtimecontracts.LiteralExpression("handler")},
				}},
			}
			rules := []runtimecontracts.HandlerRuleEntry{{
				Condition: `entity.branch_target == "handler"`,
				DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
					{TargetField: "selected", Value: runtimecontracts.LiteralExpression(true)},
				}},
			}}
			if selection == "rules" {
				handler.Rules = rules
			} else {
				handler.OnComplete = rules
			}
			result, err := executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: "entity-1", Node: testRootExecutableNode(t, "node-1"), Handler: handler, State: state,
				Event: eventtest.RunCreatingRootIngress("event-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := result.StateMutation.Fields["selected"]; got != true {
				t.Fatalf("%s selection did not see the unconditional handler write: selected=%v", selection, got)
			}
			if got := result.StateMutation.Fields["branch_target"]; got != "handler" {
				t.Fatalf("handler write lost: %v", got)
			}
		})
	}
}

func Test2293Q1SelectedRuleWriteWinsOverHandlerWrite(t *testing.T) {
	state := testStateSnapshot("pending", map[string]any{}, nil, nil)
	executor, err := NewExecutor(RuntimeDependencies{
		Source: rootHandlerOrderSource(map[string]runtimecontracts.EntityFieldDecl{
			"winner": {Type: "text"}, "rule_only": {Type: "text"},
		}, runtimecontracts.EventCatalogEntry{}),
		StateRepo: sparseSnapshotRepo{snapshot: state}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{},
	}, stubEvaluator{bools: map[string]bool{"true": true}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1", Node: testRootExecutableNode(t, "node-1"), State: state,
		Event: eventtest.RunCreatingRootIngress("event-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "winner", Value: runtimecontracts.LiteralExpression("handler")},
			}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				Condition: "true",
				DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
					{TargetField: "winner", Value: runtimecontracts.LiteralExpression("rule")},
					{TargetField: "rule_only", Value: runtimecontracts.LiteralExpression("kept")},
				}},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.StateMutation.Fields["winner"]; got != "rule" {
		t.Fatalf("selected rule must write last, got %v", got)
	}
	if got := result.StateMutation.Fields["rule_only"]; got != "kept" {
		t.Fatalf("selected rule-only write lost: %v", got)
	}
}

func Test2293Q1HandlerOrderContract(t *testing.T) {
	for _, pair := range [][2]Step{
		{StepGuard, StepJoin}, {StepJoin, StepAccumulate}, {StepAccumulate, StepCompute},
		{StepCompute, StepDataWrites}, {StepDataWrites, StepFanOut},
		{StepFanOut, StepRules}, {StepFanOut, StepOnComplete},
		{StepRules, StepAdvancesTo}, {StepOnComplete, StepAdvancesTo},
		{StepAdvancesTo, StepEmits}, {StepEmits, StepActivity}, {StepActivity, StepClear},
	} {
		before, after := slices.Index(OrderedSteps, pair[0]), slices.Index(OrderedSteps, pair[1])
		if before < 0 || after < 0 || before >= after {
			t.Errorf("%s must precede %s: %v", pair[0], pair[1], OrderedSteps)
		}
	}
}
