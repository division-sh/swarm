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
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
		{StepGuard, StepJoin}, {StepJoin, StepAccumulate}, {StepAccumulate, StepQuery}, {StepQuery, StepCompute},
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

func Test2293Q1AssignmentFollowsHandlerOrder(t *testing.T) {
	source := rootHandlerOrderSource(map[string]runtimecontracts.EntityFieldDecl{
		"written": {Type: "boolean"}, "derived": {Type: "[text]"}, "selected": {Type: "boolean"},
	}, runtimecontracts.EventCatalogEntry{})
	entity, err := semanticview.ResolveEntityStructuralType(source, ".")
	if err != nil || entity == nil {
		t.Fatalf("fixture entity type: %v", err)
	}
	analysis := &EntityAssignmentAnalysis{source: source, entity: *entity}
	node := testRootExecutableNode(t, "node-1")
	handler := runtimecontracts.SystemNodeEventHandler{
		Query: &runtimecontracts.QuerySpec{StoreAs: "entity.derived"},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
			{TargetField: "written", Value: runtimecontracts.LiteralExpression(true)},
		}},
		Rules: []runtimecontracts.HandlerRuleEntry{{
			Condition: "entity.written",
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "selected", Value: runtimecontracts.RefExpression("entity.written")},
			}},
		}},
	}
	outcomes := entityAssignmentOutcomes(handler)
	if len(outcomes) == 0 || outcomes[0].rule == nil || outcomes[0].kind != runtimecontracts.HandlerAdvanceCarrierRules {
		t.Fatal("fixture has no selected-rule outcome")
	}
	outcome := outcomes[0]
	t.Run("guard_precedes_query_writes", func(t *testing.T) {
		facts := analysis.transfer(node, "task.completed", handler, outcome, entityruntime.AssignmentFacts{}, &EntityAssignmentPoint{Step: StepGuard})
		if _, assigned := facts["derived"]; assigned {
			t.Fatal("guard borrowed a later query write")
		}
	})
	t.Run("condition_sees_handler_writes", func(t *testing.T) {
		facts := analysis.transfer(node, "task.completed", handler, outcome, entityruntime.AssignmentFacts{}, &EntityAssignmentPoint{Step: StepRules, Condition: true})
		if _, assigned := facts["written"]; !assigned {
			t.Fatal("condition lost the preceding handler write")
		}
		if _, assigned := facts["selected"]; assigned {
			t.Fatal("condition borrowed the selected rule's later write")
		}
	})
	t.Run("rule_write_sees_handler_not_future_rule_write", func(t *testing.T) {
		facts := analysis.transfer(node, "task.completed", handler, outcome, entityruntime.AssignmentFacts{}, &EntityAssignmentPoint{
			Step: StepDataWrites, HasWriteIndex: true, WriteIndex: 0, RuleKind: runtimecontracts.HandlerAdvanceCarrierRules,
		})
		if _, assigned := facts["written"]; !assigned {
			t.Fatal("rule write lost the preceding handler assignment")
		}
		if _, assigned := facts["selected"]; assigned {
			t.Fatal("rule write borrowed its own future assignment")
		}
	})
}
