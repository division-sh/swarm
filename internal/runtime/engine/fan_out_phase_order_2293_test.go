package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestReviewerD2293FanOutDoesNotSkipLaterSteps(t *testing.T) {
	_, _, result, err := a2FanOutCollectionFixture(t, "payload.items", "[text]",
		map[string]any{"items": []any{"a"}}, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.FanOutIntents) == 0 {
		t.Fatal("fixture did not execute fan-out")
	}
	for _, step := range []Step{StepOnComplete, StepRules, StepAdvancesTo, StepEmits, StepActivity, StepClear} {
		if !slices.Contains(result.ExecutedSteps, step) {
			t.Errorf("Q1 rule 5: fan-out skipped %s; executed=%v", step, result.ExecutedSteps)
		}
	}
}

func Test2293FanOutAssignmentBoundariesMatchExecution(t *testing.T) {
	for _, selection := range []string{"rules", "on_complete"} {
		t.Run(selection, func(t *testing.T) {
			executor, _, _, err := a2FanOutCollectionFixture(t, "payload.items", "[text]", map[string]any{"items": []any{"a"}}, nil, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			bundle, _ := semanticview.Bundle(executor.deps.Source)
			for _, name := range []string{"early", "late"} {
				bundle.RootEntities["subject"].Fields[name] = rc.EntityFieldDecl{Type: "boolean", IsOptional: true}
			}
			fan := func() *rc.FanOutSpec {
				return &rc.FanOutSpec{ItemsFrom: "payload.items", As: "key", Emit: rc.EmitSpec{Event: "item.ready"}}
			}
			handler := rc.SystemNodeEventHandler{FanOut: fan(), DataAccumulation: rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{{TargetField: "early", Value: rc.LiteralExpression(true)}}}}
			row := rc.HandlerRuleEntry{FanOut: fan(), DataAccumulation: rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{{TargetField: "late", Value: rc.LiteralExpression(true)}}}}
			if selection == "rules" {
				handler.Rules = []rc.HandlerRuleEntry{row}
			} else {
				handler.OnComplete = []rc.HandlerRuleEntry{row}
			}
			node := testRootExecutableNode(t, "assignment_"+selection)
			handler, err = completeSemanticFixtureHandlerRuleIdentity(node, "batch.ready", handler)
			if err != nil {
				t.Fatal(err)
			}
			if err := bundle.CompileFanOutHandlerPlans(node, "batch.ready", handler); err != nil {
				t.Fatal(err)
			}
			typ, err := semanticview.ResolveEntityStructuralType(executor.deps.Source, ".")
			if err != nil || typ == nil {
				t.Fatal(err)
			}
			analysis := &EntityAssignmentAnalysis{source: executor.deps.Source, entity: *typ, initial: entityruntime.AssignmentFacts{}}
			outcome := entityAssignmentOutcomes(handler)[0]
			selectedStep := entityAssignmentSelectionStep(outcome.kind)
			for _, point := range []EntityAssignmentPoint{
				{Step: StepFanOut, FanOut: true},
				{Step: selectedStep, FanOut: true, RuleKind: outcome.kind, RuleRef: outcome.ref},
				{Step: StepEmits, RuleKind: outcome.kind, RuleRef: outcome.ref},
			} {
				facts := analysis.BeforeRead(node, "batch.ready", handler, point)
				if _, ok := facts["early"]; !ok {
					t.Fatalf("%s lost earlier handler write", point.Step)
				}
				_, late := facts["late"]
				if late != (point.Step == StepEmits) {
					t.Fatalf("%s selected-write visibility=%t", point.Step, late)
				}
			}
		})
	}
}

func Test2293HandlerFanOutContinuesThroughSelectionAndEffects(t *testing.T) {
	for _, selection := range []string{"rules", "on_complete"} {
		for _, effect := range []string{"emit", "activity"} {
			for _, preview := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/preview_%t", selection, effect, preview), func(t *testing.T) {
					executor, trigger, _, err := a2FanOutCollectionFixture(t, "payload.items", "[text]", map[string]any{"items": []any{"a"}}, nil, nil, 0)
					if err != nil {
						t.Fatal(err)
					}
					bundle, _ := semanticview.Bundle(executor.deps.Source)
					bundle.Tools = sourceWithActivityTool().ToolEntries()
					entity := bundle.RootEntities["subject"]
					entity.Fields["selected"] = rc.EntityFieldDecl{Type: "boolean"}
					bundle.RootEntities["subject"] = entity
					executor.deps.Source = sourceWithFixtureStages(executor.deps.Source, ".", "pending", "pending", "handler_stage", "selected_stage")
					executor.evaluator = contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
						"entity.selected": func(base BaseContext) (bool, error) {
							return base.Entity.Raw()["selected"] == true && base.Computed.Raw()["temporary"] == 1, nil
						},
					}}
					handler := rc.SystemNodeEventHandler{
						AdvancesTo:       "handler_stage",
						Count:            &rc.CountSpec{ItemsFrom: "payload.items", StoreAs: "computed.temporary"},
						DataAccumulation: rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{{TargetField: "selected", Value: rc.LiteralExpression(true)}}},
						FanOut: &rc.FanOutSpec{ItemsFrom: "payload.items", As: "key", Emit: rc.EmitSpec{Event: "item.ready", Fields: map[string]rc.ExpressionValue{
							"key": rc.CELExpression("key"), "index": rc.CELExpression("fan_out.index"), "count": rc.CELExpression("fan_out.count"),
						}}},
						Clear: &rc.ClearSpec{Targets: []string{"computed.temporary"}},
					}
					row := rc.HandlerRuleEntry{Condition: "entity.selected", AdvancesTo: "selected_stage"}
					wantEmits, wantActivities := 0, 0
					if effect == "emit" {
						row.Emit = rc.EmitSpec{Event: "item.ready", Fields: map[string]rc.ExpressionValue{
							"key": rc.LiteralExpression("ordinary"), "index": rc.LiteralExpression(0), "count": rc.LiteralExpression(1),
						}}
						wantEmits = 1
					} else {
						row.Activity = rc.ActivitySpec{ID: "after_scatter", Tool: "source_scrape"}
						wantActivities = 1
						if selection == "on_complete" {
							handler.Activity = row.Activity
							row.Activity = rc.ActivitySpec{}
						}
					}
					if selection == "rules" {
						handler.Rules = []rc.HandlerRuleEntry{row}
					} else {
						handler.OnComplete = []rc.HandlerRuleEntry{row}
					}
					result, err := executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
						EntityID: identity.NormalizeEntityID(semanticExecutionFixtureRunID), Node: testRootExecutableNode(t, "order_"+selection), Event: trigger, Handler: handler,
						State: testStateSnapshot("pending", nil, nil, nil), Preview: preview,
					})
					if err != nil {
						t.Fatal(err)
					}
					if result.NextState != "selected_stage" || result.StateMutation.NextState != "selected_stage" {
						t.Fatalf("transition bypassed selected row: %+v", result.StateMutation)
					}
					transitions := 0
					for _, action := range result.ActionsExecuted {
						if action == identity.ActionUpdateState.String() {
							transitions++
						}
					}
					if transitions != 1 {
						t.Fatalf("stage transition executed %d times", transitions)
					}
					if len(result.EmitIntents) != wantEmits || len(result.ActivityIntents) != wantActivities {
						t.Fatalf("later effects skipped: emits=%d activity=%d", len(result.EmitIntents), len(result.ActivityIntents))
					}
					if _, exists := result.Computed["temporary"]; exists {
						t.Fatal("clear skipped")
					}
					if result.StateMutation.Fields["selected"] != true {
						t.Fatal("unconditional write lost")
					}
					if !preview && len(result.FanOutIntents) != 1 {
						t.Fatal("fan-out intent lost")
					}
					for _, step := range []Step{StepOnComplete, StepRules, StepAdvancesTo, StepEmits, StepActivity, StepClear} {
						if !slices.Contains(result.ExecutedSteps, step) {
							t.Fatalf("skipped %s", step)
						}
					}
				})
			}
		}
	}
}

func Test2293HandlerAndSelectedFanOutKeepDistinctIntents(t *testing.T) {
	for _, selection := range []string{"rules", "on_complete"} {
		t.Run(selection, func(t *testing.T) {
			executor, trigger, _, err := a2FanOutCollectionFixture(t, "payload.items", "[text]", map[string]any{"items": []any{"a"}}, nil, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			fan := func() *rc.FanOutSpec {
				return &rc.FanOutSpec{ItemsFrom: "payload.items", As: "key", Emit: rc.EmitSpec{Event: "item.ready"}}
			}
			handler := rc.SystemNodeEventHandler{FanOut: fan()}
			row := rc.HandlerRuleEntry{FanOut: fan()}
			if selection == "rules" {
				handler.Rules = []rc.HandlerRuleEntry{row}
			} else {
				handler.OnComplete = []rc.HandlerRuleEntry{row}
			}
			result, err := executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{EntityID: identity.NormalizeEntityID(semanticExecutionFixtureRunID), Node: testRootExecutableNode(t, "both_"+selection), Event: trigger, Handler: handler, State: testStateSnapshot("pending", nil, nil, nil)})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.FanOutIntents) != 2 {
				t.Fatalf("site intent overwritten: %v", result.FanOutIntents)
			}
			first, second := result.FanOutIntents[0], result.FanOutIntents[1]
			if first.Key == second.Key || first.PlanRef == second.PlanRef || first.Key.TriggeringDeliveryID != second.Key.TriggeringDeliveryID {
				t.Fatal("fan-out site ownership lost")
			}
		})
	}
}

func TestReviewerD2293SelectedFanOutSourcePrecedesRuleWrites(t *testing.T) {
	for _, replacement := range [][]any{{"new"}, {"new", "extra"}} {
		executor, trigger, _, err := a2FanOutCollectionFixture(t, "entity.items", "[text]",
			map[string]any{"items": []any{"old"}}, map[string]any{"items": []any{"old"}}, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		executor.evaluator = stubEvaluator{bools: map[string]bool{"true": true}}
		handler := rc.SystemNodeEventHandler{Rules: []rc.HandlerRuleEntry{{
			Condition: "true",
			FanOut: &rc.FanOutSpec{ItemsFrom: "entity.items", As: "key", Emit: rc.EmitSpec{
				Event: "item.ready", Fields: map[string]rc.ExpressionValue{
					"key": rc.CELExpression("key"), "index": rc.CELExpression("fan_out.index"), "count": rc.CELExpression("fan_out.count"),
				},
			}},
			DataAccumulation: rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{{
				TargetField: "items", Value: rc.LiteralExpression(replacement),
			}}},
		}}}
		result, err := executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
			EntityID: identity.NormalizeEntityID(semanticExecutionFixtureRunID), Node: testRootExecutableNode(t, "scatter_rule"),
			Event: trigger, Handler: handler, State: testStateSnapshot("pending", map[string]any{"items": []any{"old"}}, nil, nil),
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.FanOutIntents) == 0 {
			t.Fatal("selected fan-out not reached")
		}
		if !slices.Contains(result.ExecutedSteps, StepRules) || !reflect.DeepEqual(result.StateMutation.StateCarrier.Fields["items"], replacement) {
			t.Fatalf("fixture did not reach selected rule write: steps=%v fields=%v", result.ExecutedSteps, result.StateMutation.StateCarrier.Fields)
		}
		value, err := result.FanOutIntents[0].CapturedEntitySource()
		if err != nil {
			t.Fatal(err)
		}
		projected, err := result.FanOutIntents[0].ProjectSource(value)
		if err != nil || !reflect.DeepEqual(projected, []any{"old"}) {
			t.Errorf("fan-out source rebound by later rule write %v: projected=%v error=%v", replacement, projected, err)
		}
	}
}

func Test2293LaterFanOutRefusalHasNoCommittedEffects(t *testing.T) {
	executor, trigger, _, err := a2FanOutCollectionFixture(t, "payload.items", "[text]", map[string]any{"items": []any{"a", "b"}}, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &transitionMutationRecorder{}
	executor.deps.MutationOwner = recorder
	state := testStateSnapshot("pending", map[string]any{"items": []any{"before"}}, nil, nil)
	before, _ := json.Marshal(state)
	handler := rc.SystemNodeEventHandler{
		FanOut: &rc.FanOutSpec{ItemsFrom: "payload.items", As: "key", Emit: rc.EmitSpec{Event: "item.ready"}},
		Rules:  []rc.HandlerRuleEntry{{FanOut: &rc.FanOutSpec{ItemsFrom: "payload.items", As: "key", MaxItems: 1, MaxItemsSet: true, Emit: rc.EmitSpec{Event: "item.ready"}}}},
	}
	result, err := executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{EntityID: identity.NormalizeEntityID(semanticExecutionFixtureRunID), Node: testRootExecutableNode(t, "late_refusal"), Event: trigger, Handler: handler, State: state})
	if err == nil || !errors.Is(err, ErrFanOutBoundExceeded) {
		t.Fatalf("expected selected bound refusal: %v", err)
	}
	if len(recorder.mutations) != 0 || result.Committed || len(result.EmitIntents) != 0 || len(result.ActivityIntents) != 0 {
		t.Fatal("later refusal committed earlier work")
	}
	after, _ := json.Marshal(state)
	if string(before) != string(after) {
		t.Fatal("refused candidate changed input snapshot")
	}
}
