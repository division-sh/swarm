package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestA2JoinClosedOutcomeFreeContextScope(t *testing.T) {
	for _, expression := range []string{
		"payload", "payload.result", `payload["result"]`, "payload.?result", "has(payload.result)",
		"event", "event.id", "policy", "computed", "fan_out", "accumulated", "_entity", "metadata", "gates", "_loop.attempt",
		`join.results.exists(r, r == payload.result)`, `join.results.map(r, event.id)`,
		`__swarm_r2_format(payload.result)`,
	} {
		t.Run(expression, func(t *testing.T) {
			if err := ValidateJoinOutcomeExpressionScope(expression); err == nil {
				t.Fatal("transport/non-outcome context admitted")
			}
		})
	}
	for _, expression := range []string{
		"join.results", "entity.total", "loop.attempt", "state.total", `entity.payload`,
		`"payload.result"`, `{"payload": "event.id"}`, `join.results.exists(payload, payload > 0)`,
		`join.results.map(event, event)`, `join.results.exists(r, r.payload > 0)`,
		`loop.attempt > 0 // payload.secret`,
	} {
		t.Run("allowed "+expression, func(t *testing.T) {
			if err := ValidateJoinOutcomeExpressionScope(expression); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestA2JoinCompilerRejectsTransportInBothClosedOutcomes(t *testing.T) {
	for _, outcome := range []string{"on_complete", "on_deadline"} {
		for name, rule := range map[string]HandlerRuleEntry{
			"direct payload":        {Emit: EmitSpec{Event: "closed", Fields: map[string]ExpressionValue{"value": RefExpression("payload.result")}}},
			"whole payload":         {Emit: EmitSpec{Event: "closed", Fields: map[string]ExpressionValue{"value": CELExpression("payload")}}},
			"until event":           {Emit: EmitSpec{Event: "closed", Fields: map[string]ExpressionValue{"value": CELExpression("event.id")}}},
			"payload sugar":         {Emit: EmitSpec{Event: "closed", From: EmitFromPayload}},
			"implicit write source": {DataAccumulation: WorkflowDataAccumulation{Writes: []WorkflowDataWrite{{TargetField: "result"}}}},
			"write value":           {DataAccumulation: WorkflowDataAccumulation{Writes: []WorkflowDataWrite{{TargetRef: "entity.result", Value: RefExpression("payload.result")}}}},
			"write key":             {DataAccumulation: WorkflowDataAccumulation{Writes: []WorkflowDataWrite{{TargetRef: "entity.result", Value: LiteralExpression(1), Key: CELExpression("event.id")}}}},
			"write index":           {DataAccumulation: WorkflowDataAccumulation{Writes: []WorkflowDataWrite{{TargetRef: "entity.result", Value: LiteralExpression(1), Index: CELExpression("payload.index")}}}},
		} {
			t.Run(outcome+"/"+name, func(t *testing.T) {
				bundle, handler := a2JoinCompilerFixture(t, "[text]")
				handler.Join.Deadline = &JoinDeadlineSpec{After: "1h", From: JoinDeadlineFromStageEntry}
				handler.Join.OnDeadlineFound = true
				handler.Join.OnDeadline = HandlerRuleEntry{AdvancesTo: "done"}
				if outcome == "on_complete" {
					handler.Join.OnComplete = rule
				} else {
					handler.Join.OnDeadline = rule
				}
				if _, err := bundle.CompileWorkflowJoinPlan(identitytest.RootNode(t, "collector"), "arrived", handler); err == nil || !strings.Contains(err.Error(), "join."+outcome) {
					t.Fatalf("closed-outcome compiler scope bypass: %v", err)
				}
			})
		}
	}
	for _, value := range []ExpressionValue{LiteralExpression("payload.result"), CELExpression("join.results"), RefExpression("entity.members"), CELExpression("loop.attempt")} {
		bundle, handler := a2JoinCompilerFixture(t, "[text]")
		handler.Join.OnComplete.Emit = EmitSpec{Event: "closed", Fields: map[string]ExpressionValue{"value": value}}
		if plan, err := bundle.CompileWorkflowJoinPlan(identitytest.RootNode(t, "collector"), "arrived", handler); err != nil || plan.Spec.Output != "payload.result" {
			t.Fatalf("legal outcome/typed arrival selector lost: %#v %v", plan, err)
		}
	}
}
