package gateruntime

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestFreezeRoutesOwnsExecutionSchemaAndPayloadValidation(t *testing.T) {
	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"code": map[string]any{"type": "string", "pattern": "^[a-z]+$"}},
		"required":             []any{"code"},
		"additionalProperties": false,
	}
	raw, err := FreezeRoutes(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {
			Verdict: "approve", AdvancesTo: "operating",
			Input: map[string]runtimecontracts.WorkflowGateInputField{"code": {Type: "text", Required: true}},
			Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
				"code": runtimecontracts.CELExpression("decision.code"),
			}},
			EmitSchema: schema,
		},
	}, gateTestCompiledTransitions(t))
	if err != nil {
		t.Fatal(err)
	}
	route, err := RouteFor(raw, "approve")
	if err != nil {
		t.Fatal(err)
	}
	invalid, _ := canonicaljson.FromGo(map[string]any{"code": "NOT-LOWER"})
	if _, err := BuildRoutePayload(route, invalid); err == nil || !strings.Contains(err.Error(), "pattern") {
		t.Fatalf("BuildRoutePayload invalid error = %v", err)
	}
	valid, _ := canonicaljson.FromGo(map[string]any{"code": "ready"})
	payload, err := BuildRoutePayload(route, valid)
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := payload.Lookup("code"); !ok || value.Interface() != "ready" {
		t.Fatalf("payload = %#v", payload.Interface())
	}
}

func TestFreezeRoutesRejectsInvalidExecutionBeforePersistence(t *testing.T) {
	unsafe := int64(9007199254740992)
	for _, tc := range []struct {
		name    string
		outcome runtimecontracts.WorkflowGateOutcomePlan
		want    string
	}{
		{name: "unsafe literal", want: "safe range", outcome: runtimecontracts.WorkflowGateOutcomePlan{
			Verdict: "approve", AdvancesTo: "operating",
			Emit:       runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{"value": runtimecontracts.LiteralExpression(unsafe)}},
			EmitSchema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "integer"}}, "required": []any{"value"}},
		}},
		{name: "optional decision field", want: "required declared", outcome: runtimecontracts.WorkflowGateOutcomePlan{
			Verdict: "approve", AdvancesTo: "operating",
			Input:      map[string]runtimecontracts.WorkflowGateInputField{"note": {Type: "text"}},
			Emit:       runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{"note": runtimecontracts.CELExpression("decision.note")}},
			EmitSchema: map[string]any{"type": "object", "properties": map[string]any{"note": map[string]any{"type": "string"}}, "required": []any{"note"}},
		}},
		{name: "noncanonical field", want: "not canonical", outcome: runtimecontracts.WorkflowGateOutcomePlan{
			Verdict: "approve", AdvancesTo: "operating",
			Emit:       runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{" note ": runtimecontracts.LiteralExpression("ready")}},
			EmitSchema: map[string]any{"type": "object", "properties": map[string]any{"note": map[string]any{"type": "string"}}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := FreezeRoutes(map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": tc.outcome}, gateTestCompiledTransitions(t)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("FreezeRoutes error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidateRoutesRejectsStructuralShadowFields(t *testing.T) {
	raw, err := FreezeRoutes(map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "operating"}}, gateTestCompiledTransitions(t))
	if err != nil {
		t.Fatal(err)
	}
	raw = strings.Replace(raw, `"fields":{}`, `"fields":{},"shadow":true`, 1)
	if err := ValidateRoutes(raw); err == nil || !strings.Contains(err.Error(), "unexpected field shadow") {
		t.Fatalf("ValidateRoutes error = %v", err)
	}
}

func gateTestCompiledTransitions(t *testing.T) map[string]runtimecontracts.CompiledTransition {
	t.Helper()
	gate := runtimecontracts.WorkflowGatePlan{
		FlowID: "review", Stage: "waiting", Decision: "review_decision",
		Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "operating"}},
	}
	graph := runtimecontracts.BuildWorkflowStageTopology("review", "waiting", []string{"waiting", "operating"}, nil, nil, nil, nil, []runtimecontracts.WorkflowGatePlan{gate})
	transition, err := graph.AdmitTransition(runtimecontracts.WorkflowTransitionSite{DecisionID: "review_decision", Verdict: "approve"}, "waiting", "operating")
	if err != nil {
		t.Fatal(err)
	}
	return map[string]runtimecontracts.CompiledTransition{"approve": transition}
}

func TestFreezeRoutesRejectsMissingExtraAndContradictoryTransitions(t *testing.T) {
	outcomes := map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "operating"}}
	compiled := gateTestCompiledTransitions(t)["approve"]
	for _, tc := range []struct {
		name        string
		transitions map[string]runtimecontracts.CompiledTransition
	}{
		{"missing", nil},
		{"extra", map[string]runtimecontracts.CompiledTransition{"approve": compiled, "reject": compiled}},
		{"wrong key", map[string]runtimecontracts.CompiledTransition{"reject": compiled}},
		{"zero evidence", map[string]runtimecontracts.CompiledTransition{"approve": {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := FreezeRoutes(outcomes, tc.transitions); err == nil {
				t.Fatal("invalid transition map accepted")
			}
		})
	}
	t.Run("contradictory target", func(t *testing.T) {
		if _, err := FreezeRoutes(map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "other"}}, gateTestCompiledTransitions(t)); err == nil {
			t.Fatal("route target contradicting compiled evidence accepted")
		}
	})
}

func TestFrozenGateRoutesPreserveEvidenceAndRejectHostileHydration(t *testing.T) {
	compiled := gateTestCompiledTransitions(t)
	raw, err := FreezeRoutes(map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "operating"}}, compiled)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateRoutes(raw); err != nil {
		t.Fatal(err)
	}
	route, err := RouteFor(raw, "approve")
	if err != nil {
		t.Fatal(err)
	}
	if route.Transition.FlowID() != compiled["approve"].FlowID() || route.Transition.Edge() != compiled["approve"].Edge() {
		t.Fatalf("frozen evidence changed: %#v", route)
	}
	for _, tc := range []struct{ name, old, replacement, verdict string }{
		{"route target", `"advances_to":"operating"`, `"advances_to":"other"`, "approve"},
		{"route verdict", `"approve":{`, `"reject":{`, "reject"},
		{"compiled target", `"To":"operating"`, `"To":"other"`, "approve"},
		{"compiled verdict", `"Verdict":"approve"`, `"Verdict":"reject"`, "approve"},
		{"compiled source kind", `"Source":"gate"`, `"Source":"timer"`, "approve"},
		{"compiled unknown field", `"DecisionID":"review_decision"`, `"DecisionID":"review_decision","shadow":true`, "approve"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := strings.Replace(raw, tc.old, tc.replacement, 1)
			if mutated == raw || !json.Valid([]byte(mutated)) {
				t.Fatalf("invalid tampering fixture: %s", mutated)
			}
			if err := ValidateRoutes(mutated); err == nil {
				t.Fatalf("ValidateRoutes accepted tampering: %s", mutated)
			}
			if _, err := RouteFor(mutated, tc.verdict); err == nil {
				t.Fatalf("RouteFor accepted tampering: %s", mutated)
			}
		})
	}
}
