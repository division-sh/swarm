package gateruntime

import (
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
	})
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
			if _, err := FreezeRoutes(map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": tc.outcome}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("FreezeRoutes error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidateRoutesRejectsStructuralShadowFields(t *testing.T) {
	raw := `{"approve":{"advances_to":"operating","emit":{"event":"","fields":{},"shadow":true},"emit_schema":{}}}`
	if err := ValidateRoutes(raw); err == nil || !strings.Contains(err.Error(), "unexpected field shadow") {
		t.Fatalf("ValidateRoutes error = %v", err)
	}
}
