package providerconnectors

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func TestMockResponseConstructorRejectsLossySources(t *testing.T) {
	cycle := map[string]any{}
	cycle["cycle"] = cycle
	for name, source := range map[string]any{
		"native invalid string": map[string]any{"text": string([]byte{0xff})},
		"native invalid key":    map[string]any{string([]byte{0xff}): true},
		"nested invalid string": []any{map[string]any{"text": string([]byte{0xff})}},
		"raw invalid UTF8":      json.RawMessage{'"', 0xff, '"'},
		"duplicate key":         json.RawMessage(`{"a":1,"\u0061":2}`),
		"nested duplicate":      json.RawMessage(`[{"a":1,"a":2}]`),
		"unsafe integer":        json.RawMessage(`9007199254740992`),
		"native unsafe integer": int64(9007199254740992),
		"negative zero":         json.RawMessage(`-0`),
		"native negative zero":  math.Copysign(0, -1),
		"infinity":              math.Inf(1),
		"underflow":             json.RawMessage(`1e-999`),
		"trailing value":        json.RawMessage(`{} {}`),
		"cycle":                 cycle,
	} {
		t.Run(name, func(t *testing.T) {
			if plan, err := NewMockResponsePlan(map[string]any{"provider.write": source}); err == nil || plan != nil {
				t.Fatalf("lossy source admitted: plan=%v err=%v", plan, err)
			}
		})
	}
}

func TestMockResponseSemanticIsolationAndNativeExecutionContrast(t *testing.T) {
	source := map[string]any{"rows": []any{map[string]any{"count": float64(8), "fraction": 8.25, "optional": nil}}}
	plan, err := NewMockResponsePlan(map[string]any{"provider.write": source})
	if err != nil {
		t.Fatal(err)
	}
	source["rows"].([]any)[0].(map[string]any)["count"] = 99
	admitted, err := plan.Admit("provider.write", mockResponseTool(Category, runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"rows": []any{map[string]any{"count": int64(8), "fraction": 8.25, "optional": nil}}}
	for i := 0; i < 3; i++ {
		got, err := admitted.Materialize()
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("materialization %d: %#v err=%v", i, got, err)
		}
		got.(map[string]any)["rows"].([]any)[0].(map[string]any)["count"] = -1
	}
	native, err := workflowexpr.ProjectCELValue(float64(8))
	if err != nil || native != float64(8) {
		t.Fatalf("native execution double lost: %T(%v) err=%v", native, native, err)
	}
	if _, err := (AdmittedMockResponse{}).Materialize(); err == nil {
		t.Fatal("zero response admitted")
	}
}

func TestMockResponseMaterializationUsesSemanticNumericExecution(t *testing.T) {
	tool := mockResponseTool(Category, runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"count": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaInteger),
		}), runtimecontracts.ToolSchemaRequired("count")))
	for _, spelling := range []string{"8", "8.0", "8e0"} {
		t.Run(spelling, func(t *testing.T) {
			plan, err := NewMockResponsePlan(map[string]json.RawMessage{"provider.write": json.RawMessage(`{"count":` + spelling + `}`)})
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := plan.Admit("provider.write", tool)
			if err != nil {
				t.Fatal(err)
			}
			value, err := admitted.Materialize()
			if err != nil {
				t.Fatal(err)
			}
			if got := value.(map[string]any)["count"]; got != int64(8) {
				t.Fatalf("semantic mock count = %T(%v), want int64(8)", got, got)
			}
		})
	}
}

func TestMockResponseNullFollowsOutputSchema(t *testing.T) {
	plan, err := NewMockResponsePlan(map[string]any{"provider.write": nil})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := plan.Admit("provider.write", mockResponseTool(Category, runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaNull)))
	if err != nil {
		t.Fatal(err)
	}
	value, err := admitted.Materialize()
	if err != nil || value != nil {
		t.Fatalf("null materialization: %v %v", value, err)
	}
	if _, err := plan.Admit("provider.write", mockResponseTool(Category, runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))); err == nil {
		t.Fatal("null admitted for object schema")
	}
}

func TestMockResponsePlanAdmitsOnlyExactProviderConnectorResponses(t *testing.T) {
	plan, err := NewMockResponsePlan(map[string]map[string]any{
		"telegram.send_message": {"ok": true},
	})
	if err != nil {
		t.Fatalf("NewMockResponsePlan: %v", err)
	}
	tool := mockResponseTool(Category, runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
		"ok": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaBoolean),
	}), runtimecontracts.ToolSchemaRequired("ok")))

	response, err := plan.Admit("telegram.send_message", tool)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	materialized, err := response.Materialize()
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	result, ok := materialized.(map[string]any)
	if !ok {
		t.Fatalf("result = %T, want object", materialized)
	}
	if result["ok"] != true {
		t.Fatalf("result = %#v, want ok=true", result)
	}
	result["ok"] = false
	materializedAgain, err := response.Materialize()
	again, ok := materializedAgain.(map[string]any)
	if err != nil || !ok || again["ok"] != true {
		t.Fatalf("immutable materialization = %#v err=%v", again, err)
	}

	for name, tc := range map[string]struct {
		id        string
		candidate runtimecontracts.ToolSchemaEntry
		want      string
	}{
		"missing exact response": {"telegram.delete_message", tool, "not configured"},
		"non provider tool":      {"telegram.send_message", mockResponseTool("platform", runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), "only provider_connector"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := plan.Admit(tc.id, tc.candidate); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Admit error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestMockResponsePlanRejectsOutputOutsideCanonicalToolSchema(t *testing.T) {
	plan, err := NewMockResponsePlan(map[string]map[string]any{
		"provider.write": {"ok": "not-a-boolean"},
	})
	if err != nil {
		t.Fatalf("NewMockResponsePlan: %v", err)
	}
	tool := mockResponseTool(Category, runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
		"ok": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaBoolean),
	}), runtimecontracts.ToolSchemaRequired("ok")))

	if _, err := plan.Admit("provider.write", tool); err == nil || !strings.Contains(err.Error(), "does not match output_schema") {
		t.Fatalf("Admit error = %v", err)
	}
}

func TestMockResponsePlanRejectsOutputOutsideTypedEnum(t *testing.T) {
	plan, err := NewMockResponsePlan(map[string]map[string]any{
		"provider.write": {"status": "wrong"},
	})
	if err != nil {
		t.Fatalf("NewMockResponsePlan: %v", err)
	}
	tool := mockResponseTool(Category, runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
		"status": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaEnum("ok")),
	}), runtimecontracts.ToolSchemaRequired("status")))

	if _, err := plan.Admit("provider.write", tool); err == nil || !strings.Contains(err.Error(), "$.status is not one of the declared enum values") {
		t.Fatalf("Admit error = %v, want exact out-of-enum rejection", err)
	}
}

func mockResponseTool(category string, output runtimecontracts.ToolInputSchema) runtimecontracts.ToolSchemaEntry {
	objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
	return runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory(category),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolSchemas(objectSchema, output),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"}),
	)
}
