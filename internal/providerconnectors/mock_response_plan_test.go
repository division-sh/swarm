package providerconnectors

import (
	"encoding/json"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

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
