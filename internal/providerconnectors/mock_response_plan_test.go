package providerconnectors

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

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
	result, err := response.Materialize()
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if result["ok"] != true {
		t.Fatalf("result = %#v, want ok=true", result)
	}
	result["ok"] = false
	again, err := response.Materialize()
	if err != nil || again["ok"] != true {
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

	if _, err := plan.Admit("provider.write", tool); err == nil || !strings.Contains(err.Error(), "$.status has invalid enum value wrong") {
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
