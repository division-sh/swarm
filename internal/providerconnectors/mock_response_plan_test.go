package providerconnectors

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

func TestMockResponsePlanAdmitsOnlyExactProviderConnectorResponses(t *testing.T) {
	plan, err := NewMockResponsePlan(map[string]map[string]any{
		"telegram.send_message": {"ok": true},
	})
	if err != nil {
		t.Fatalf("NewMockResponsePlan: %v", err)
	}
	tool := runtimecontracts.ToolSchemaEntry{
		Category: Category, HandlerType: "http", HTTP: &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"},
		OutputSchema: runtimecontracts.ToolInputSchema{
			Type: "object",
			Properties: map[string]runtimecontracts.ToolInputSchema{
				"ok": {Type: "boolean"},
			},
			Required: []string{"ok"},
		},
	}
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
		"non provider tool":      {"telegram.send_message", runtimecontracts.ToolSchemaEntry{Category: "native"}, "only provider_connector"},
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
	tool := runtimecontracts.ToolSchemaEntry{
		Category: Category, HandlerType: "http", HTTP: &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"},
		OutputSchema: runtimecontracts.ToolInputSchema{
			Type: "object",
			Properties: map[string]runtimecontracts.ToolInputSchema{
				"ok": {Type: "boolean"},
			},
			Required: []string{"ok"},
		},
	}
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
	tool := runtimecontracts.ToolSchemaEntry{
		Category: Category, HandlerType: "http", HTTP: &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"},
		OutputSchema: runtimecontracts.ToolInputSchema{
			Type: "object",
			Properties: map[string]runtimecontracts.ToolInputSchema{
				"status": {
					Type: "string",
					Enum: []runtimecontracts.SchemaLiteral{{Node: yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "ok"}}},
				},
			},
			Required: []string{"status"},
		},
	}
	if _, err := plan.Admit("provider.write", tool); err == nil || !strings.Contains(err.Error(), "$.status has invalid enum value wrong") {
		t.Fatalf("Admit error = %v, want exact out-of-enum rejection", err)
	}
}
