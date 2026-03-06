package tools_test

import (
	"strings"
	"testing"

	"empireai/internal/commgraph"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimetools "empireai/internal/runtime/tools"
)

func schemaFor(eventType string) (runtimetools.EmitSchema, bool) {
	schema, ok := runtimecontracts.EventSchemaRegistry()[strings.TrimSpace(eventType)]
	return schema, ok
}

func TestGenerateEmitToolsForRole(t *testing.T) {
	tools := runtimetools.GenerateEmitToolsForRole("empire-coordinator", nil)
	if len(tools) == 0 {
		t.Fatal("expected emit tools for empire-coordinator")
	}
	foundScan := false
	for _, tool := range tools {
		if tool.Name != "emit_scan_requested" {
			continue
		}
		foundScan = true
		schema, ok := tool.Schema.(map[string]any)
		if !ok {
			t.Fatalf("expected scan tool schema object, got %#v", tool.Schema)
		}
		requiredAny, ok := schema["required"].([]any)
		if !ok || len(requiredAny) == 0 {
			t.Fatalf("expected required list, got %#v", schema["required"])
		}
		required := make([]string, 0, len(requiredAny))
		for _, v := range requiredAny {
			required = append(required, strings.TrimSpace(v.(string)))
		}
		for _, field := range []string{"mode", "geography", "campaign_context"} {
			seen := false
			for _, got := range required {
				if got == field {
					seen = true
					break
				}
			}
			if !seen {
				t.Fatalf("expected required field %q in %#v", field, required)
			}
		}
	}
	if !foundScan {
		t.Fatal("expected emit_scan_requested")
	}
}

func TestEmitToolNameRoundTrip(t *testing.T) {
	index := map[string]string{runtimetools.EmitToolName("spec.validation_passed"): "spec.validation_passed"}
	name := runtimetools.EmitToolName("spec.validation_passed")
	if name != "emit_spec_validation_passed" {
		t.Fatalf("unexpected name %s", name)
	}
	eventType, ok := runtimetools.EventTypeFromEmitToolName(name, index)
	if !ok || eventType != "spec.validation_passed" {
		t.Fatalf("unexpected round trip ok=%v event=%q", ok, eventType)
	}
}

func TestIsEmitToolAllowedForRole(t *testing.T) {
	if !runtimetools.IsEmitToolAllowedForRole("empire-coordinator", "emit_scan_requested") {
		t.Fatal("expected allowed emit tool")
	}
	if runtimetools.IsEmitToolAllowedForRole("spec-reviewer", "emit_scan_requested") {
		t.Fatal("unexpected allowed emit tool")
	}
}

func TestGenerateEmitTools_SchemaPresentForAllProducedEvents(t *testing.T) {
	for _, role := range commgraph.ProducerRoles() {
		allowed := commgraph.ProducerEventsForRole(role)
		tools := runtimetools.GenerateEmitToolsForRole(role, nil)
		if len(allowed) != len(tools) {
			t.Fatalf("role %s expected %d tools, got %d", role, len(allowed), len(tools))
		}
		for _, tool := range tools {
			schema, ok := tool.Schema.(map[string]any)
			if !ok || schema["type"] != "object" {
				t.Fatalf("role %s tool %s schema invalid: %#v", role, tool.Name, tool.Schema)
			}
		}
	}
}

func TestEventSchemaRegistry_ExplicitForAllProducerEvents(t *testing.T) {
	registry := runtimecontracts.EventSchemaRegistry()
	missing := []string{}
	for _, role := range commgraph.ProducerRoles() {
		for _, eventType := range commgraph.ProducerEventsForRole(role) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := registry[eventType]; !ok {
				missing = append(missing, eventType+" (role="+role+")")
			}
		}
	}
	if len(missing) > 0 {
		t.Fatalf("missing explicit registry entries: %v", missing)
	}
}

func TestEventSchemaSnapshot_IncludesSpecSchemas(t *testing.T) {
	snapshot := runtimetools.EventSchemaSnapshot()
	if len(snapshot) == 0 {
		t.Fatal("expected non-empty snapshot")
	}
	if _, ok := snapshot["spec.approved"]; !ok {
		t.Fatal("expected spec.approved schema in snapshot")
	}
}

func TestGeneratedEmitSchemasForAgentRoles_None(t *testing.T) {
	if got := runtimetools.GeneratedEmitSchemasForAgentRoles(); len(got) != 0 {
		t.Fatalf("expected no generated schemas, got %v", got)
	}
}

func TestGenerateEmitTools_AllAgentSchemasStrictTopLevel(t *testing.T) {
	for _, role := range commgraph.ProducerRoles() {
		tools := runtimetools.GenerateEmitToolsForRole(role, nil)
		for _, tool := range tools {
			schema, ok := tool.Schema.(map[string]any)
			if !ok {
				t.Fatalf("role %s tool %s schema invalid", role, tool.Name)
			}
			addl, ok := schema["additionalProperties"].(bool)
			if !ok || addl {
				t.Fatalf("role %s tool %s expected strict schema", role, tool.Name)
			}
		}
	}
}

func TestScoringRequestedSchemaRejectsLegacyTaskID(t *testing.T) {
	if err := runtimetools.ValidateEventPayloadAgainstSchema("scoring.requested", map[string]any{
		"vertical_id":          "v1",
		"vertical_name":        "Dental Clinic Scheduling",
		"geography":            "argentina",
		"mode":                 "saas_gap",
		"rubric":               "universal",
		"dimensions_requested": []any{"build_complexity", "automation_completeness", "icp_crispness"},
		"discovery_context":    map[string]any{"opportunity_name": "Clinic scheduling optimization"},
		"task_id":              "task-123",
	}); err == nil {
		t.Fatal("expected scoring.requested to reject legacy task_id")
	}
}

func TestScanAndScoringSupportAutomationMicro(t *testing.T) {
	if err := runtimetools.ValidateEventPayloadAgainstSchema("scan.requested", map[string]any{
		"mode":             "automation_micro",
		"geography":        "Argentina",
		"campaign_context": map[string]any{"modes": []any{"automation_micro", "saas_gap"}, "strategic_context": "Automation-first micro opportunities.", "directive_id": "dir-1"},
	}); err != nil {
		t.Fatalf("expected scan.requested payload to validate, got %v", err)
	}
	if err := runtimetools.ValidateEventPayloadAgainstSchema("scoring.requested", map[string]any{
		"vertical_id":          "v-auto-1",
		"vertical_name":        "Appointment Recovery",
		"geography":            "argentina",
		"mode":                 "automation_micro",
		"rubric":               "universal",
		"dimensions_requested": []any{"build_complexity", "automation_completeness", "distribution_leverage"},
		"discovery_context":    map[string]any{"opportunity_name": "Appointment recovery automation"},
	}); err != nil {
		t.Fatalf("expected scoring.requested payload to validate, got %v", err)
	}
}
