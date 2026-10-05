package apispec

import "testing"

func TestPlatformSpecRetiresCreateEntityTool(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	for _, catalog := range []string{"tools", "entity_tool_schemas"} {
		node := mustYAMLPath(t, root, "tool_model", "platform_builtin_tools", catalog)
		if mappingValue(node, "create_entity") != nil {
			t.Fatalf("retired constructor remains in %s", catalog)
		}
	}
}

func TestPlatformSpecConstructionPrecedesOrdinaryHandlers(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	for _, path := range [][]string{
		{"runtime_enforcement", "create_entity"},
		{"handler_specification", "handler_fields", "create_entity"},
	} {
		retired := mustYAMLPath(t, root, path...)
		assertScalarValue(t, mustMappingValue(t, retired, "status"), "retired_unsupported")
		assertScalarContains(t, mustMappingValue(t, retired, "rule"), "Ordinary handlers cannot construct")
		if mappingValue(retired, "entity_creation") != nil || mappingValue(retired, "interactions") != nil {
			t.Fatal("retired handler construction retains executable semantics")
		}
	}
	constructor := mustYAMLPath(t, root, "engine", "flow_constructor")
	assertScalarContains(t, mustMappingValue(t, constructor, "ordering"), "The creating input is delivered exactly once after readiness")
	projections := collectMappingValuesByKey(root, "target_owner_projection")
	if len(projections) != 1 {
		t.Fatalf("target owner projections = %d, want 1", len(projections))
	}
	assertScalarContains(t, projections[0], "Construction commits before ordinary handler delivery")
	assertScalarContains(t, projections[0], "header for every instance")
	availability := mustYAMLPath(t, root, "engine", "cross_flow_routing", "receiver_ownership", "availability")
	assertScalarContains(t, availability, "State-only rows cannot authorize ordinary execution")
	lifecycle := mustYAMLPath(t, root, "engine", "timer_model", "lifecycle")
	assertScalarContains(t, lifecycle.Content[0], "Ordinary handlers cannot synthesize InitialEntry")
	sourceStates := collectMappingValuesByKey(root, "source_state_derivation")
	if len(sourceStates) != 1 {
		t.Fatalf("source-state derivations = %d, want 1", len(sourceStates))
	}
	if mappingValue(sourceStates[0], "create_entity_handlers") != nil || mappingValue(sourceStates[0], "non_create_entity_handlers") != nil {
		t.Fatal("reachability still interprets retired construction flags")
	}
	assertScalarContains(t, mustYAMLPath(t, sourceStates[0], "ordinary_handlers", "rule"), "already-constructed instance")
}
