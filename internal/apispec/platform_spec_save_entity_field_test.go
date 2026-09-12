package apispec

import "testing"

func TestPlatformSpecSaveEntityFieldDeclaredDottedReplacement(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)

	rule := mustYAMLPath(t, root, "runtime_enforcement", "save_entity_field", "rule")
	assertScalarContains(t, rule, "declared dotted subpaths")
	assertScalarContains(t, rule, "named types")
	assertScalarContains(t, rule, "whole-value replacement")
	assertScalarContains(t, rule, "Bracket")
	assertScalarContains(t, rule, "index")
	assertScalarContains(t, rule, "dynamic path writes are rejected")
	assertScalarContains(t, rule, "Materialized/runtime-owned fields are rejected")

	inputField := mustYAMLPath(t, root, "tool_model", "platform_builtin_tools", "entity_tool_schemas", "save_entity_field", "input", "field")
	assertScalarContains(t, inputField, "declared top-level field name or declared dotted subpath")
	assertScalarContains(t, inputField, "must not be an envelope field")

	validation := mustYAMLPath(t, root, "tool_model", "platform_builtin_tools", "entity_tool_schemas", "save_entity_field", "validation")
	assertScalarContains(t, validation, "bracket/index/dynamic paths")
	assertScalarContains(t, validation, "Values must satisfy the resolved declared type")
}

func TestPlatformSpecGeneratedEntityUpdatesConsumeTypedMutationOwner(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	updatePath := mustYAMLPath(t, root, "contract_formats", "persistence_model", "role_scoped_entity_tools", "generated_writes", "update_path")

	assertScalarContains(t, updatePath, "exact declared subpath type")
	assertScalarContains(t, updatePath, "same declared typed mutation plan as handlers")
	assertScalarContains(t, updatePath, "A missing named parent rejects")
}
