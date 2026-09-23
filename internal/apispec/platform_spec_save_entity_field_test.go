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

func TestPlatformSpecGeneratedEntityUpdatesConsumeSaveEntityFieldPathOwner(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	updatePath := mustYAMLPath(t, root, "contract_formats", "persistence_model", "role_scoped_entity_tools", "generated_writes", "update_path")

	assertScalarContains(t, updatePath, "exact declared subpath type")
	assertScalarContains(t, updatePath, "same declared dotted replacement owner as save_entity_field")
}

func TestPlatformSpecSaveEntityFieldCommittedPostCommitFailureResponse(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	output := mustYAMLPath(t, root, "tool_model", "platform_builtin_tools", "entity_tool_schemas", "save_entity_field", "output")
	assertScalarContains(t, mustYAMLPath(t, output, "revision"), "new revision after write")
	assertScalarContains(t, mustYAMLPath(t, output, "status"), "committed_with_post_commit_error")
	assertScalarContains(t, mustYAMLPath(t, output, "retry_write"), "must not be repeated")
	assertScalarContains(t, mustYAMLPath(t, output, "post_commit_error_code"), "entity_field_write_post_commit_failure")
	semantics := mustYAMLPath(t, root, "tool_model", "platform_builtin_tools", "entity_tool_schemas", "save_entity_field", "response_semantics")
	assertScalarContains(t, semantics, "acknowledged write returns its committed revision")
	assertScalarContains(t, semantics, "raw joined cause is restricted")
	assertScalarContains(t, semantics, "returns write_failed without a revision")
	generated := mustYAMLPath(t, root, "contract_formats", "persistence_model", "role_scoped_entity_tools", "generated_writes", "response_semantics")
	assertScalarContains(t, generated, "inherit save_entity_field committed revision")
	assertScalarContains(t, generated, "MUST NOT expose raw SQL or connection error text")
}
