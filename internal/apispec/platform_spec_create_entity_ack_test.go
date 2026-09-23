package apispec

import "testing"

func TestPlatformSpecCreateEntityCommittedPostCommitFailureResponse(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	create := mustYAMLPath(t, root, "tool_model", "platform_builtin_tools", "entity_tool_schemas", "create_entity")
	output := mustYAMLPath(t, create, "output")
	assertScalarContains(t, mustYAMLPath(t, output, "entity_id"), "created entity's ID")
	assertScalarContains(t, mustYAMLPath(t, output, "status"), "committed_with_post_commit_error")
	assertScalarContains(t, mustYAMLPath(t, output, "retry_write"), "must not be repeated")
	assertScalarContains(t, mustYAMLPath(t, output, "post_commit_error_code"), "entity_create_post_commit_failure")
	semantics := mustYAMLPath(t, create, "response_semantics")
	assertScalarContains(t, semantics, "generated canonical entity_id")
	assertScalarContains(t, semantics, "forbids a duplicate retry")
	assertScalarContains(t, semantics, "unacknowledged create returns")
}
