package apispec

import "testing"

func TestPlatformSpecWorkflowTimerRootScopeRetainsLifecycleFlowInstance(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	timers := mustYAMLPath(t, root, "platform_tables", "tables", "timers")

	assertScalarContains(t, mustMappingValue(t, timers, "description"), "Entity-scoped timers: entity_id and flow_instance set")
	assertScalarContains(t, mustMappingValue(t, timers, "ddl"), "task_type <> 'workflow_timer' OR (run_id IS NOT NULL AND entity_id IS NOT NULL AND flow_instance = TRIM(flow_instance) AND NULLIF(flow_instance, '') IS NOT NULL)")
	entityScope := mustYAMLPath(t, timers, "scope_rules", "entity_scoped")
	assertScalarContains(t, entityScope, "Generic root-owned schedules")
	assertScalarContains(t, entityScope, "task_type=workflow_timer rows always retain their exact nonempty lifecycle flow_instance")
	assertScalarContains(t, entityScope, "root-owned rows whose producer routing_source remains root")
}
