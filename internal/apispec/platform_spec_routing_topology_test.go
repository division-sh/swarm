package apispec

import "testing"

func TestPlatformSpecPromotesVersionedRoutingTopologyArtifact(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	routing := mustYAMLPath(t, root, "flow_model", "flow_instance_authoring", "analyzer_obligations", "expand_minimize_tooling", "routing_topology")

	assertScalarValue(t, mustMappingValue(t, routing, "command"), "swarm describe routes")
	assertScalarValue(t, mustMappingValue(t, routing, "schema_version"), "routing-topology/v1")
	assertScalarValue(t, mustMappingValue(t, routing, "projection_only"), "true")
	assertScalarContains(t, mustMappingValue(t, routing, "canonical_code_owner"), "internal/runtime/routingtopology.Build")
	assertScalarContains(t, mustMappingValue(t, routing, "evolution_rule"), "new artifact schema identity")
	assertScalarContains(t, mustMappingValue(t, routing, "evolution_rule"), "Runtime recipients")
	assertScalarContains(t, mustMappingValue(t, routing, "endpoint_rule"), "immutable authored endpoint census")
	assertScalarContains(t, mustMappingValue(t, routing, "endpoint_rule"), "interface exposures")
	assertScalarContains(t, mustMappingValue(t, routing, "resolution_rule"), "select-or-create")
	assertScalarContains(t, mustMappingValue(t, routing, "identity_rule"), "fail closed")

	rows := mustYAMLPath(t, root, "cli_specification", "foundations", "output_contract", "command_support", "output_conformance_registry", "rows")
	row := mustMappingValue(t, rows, "describe_routes")
	assertScalarValue(t, mustMappingValue(t, row, "command"), "swarm describe routes")
	assertScalarValue(t, mustMappingValue(t, row, "classification"), "shared_output")
	assertScalarContains(t, mustMappingValue(t, row, "json_shape"), "routing-topology/v1")
}
