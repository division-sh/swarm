package apispec

import "testing"

func TestPlatformSpecFlowInstanceAuthoringSourceAuthority(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	authoring := mustYAMLPath(t, root, "flow_model", "flow_instance_authoring")

	assertScalarValue(t, mustMappingValue(t, authoring, "status"), "merge_bearing_source_authority")
	assertScalarValue(t, mustMappingValue(t, authoring, "promoted_by"), "#1538")
	assertScalarValue(t, mustMappingValue(t, authoring, "parent_tracker"), "#1537")
	assertScalarValue(t, mustMappingValue(t, authoring, "owner"), "platform-spec.yaml#flow_model.flow_instance_authoring")

	locked := mustMappingValue(t, authoring, "locked_principle")
	for _, want := range []string{
		"flow-instance-centered",
		"one primary state entity",
		"`connect` and instance keys",
		"Lists and maps are contained state",
		"child/template flows",
		"lifecycle, routing, timers, retries, agents, or audit",
		"Static multi-entity flows remain an advanced/legacy escape hatch",
	} {
		assertScalarContains(t, locked, want)
	}

	vocabulary := mustMappingValue(t, authoring, "vocabulary")
	for _, key := range []string{
		"flow_definition",
		"flow_instance",
		"primary_entity",
		"contained_state",
		"child_template_flow",
		"singleton_coordinator",
		"connect",
		"interface",
		"analyzer",
	} {
		if !hasMappingKey(vocabulary, key) {
			t.Fatalf("flow_instance_authoring.vocabulary missing %s", key)
		}
	}

	normal := mustMappingValue(t, authoring, "normal_model")
	assertScalarContains(t, mustMappingValue(t, normal, "rule"), "normal unit of durable workflow state is the flow instance")
	assertScalarContains(t, mustMappingValue(t, normal, "rule"), "exactly one primary state entity")
	for _, want := range []string{
		"field = scalar state on the primary entity",
		"list/map = contained local state on the primary entity",
		"child/template flow instance = independently addressable lifecycle",
		"singleton coordinator = shared policy, aggregate state, or cross-instance learning",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, normal, "identity_ladder"), want) {
			t.Fatalf("flow_instance_authoring.normal_model.identity_ladder missing %q", want)
		}
	}

	templateModel := mustMappingValue(t, authoring, "template_instance_model")
	assertScalarValue(t, mustMappingValue(t, templateModel, "status"), "spec_vocabulary_only")
	assertScalarValue(t, mustMappingValue(t, templateModel, "implementation_tracker"), "#1543")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "process/case/job state")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "independent lifecycle")

	primaryEntity := mustMappingValue(t, authoring, "primary_entity_model")
	assertScalarValue(t, mustMappingValue(t, primaryEntity, "implementation_tracker"), "#1539")
	assertScalarContains(t, mustMappingValue(t, primaryEntity, "inference_rule"), "exactly one entity type")
	assertScalarContains(t, mustMappingValue(t, primaryEntity, "inference_rule"), "advanced/legacy multi-entity escape hatch")

	composition := mustMappingValue(t, authoring, "composition_model")
	assertScalarValue(t, mustMappingValue(t, composition, "canonical_routing_owner"), "platform-spec.yaml#flow_model.flow_package.composition_routing")
	assertScalarValue(t, mustMappingValue(t, composition, "route_plan_owner"), "platform-spec.yaml#contract_formats.event_schema.routing_derivation.route_plan_authority")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "Parent `connect` routes across flow instances")
	assertScalarValue(t, mustYAMLPath(t, composition, "split_children", "output_pin_key_carries"), "#1544")
	assertScalarValue(t, mustYAMLPath(t, composition, "split_children", "connect_to_instance_route_planning"), "#1545")
	assertScalarValue(t, mustYAMLPath(t, composition, "split_children", "connect_key_adapters"), "#1546")

	contained := mustMappingValue(t, authoring, "contained_state_model")
	assertScalarValue(t, mustMappingValue(t, contained, "implementation_tracker"), "#1548")
	assertScalarContains(t, mustMappingValue(t, contained, "rule"), "Typed lists and maps are contained state")
	assertScalarContains(t, mustMappingValue(t, contained, "rule"), "MUST NOT be addressed through")
	assertScalarContains(t, mustMappingValue(t, contained, "rule"), "promoted to a child/template")

	coordinator := mustMappingValue(t, authoring, "singleton_coordinator_model")
	assertScalarValue(t, mustMappingValue(t, coordinator, "implementation_tracker"), "#1549")
	assertScalarContains(t, mustMappingValue(t, coordinator, "rule"), "shared policy, aggregate state, or cross-instance learning")

	escapeHatches := mustMappingValue(t, authoring, "escape_hatches")
	staticMulti := mustMappingValue(t, escapeHatches, "static_multi_entity_flows")
	assertScalarValue(t, mustMappingValue(t, staticMulti, "status"), "advanced_legacy_escape_hatch")
	assertScalarValue(t, mustMappingValue(t, staticMulti, "implementation_tracker"), "#1554")
	assertScalarContains(t, mustMappingValue(t, staticMulti, "rule"), "not the default authoring model")
	selectEntity := mustMappingValue(t, escapeHatches, "select_entity")
	assertScalarValue(t, mustMappingValue(t, selectEntity, "implementation_tracker"), "#1547")
	assertScalarContains(t, mustMappingValue(t, selectEntity, "rule"), "external ingress, legacy migration")
	assertScalarContains(t, mustMappingValue(t, selectEntity, "rule"), "Normal in-topology composition")

	analyzer := mustMappingValue(t, authoring, "analyzer_obligations")
	assertScalarValue(t, mustMappingValue(t, analyzer, "status"), "child_tracked")
	assertScalarContains(t, mustMappingValue(t, analyzer, "rule"), "source authority only")
	for _, tc := range []struct {
		key  string
		want string
	}{
		{"primary_entity_inference", "#1539"},
		{"instance_key_verification", "#1543"},
		{"output_key_carries_verification", "#1544"},
		{"connect_key_mapping", "#1546"},
		{"select_entity_demotion", "#1547"},
		{"typed_map_list_update_verification", "#1548"},
		{"expand_minimize_tooling", "#1551"},
	} {
		assertScalarValue(t, mustYAMLPath(t, analyzer, "children", tc.key), tc.want)
	}
}
