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
		"Static multi-entity flow ownership is retired model debt",
	} {
		assertScalarContains(t, locked, want)
	}

	coverage := mustMappingValue(t, authoring, "locked_design_coverage")
	assertScalarValue(t, mustMappingValue(t, coverage, "status"), "exhaustive_against_locked_1476_design")
	assertScalarContains(t, mustMappingValue(t, coverage, "rule"), "every major section of the locked #1476")
	coverageRows := mustMappingValue(t, coverage, "rows")
	for _, tc := range []struct {
		id       string
		coverage string
	}{
		{"locked_principle", "specified_by_1538"},
		{"locked_mental_model", "specified_by_1538"},
		{"authoring_decision_rubric", "specified_by_1538"},
		{"composition_model", "split_to_child"},
		{"delivery_vs_contained_state_update", "split_to_child"},
		{"escape_hatches", "specified_by_1538"},
		{"immediate_platform_surface", "split_to_child"},
		{"empire_migration_framing", "split_to_child"},
		{"analyzer_obligations", "split_to_child"},
		{"first_pilots", "split_to_child"},
	} {
		row := mustSequenceMappingByScalarField(t, coverageRows, "id", tc.id)
		assertScalarValue(t, mustMappingValue(t, row, "coverage"), tc.coverage)
		if !hasMappingKey(row, "owner") {
			t.Fatalf("locked_design_coverage row %s missing owner", tc.id)
		}
	}

	vocabulary := mustMappingValue(t, authoring, "vocabulary")
	for _, key := range []string{
		"flow_definition",
		"flow_instance",
		"primary_entity",
		"contained_state",
		"child_template_flow",
		"singleton_flow",
		"singleton_coordinator",
		"connect",
		"interface",
		"analyzer",
	} {
		if !hasMappingKey(vocabulary, key) {
			t.Fatalf("flow_instance_authoring.vocabulary missing %s", key)
		}
	}

	rubric := mustMappingValue(t, authoring, "authoring_decision_rubric")
	assertScalarValue(t, mustMappingValue(t, rubric, "status"), "merge_bearing_authoring_guidance")
	for _, tc := range []struct {
		id       string
		wantUse  string
		wantWhen string
	}{
		{"template_flow_instance", "child/template flow instance", "independent states"},
		{"contained_state", "typed field/list/map on the primary entity", "just data owned"},
		{"singleton_coordinator", "singleton flow with real typed map/list state consumed by exact contract rows", "learns across many instances"},
		{"promotion_line", "promote it to a child/template flow instance", "routable recipient"},
	} {
		decision := mustSequenceMappingByScalarField(t, mustMappingValue(t, rubric, "decisions"), "id", tc.id)
		assertScalarContains(t, mustMappingValue(t, decision, "when"), tc.wantWhen)
		assertScalarValue(t, mustMappingValue(t, decision, "use"), tc.wantUse)
	}

	normal := mustMappingValue(t, authoring, "normal_model")
	assertScalarContains(t, mustMappingValue(t, normal, "rule"), "normal unit of durable workflow state is the flow instance")
	assertScalarContains(t, mustMappingValue(t, normal, "rule"), "exactly one primary state entity")
	for _, want := range []string{
		"field = scalar state on the primary entity",
		"list/map = contained local state on the primary entity",
		"child/template flow instance = independently addressable lifecycle",
		"singleton flow = one durable instance, including intentionally stateless services",
		"singleton coordinator = singleton flow whose exact contract consumers use typed map/list state",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, normal, "identity_ladder"), want) {
			t.Fatalf("flow_instance_authoring.normal_model.identity_ladder missing %q", want)
		}
	}

	templateModel := mustMappingValue(t, authoring, "template_instance_model")
	assertScalarValue(t, mustMappingValue(t, templateModel, "status"), "merge_bearing_contract_behavior")
	assertScalarValue(t, mustMappingValue(t, templateModel, "implementation_tracker"), "#2087")
	assertScalarValue(t, mustMappingValue(t, templateModel, "canonical_code_owner"), "internal/runtime/contracts.WorkflowContractBundle.ResolveFlowTemplateInstance")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "process/case/job state")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "independent lifecycle")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "instance: <field>")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "sole create/select/select-or-create behavior owner")
	assertScalarContains(t, mustMappingValue(t, templateModel, "primary_entity_dependency"), "ResolveFlowPrimaryEntity")
	assertScalarContains(t, mustMappingValue(t, templateModel, "primary_entity_dependency"), "`schema.yaml entity`")
	assertScalarContains(t, mustMappingValue(t, templateModel, "key_rule"), "top-level scalar or enum field")
	assertScalarContains(t, mustMappingValue(t, templateModel, "key_rule"), "opaque validated semantic value")
	assertScalarContains(t, mustMappingValue(t, templateModel, "strict_retirement_rule"), "lifecycle policy fields")
	assertScalarContains(t, mustMappingValue(t, templateModel, "strict_retirement_rule"), "rejected on presence")
	assertScalarContains(t, mustMappingValue(t, templateModel, "connect_time_lifecycle_rule"), "Field, Source, and Mode")
	assertScalarContains(t, mustMappingValue(t, templateModel, "connect_time_lifecycle_rule"), "route_plan_instance_conflict")
	assertScalarContains(t, mustMappingValue(t, templateModel, "non_authoritative_paths"), "Retired output-pin key/carries")
	assertScalarContains(t, mustMappingValue(t, templateModel, "non_authoritative_paths"), "create_flow_instance actions")

	primaryEntity := mustMappingValue(t, authoring, "primary_entity_model")
	assertScalarValue(t, mustMappingValue(t, primaryEntity, "status"), "merge_bearing_contract_behavior")
	assertScalarValue(t, mustMappingValue(t, primaryEntity, "implementation_tracker"), "#1539")
	assertScalarValue(t, mustMappingValue(t, primaryEntity, "declaration_surface"), "exactly one flow-owned entities.yaml entry; root entities.yaml uses the same resolver when present")
	assertScalarValue(t, mustMappingValue(t, primaryEntity, "canonical_code_owner"), "internal/runtime/contracts.WorkflowContractBundle.ResolveRootPrimaryEntity / ResolveFlowPrimaryEntity")
	assertScalarContains(t, mustMappingValue(t, primaryEntity, "single_entity_rule"), "exactly one entity type")
	assertScalarContains(t, mustMappingValue(t, primaryEntity, "single_entity_rule"), "schema.yaml entity")
	assertScalarContains(t, mustMappingValue(t, primaryEntity, "stateful_presence_rule"), "stateful normal child flow")

	composition := mustMappingValue(t, authoring, "composition_model")
	assertScalarValue(t, mustMappingValue(t, composition, "canonical_routing_owner"), "platform-spec.yaml#flow_model.composition_routing")
	assertScalarValue(t, mustMappingValue(t, composition, "route_plan_owner"), "platform-spec.yaml#contract_formats.event_schema.routing_derivation.route_plan_authority")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "owns only the directed edge")
	assertScalarValue(t, mustYAMLPath(t, composition, "public_target_revision", "revise"), "remove parent-owned receiver identity, cardinality, and lifecycle syntax")
	assertScalarValue(t, mustYAMLPath(t, composition, "public_target_revision", "prefer"), "edge-only connect + receiver-owned scalar instance/resolution semantics")
	assertScalarValue(t, mustYAMLPath(t, composition, "split_children", "output_pin_key_carries"), "#1544")
	assertScalarValue(t, mustYAMLPath(t, composition, "split_children", "connect_to_instance_route_planning"), "#1545")
	assertScalarValue(t, mustYAMLPath(t, composition, "split_children", "retired_connect_key_adapters"), "#2087")
	retiredAliases := mustMappingValue(t, composition, "retired_connect_identity_aliases")
	assertScalarValue(t, mustMappingValue(t, retiredAliases, "implementation_tracker"), "#2087")
	assertScalarValue(t, mustMappingValue(t, retiredAliases, "canonical_code_owner"), "strict contract decoders")
	assertScalarContains(t, mustMappingValue(t, retiredAliases, "rule"), "no dormant DTO")
	if !sequenceContainsScalar(mustMappingValue(t, retiredAliases, "fail_closed"), "any connect using.instance field") {
		t.Fatal("retired aliases must fail closed for using.instance")
	}
	outputContract := mustMappingValue(t, composition, "output_pin_key_carries_contract")
	assertScalarValue(t, mustMappingValue(t, outputContract, "status"), "retired_on_presence")
	assertScalarValue(t, mustMappingValue(t, outputContract, "implementation_tracker"), "#2352")
	assertScalarContains(t, mustMappingValue(t, outputContract, "canonical_code_owner"), "CompiledEventSchema")
	assertScalarContains(t, mustMappingValue(t, outputContract, "canonical_code_owner"), "ConnectRoutePlan")
	assertScalarContains(t, mustMappingValue(t, outputContract, "rule"), "invalid on presence")
	assertScalarContains(t, mustMappingValue(t, outputContract, "rule"), "No compatibility decoder")
	assertScalarContains(t, mustMappingValue(t, outputContract, "non_authoritative_paths"), "never choose or restate")

	contained := mustMappingValue(t, authoring, "contained_state_model")
	assertScalarValue(t, mustMappingValue(t, contained, "implementation_tracker"), "#1548")
	assertScalarContains(t, mustMappingValue(t, contained, "rule"), "Typed lists and maps are contained state")
	assertScalarContains(t, mustMappingValue(t, contained, "rule"), "MUST NOT be addressed through")
	assertScalarContains(t, mustMappingValue(t, contained, "rule"), "promoted to a child/template")

	effectiveMode := mustMappingValue(t, authoring, "effective_flow_mode_model")
	assertScalarValue(t, mustMappingValue(t, effectiveMode, "implementation_tracker"), "#2238")
	assertScalarContains(t, mustMappingValue(t, effectiveMode, "canonical_code_owner"), "ResolveEffectiveFlowMode")
	assertScalarContains(t, mustMappingValue(t, effectiveMode, "rule"), "MUST agree")
	assertScalarContains(t, mustMappingValue(t, effectiveMode, "rule"), "before semantic source publication")
	if !sequenceContainsScalar(mustMappingValue(t, effectiveMode, "non_authoritative_paths"), "raw ProjectFlowRef.Mode after contract loading") {
		t.Fatal("effective_flow_mode_model must retire raw package mode as behavioral authority")
	}

	cardinality := mustMappingValue(t, authoring, "singleton_cardinality_model")
	assertScalarValue(t, mustMappingValue(t, cardinality, "implementation_tracker"), "#2238")
	assertScalarContains(t, mustMappingValue(t, cardinality, "canonical_code_owner"), "ResolveFlowSingleton")
	assertScalarContains(t, mustMappingValue(t, cardinality, "rule"), "cardinality and lifecycle only")
	assertScalarContains(t, mustMappingValue(t, cardinality, "rule"), "empty or scalar-only")
	assertScalarContains(t, mustMappingValue(t, cardinality, "rule"), "does not by itself grant coordinator")

	coordinator := mustMappingValue(t, authoring, "singleton_coordinator_model")
	assertScalarValue(t, mustMappingValue(t, coordinator, "status"), "merge_bearing_contract_runtime_behavior")
	assertScalarValue(t, mustMappingValue(t, coordinator, "implementation_tracker"), "#1549")
	assertScalarValue(t, mustMappingValue(t, coordinator, "refinement_tracker"), "#2238")
	assertScalarContains(t, mustMappingValue(t, coordinator, "declaration_surface"), "exact typed map/list consumer usage")
	assertScalarContains(t, mustMappingValue(t, coordinator, "canonical_code_owner"), "BuildSingletonCoordinatorDemandProjection")
	assertScalarContains(t, mustMappingValue(t, coordinator, "canonical_code_owner"), "ResolveFlowSingletonCoordinator")
	assertScalarContains(t, mustMappingValue(t, coordinator, "canonical_code_owner"), "applyContainedDataOperation")
	assertScalarContains(t, mustMappingValue(t, coordinator, "rule"), "derived from exact authored consumers")
	assertScalarContains(t, mustMappingValue(t, coordinator, "rule"), "Map/list declaration shape without an exact consumer")
	assertScalarContains(t, mustMappingValue(t, coordinator, "lifecycle_policy"), "archive, roll up, clean up, or promote")
	assertScalarContains(t, mustMappingValue(t, coordinator, "promotion_rule"), "#1553")
	for _, want := range []string{
		"bare mode: static used as singleton/coordinator proof",
		"coordinator demand exists but the singleton primary entity lacks typed contained map/list state",
		"singleton flow contained map/list value or item types do not resolve",
		"agent conversation/session memory is used as coordinator state authority",
		"contained map/list items are targeted as route recipients",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, coordinator, "fail_closed"), want) {
			t.Fatalf("singleton_coordinator_model.fail_closed missing %q", want)
		}
	}
	for _, want := range []string{
		"mode: static as implicit coordinator declaration",
		"agent memory intent as lifecycle or coordinator authority",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, coordinator, "non_authoritative_paths"), want) {
			t.Fatalf("singleton_coordinator_model.non_authoritative_paths missing %q", want)
		}
	}

	escapeHatches := mustMappingValue(t, authoring, "escape_hatches")
	staticMulti := mustMappingValue(t, escapeHatches, "static_multi_entity_flows")
	assertScalarValue(t, mustMappingValue(t, staticMulti, "status"), "retired_unsupported")
	assertScalarValue(t, mustMappingValue(t, staticMulti, "implementation_tracker"), "#1554")
	assertScalarContains(t, mustMappingValue(t, staticMulti, "rule"), "Static multi-row ownership is retired")
	selectEntity := mustMappingValue(t, escapeHatches, "select_entity")
	assertScalarValue(t, mustMappingValue(t, selectEntity, "implementation_tracker"), "#1547")
	assertScalarContains(t, mustMappingValue(t, selectEntity, "rule"), "separately owned non-static/runtime surfaces")
	assertScalarContains(t, mustMappingValue(t, selectEntity, "rule"), "Normal in-topology composition")
	producerRouting := mustMappingValue(t, escapeHatches, "producer_emit_routing")
	assertScalarValue(t, mustMappingValue(t, producerRouting, "status"), "retired_unsupported")
	assertScalarValue(t, mustMappingValue(t, producerRouting, "implementation_tracker"), "#2086")
	assertScalarValue(t, mustMappingValue(t, producerRouting, "adapter_retirement_tracker"), "#2352")
	assertScalarContains(t, mustMappingValue(t, producerRouting, "rule"), "retired on presence")
	assertScalarContains(t, mustMappingValue(t, producerRouting, "rule"), "No adapter can restore producer")
	customAdapters := mustMappingValue(t, escapeHatches, "custom_adapters")
	assertScalarValue(t, mustMappingValue(t, customAdapters, "status"), "retired_unsupported")
	assertScalarValue(t, mustMappingValue(t, customAdapters, "implementation_tracker"), "#2352")
	assertScalarContains(t, mustMappingValue(t, customAdapters, "rule"), "connect.adapter")
	assertScalarContains(t, mustMappingValue(t, customAdapters, "rule"), "distinct declared event contract")

	migration := mustMappingValue(t, authoring, "migration_model")
	assertScalarValue(t, mustMappingValue(t, migration, "status"), "child_tracked")
	assertScalarContains(t, mustMappingValue(t, migration, "rule"), "migrated blindly")
	assertScalarValue(t, mustYAMLPath(t, migration, "implementation_trackers", "template_pilot"), "#1552")
	assertScalarValue(t, mustYAMLPath(t, migration, "implementation_trackers", "singleton_map_pilot"), "#1553")
	assertScalarValue(t, mustYAMLPath(t, migration, "implementation_trackers", "static_multi_entity_escape_hatch_policy"), "#1554")
	pilot := mustMappingValue(t, authoring, "pilot_model")
	assertScalarValue(t, mustMappingValue(t, pilot, "status"), "child_tracked")
	assertScalarContains(t, mustMappingValue(t, pilot, "rule"), "both a template pilot and a singleton+map")
	assertScalarValue(t, mustYAMLPath(t, pilot, "implementation_trackers", "template_pilot"), "#1552")
	assertScalarValue(t, mustYAMLPath(t, pilot, "implementation_trackers", "singleton_map_pilot"), "#1553")

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
		{"connect_to_instance_route_plans", "#1545"},
		{"connect_key_mapping", "#1546"},
		{"ambiguous_key_rejection", "#1545"},
		{"select_entity_demotion", "#1547"},
		{"typed_map_list_update_verification", "#1548"},
		{"singleton_coordinator_contract", "#1549"},
		{"expand_minimize_tooling", "#1551"},
	} {
		assertScalarValue(t, mustYAMLPath(t, analyzer, "children", tc.key), tc.want)
	}
	expandMinimize := mustMappingValue(t, analyzer, "expand_minimize_tooling")
	assertScalarValue(t, mustMappingValue(t, expandMinimize, "status"), "merge_bearing_supported_tooling")
	assertScalarValue(t, mustMappingValue(t, expandMinimize, "implementation_tracker"), "#1551")
	assertScalarValue(t, mustMappingValue(t, expandMinimize, "command"), "swarm describe")
	assertScalarContains(t, mustMappingValue(t, expandMinimize, "canonical_code_owner"), "internal/runtime/authoringview.Build")
	assertScalarContains(t, mustMappingValue(t, expandMinimize, "rule"), "projection over existing semantic owners")
	assertScalarContains(t, mustMappingValue(t, expandMinimize, "rule"), "without becoming a new semantic owner")
	assertScalarContains(t, mustMappingValue(t, expandMinimize, "source_location_rule"), "check_id")
	assertScalarContains(t, mustMappingValue(t, expandMinimize, "source_location_rule"), "authored YAML file")
	assertScalarContains(t, mustMappingValue(t, expandMinimize, "source_location_rule"), "remediation/evidence")
	if !sequenceContainsScalar(mustMappingValue(t, expandMinimize, "non_authoritative_paths"), "generated expanded YAML as merge authority") {
		t.Fatal("expand_minimize_tooling must keep generated expanded YAML non-authoritative")
	}
}
