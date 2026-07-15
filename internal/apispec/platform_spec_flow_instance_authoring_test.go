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
		{"singleton_coordinator", "singleton coordinator with real shared state or policy", "learns across many instances"},
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
		"singleton coordinator = shared policy, aggregate state, or cross-instance learning",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, normal, "identity_ladder"), want) {
			t.Fatalf("flow_instance_authoring.normal_model.identity_ladder missing %q", want)
		}
	}

	templateModel := mustMappingValue(t, authoring, "template_instance_model")
	assertScalarValue(t, mustMappingValue(t, templateModel, "status"), "merge_bearing_contract_behavior")
	assertScalarValue(t, mustMappingValue(t, templateModel, "implementation_tracker"), "#1543")
	assertScalarValue(t, mustMappingValue(t, templateModel, "policy_default_tracker"), "#1595")
	assertScalarValue(t, mustMappingValue(t, templateModel, "canonical_code_owner"), "internal/runtime/contracts.WorkflowContractBundle.ResolveFlowTemplateInstance")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "process/case/job state")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "independent lifecycle")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "`mode: template` flows MUST declare `instance.by`")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "`on_missing` defaults to")
	assertScalarContains(t, mustMappingValue(t, templateModel, "rule"), "`on_conflict` defaults to")
	assertScalarContains(t, mustMappingValue(t, templateModel, "primary_entity_dependency"), "ResolveFlowPrimaryEntity")
	assertScalarContains(t, mustMappingValue(t, templateModel, "primary_entity_dependency"), "`schema.yaml entity`")
	assertScalarContains(t, mustMappingValue(t, templateModel, "key_rule"), "top-level scalar or enum field")
	assertScalarContains(t, mustMappingValue(t, templateModel, "key_rule"), "Composite key material is ordered exactly as declared")
	assertScalarContains(t, mustMappingValue(t, templateModel, "policy_rule"), "`on_missing` MAY be omitted and defaults to `create`")
	assertScalarContains(t, mustMappingValue(t, templateModel, "policy_rule"), "`on_conflict` MAY be omitted and defaults to `reject`")
	assertScalarContains(t, mustMappingValue(t, templateModel, "policy_rule"), "explicit empty")
	assertScalarContains(t, mustMappingValue(t, templateModel, "policy_rule"), "Generated/expanded authoring")
	assertScalarContains(t, mustMappingValue(t, templateModel, "non_authoritative_paths"), "Flow input-pin `address`")
	assertScalarContains(t, mustMappingValue(t, templateModel, "non_authoritative_paths"), "`create_flow_instance.instance_id_from`")

	primaryEntity := mustMappingValue(t, authoring, "primary_entity_model")
	assertScalarValue(t, mustMappingValue(t, primaryEntity, "status"), "merge_bearing_contract_behavior")
	assertScalarValue(t, mustMappingValue(t, primaryEntity, "implementation_tracker"), "#1539")
	assertScalarValue(t, mustMappingValue(t, primaryEntity, "declaration_surface"), "exactly one flow-owned entities.yaml entry; root entities.yaml uses the same resolver when present")
	assertScalarValue(t, mustMappingValue(t, primaryEntity, "canonical_code_owner"), "internal/runtime/contracts.WorkflowContractBundle.ResolveRootPrimaryEntity / ResolveFlowPrimaryEntity")
	assertScalarContains(t, mustMappingValue(t, primaryEntity, "single_entity_rule"), "exactly one entity type")
	assertScalarContains(t, mustMappingValue(t, primaryEntity, "single_entity_rule"), "schema.yaml entity")
	assertScalarContains(t, mustMappingValue(t, primaryEntity, "stateful_presence_rule"), "stateful normal child flow")

	composition := mustMappingValue(t, authoring, "composition_model")
	assertScalarValue(t, mustMappingValue(t, composition, "canonical_routing_owner"), "platform-spec.yaml#flow_model.flow_package.composition_routing")
	assertScalarValue(t, mustMappingValue(t, composition, "route_plan_owner"), "platform-spec.yaml#contract_formats.event_schema.routing_derivation.route_plan_authority")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "Parent `connect` routes across flow instances")
	assertScalarValue(t, mustYAMLPath(t, composition, "public_target_revision", "revise"), "parent-owned correlate_by/cardinality as the main authoring syntax")
	assertScalarValue(t, mustYAMLPath(t, composition, "public_target_revision", "prefer"), "edge-only connect + receiver-owned address/resolution semantics")
	assertScalarValue(t, mustYAMLPath(t, composition, "split_children", "output_pin_key_carries"), "#1544")
	assertScalarValue(t, mustYAMLPath(t, composition, "split_children", "connect_to_instance_route_planning"), "#1545")
	assertScalarValue(t, mustYAMLPath(t, composition, "split_children", "connect_key_adapters"), "#1546")
	keyAdapters := mustMappingValue(t, composition, "connect_key_adapters")
	assertScalarValue(t, mustMappingValue(t, keyAdapters, "implementation_tracker"), "#1546")
	assertScalarContains(t, mustMappingValue(t, keyAdapters, "canonical_code_owner"), "FlowPackageConnect.Using.Instance")
	assertScalarContains(t, mustMappingValue(t, keyAdapters, "canonical_code_owner"), "validateCompositionConnectInstanceKeyAdapter")
	assertScalarContains(t, mustMappingValue(t, keyAdapters, "canonical_code_owner"), "ConnectRoutePlan.InstanceKey.Mappings")
	assertScalarValue(t, mustMappingValue(t, keyAdapters, "syntax"), "connect.using.instance.source / connect.using.instance.target")
	assertScalarContains(t, mustMappingValue(t, keyAdapters, "rule"), "Same-name")
	assertScalarContains(t, mustMappingValue(t, keyAdapters, "rule"), "`connect.map` is not adapter authority")
	if !sequenceContainsScalar(mustMappingValue(t, keyAdapters, "fail_closed"), "using.instance declared on addressed-input, broadcast, or non-template receiver routes") {
		t.Fatal("connect_key_adapters must fail closed for unsupported declaration surfaces")
	}
	if !sequenceContainsScalar(mustMappingValue(t, keyAdapters, "non_authoritative_paths"), "connect.map for addressless template instance-key adapters") {
		t.Fatal("connect_key_adapters must mark connect.map non-authoritative for addressless template instance-key adapters")
	}
	outputContract := mustMappingValue(t, composition, "output_pin_key_carries_contract")
	assertScalarValue(t, mustMappingValue(t, outputContract, "implementation_tracker"), "#1544")
	assertScalarContains(t, mustMappingValue(t, outputContract, "canonical_code_owner"), "FlowOutputEventPin")
	assertScalarContains(t, mustMappingValue(t, outputContract, "canonical_code_owner"), "checkOutputPinKeyCarriesValidation")
	assertScalarContains(t, mustMappingValue(t, outputContract, "rule"), "MUST declare `key` and `carries`")
	assertScalarContains(t, mustMappingValue(t, outputContract, "non_authoritative_paths"), "connect.map")

	contained := mustMappingValue(t, authoring, "contained_state_model")
	assertScalarValue(t, mustMappingValue(t, contained, "implementation_tracker"), "#1548")
	assertScalarContains(t, mustMappingValue(t, contained, "rule"), "Typed lists and maps are contained state")
	assertScalarContains(t, mustMappingValue(t, contained, "rule"), "MUST NOT be addressed through")
	assertScalarContains(t, mustMappingValue(t, contained, "rule"), "promoted to a child/template")

	coordinator := mustMappingValue(t, authoring, "singleton_coordinator_model")
	assertScalarValue(t, mustMappingValue(t, coordinator, "status"), "merge_bearing_contract_runtime_behavior")
	assertScalarValue(t, mustMappingValue(t, coordinator, "implementation_tracker"), "#1549")
	assertScalarValue(t, mustMappingValue(t, coordinator, "declaration_surface"), "mode: singleton")
	assertScalarContains(t, mustMappingValue(t, coordinator, "canonical_code_owner"), "ResolveFlowSingletonCoordinator")
	assertScalarContains(t, mustMappingValue(t, coordinator, "canonical_code_owner"), "checkSingletonCoordinatorValidation")
	assertScalarContains(t, mustMappingValue(t, coordinator, "canonical_code_owner"), "applyContainedDataOperation")
	assertScalarContains(t, mustMappingValue(t, coordinator, "rule"), "shared policy, aggregate state, or cross-instance learning")
	assertScalarContains(t, mustMappingValue(t, coordinator, "rule"), "Bare `mode: static` is")
	assertScalarContains(t, mustMappingValue(t, coordinator, "lifecycle_policy"), "archive, roll up, clean up, or promote")
	assertScalarContains(t, mustMappingValue(t, coordinator, "promotion_rule"), "#1553")
	for _, want := range []string{
		"bare mode: static used as singleton/coordinator proof",
		"mode: singleton declares template instance fields",
		"singleton flow primary entity lacks typed contained map/list state",
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
	producerTarget := mustMappingValue(t, escapeHatches, "producer_emit_target")
	assertScalarValue(t, mustMappingValue(t, producerTarget, "status"), "exotic_dynamic_routing_escape_hatch")
	assertScalarValue(t, mustMappingValue(t, producerTarget, "implementation_tracker"), "#1545")
	assertScalarValue(t, mustMappingValue(t, producerTarget, "adjacent_authority"), "platform-spec.yaml#flow_model.flow_package.composition_routing.emit_target_escape_hatch")
	assertScalarValue(t, mustMappingValue(t, producerTarget, "adapter_boundary_tracker"), "#1546")
	assertScalarContains(t, mustMappingValue(t, producerTarget, "rule"), "genuinely exotic dynamic routing")
	assertScalarContains(t, mustMappingValue(t, producerTarget, "rule"), "#1545 owns the route-planning proof")
	customAdapters := mustMappingValue(t, escapeHatches, "custom_adapters")
	assertScalarValue(t, mustMappingValue(t, customAdapters, "implementation_tracker"), "#1546")
	assertScalarContains(t, mustMappingValue(t, customAdapters, "rule"), "explicit parent-owned mappings")

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
