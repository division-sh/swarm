package apispec

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPlatformSpecCompositionRoutingSourceAuthority(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	composition := mustYAMLPath(t, root, "flow_model", "flow_package", "composition_routing")

	assertScalarValue(t, mustMappingValue(t, composition, "status"), "source_authority_promoted_runtime_migration_pending")
	assertScalarValue(t, mustMappingValue(t, composition, "promoted_by"), "#1467")
	assertScalarValue(t, mustMappingValue(t, composition, "parent_decision"), "#1466")
	assertScalarValue(t, mustMappingValue(t, composition, "owner"), "platform-spec.yaml#flow_model.flow_package.composition_routing")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "Parent-authored composition routing is the canonical source authority")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "Producer emit sites MUST NOT own consumer routing")

	authored := mustMappingValue(t, composition, "authored_shapes")
	addressed := mustMappingValue(t, authored, "addressed_input_pin")
	assertScalarContains(t, mustMappingValue(t, addressed, "canonical_form"), "{name, event, address}")
	assertScalarContains(t, mustYAMLPath(t, addressed, "address_fields", "cardinality"), "one and many")

	connect := mustMappingValue(t, authored, "parent_connect")
	assertScalarValue(t, mustMappingValue(t, connect, "location"), "parent package.yaml connect")
	assertScalarContains(t, mustMappingValue(t, connect, "canonical_form"), "`connect` is a list")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "from"), "producer")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "to"), "receiver")

	ownership := mustMappingValue(t, composition, "ownership_split")
	assertScalarContains(t, mustMappingValue(t, ownership, "parent_connect"), "owns inter-flow delivery topology")
	assertScalarContains(t, mustMappingValue(t, ownership, "input_pins"), "receiver address resolution")
	assertScalarContains(t, mustMappingValue(t, ownership, "producer_emit_target"), "exceptional dynamic routing")

	verify := mustMappingValue(t, composition, "analyzer_verify_requirements")
	for _, key := range []string{
		"producer_flow_exists",
		"producer_output_pin_exists",
		"receiver_flow_exists",
		"receiver_input_pin_exists",
		"event_alias_or_adapter_valid",
		"output_carries_address_key",
		"receiver_address_rule_present",
		"key_types_compatible",
		"delivery_topology_valid",
		"reply_lineage_usable",
		"inference_unambiguous",
		"lowered_route_plan_concrete",
	} {
		if !hasMappingKey(verify, key) {
			t.Fatalf("composition_routing analyzer_verify_requirements missing %s", key)
		}
	}

	lowering := mustMappingValue(t, composition, "route_plan_lowering")
	assertScalarValue(t, mustMappingValue(t, lowering, "owner"), "platform-spec.yaml#flow_model.flow_package.composition_routing.route_plan_lowering")
	for _, want := range []string{
		"parent package.yaml connect entries",
		"receiver addressed input pin rules",
		"import-boundary pin alias bindings",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, lowering, "consumes"), want) {
			t.Fatalf("route_plan_lowering consumes missing %q", want)
		}
	}
	for _, want := range []string{
		"concrete target route for delivery: one",
		"concrete target_set routes for delivery: many or broadcast",
		"reply route lineage for delivery: reply",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, lowering, "produces"), want) {
			t.Fatalf("route_plan_lowering produces missing %q", want)
		}
	}

	assertScalarContains(t, mustYAMLPath(t, composition, "pin_alias_delivery_composition", "owner_consumed"), "pin_alias_delivery")
	assertScalarContains(t, mustYAMLPath(t, composition, "emit_target_escape_hatch", "role"), "not a compatibility path")
	assertScalarContains(t, mustYAMLPath(t, composition, "split_boundaries", "runtime_route_consumption"), "does not claim runtime behavior closure")
}

func TestPlatformSpecCompositionRoutingDemotesProducerTargetAuthority(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)

	crossFlow := mustYAMLPath(t, root, "engine", "cross_flow_routing")
	assertScalarValue(t, mustMappingValue(t, crossFlow, "canonical_owner"), "platform-spec.yaml#flow_model.flow_package.composition_routing")
	assertScalarValue(t, mustMappingValue(t, crossFlow, "implementation_status"), "source_authority_promoted_runtime_migration_pending")
	if !sequenceContainsScalar(mustYAMLPath(t, crossFlow, "target_resolution", "precedence"), "lowered parent connect route plan") {
		t.Fatal("cross_flow_routing target precedence must start from lowered parent connect route plan")
	}
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_resolution", "explicit_target_wins"), "exceptional dynamic-routing escape hatch")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_resolution", "fail_closed"), "no lowered parent connect route")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "auto_wiring", "description"), "only as an inference candidate")

	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_forms", "flow_match_allow_fanout"), "explicit dynamic fan-out escape hatch")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_forms", "broadcast"), "producer-authored explicit opt-out escape hatch")

	pinAuthority := mustYAMLPath(t, root, "flow_model", "pins", "routing_authority")
	assertScalarContains(t, pinAuthority, "Parent package connect entries own common inter-flow topology")
	assertScalarContains(t, pinAuthority, "flow_model.flow_package.composition_routing")

	pinTargetResolution := mustYAMLPath(t, root, "static_analyzer", "slice_3a_pin_target_resolution")
	assertScalarValue(t, mustMappingValue(t, pinTargetResolution, "canonical_replacement"), "flow_model.flow_package.composition_routing.analyzer_verify_requirements")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "lowered_parent_connect", "rule"), "Parent connect")
}

func mustYAMLPath(t *testing.T, node *yaml.Node, keys ...string) *yaml.Node {
	t.Helper()
	current := node
	for _, key := range keys {
		current = mustMappingValue(t, current, key)
	}
	return current
}
