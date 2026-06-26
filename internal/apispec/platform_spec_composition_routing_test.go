package apispec

import (
	"os"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/platform"
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
	assertScalarContains(t, mustYAMLPath(t, composition, "split_boundaries", "runtime_route_consumption"), "#1473 closes supported EventBus publish/preflight/outbox")
	assertScalarContains(t, mustYAMLPath(t, composition, "split_boundaries", "runtime_route_consumption"), "selected-contract runfork readiness")
	slice1473 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1473")
	assertScalarValue(t, mustMappingValue(t, slice1473, "status"), "merge_bearing_runtime_behavior")
	assertScalarContains(t, mustMappingValue(t, slice1473, "canonical_code_owner"), "internal/runtime/bus.RoutePlan")
	assertScalarContains(t, mustMappingValue(t, slice1473, "rule"), "Supported EventBus publish/preflight/outbox dispatch consumes lowered ConnectRoutePlan")

	slice1475 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1475")
	assertScalarValue(t, mustMappingValue(t, slice1475, "status"), "merge_bearing_runtime_behavior")
	assertScalarContains(t, mustMappingValue(t, slice1475, "canonical_code_owner"), "ProducerRouteCommonPathFailure")
	assertScalarContains(t, mustMappingValue(t, slice1475, "rule"), "not valid common-path")
	assertScalarContains(t, mustMappingValue(t, slice1475, "rule"), "does not grandfather")
	if !sequenceContainsScalar(mustMappingValue(t, slice1475, "produces"), "producer_target_common_path_forbidden for loaded flow-scope target.flow/match common-path composition") {
		t.Fatal("implementation_slice_1475 missing producer_target_common_path_forbidden proof surface")
	}
	if !sequenceContainsScalar(mustMappingValue(t, slice1475, "produces"), "producer_broadcast_common_path_forbidden for loaded flow-scope broadcast:true common-path composition") {
		t.Fatal("implementation_slice_1475 missing producer_broadcast_common_path_forbidden proof surface")
	}

	entityContracts := mustYAMLPath(t, root, "entity_contracts")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "indexed: true")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "descriptor/index materialization")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "top-level")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "Nested entity paths")

	slice1479 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1479")
	assertScalarValue(t, mustMappingValue(t, slice1479, "status"), "merge_bearing_runtime_behavior")
	assertScalarContains(t, mustMappingValue(t, slice1479, "canonical_code_owner"), "MaterializeConnectRoutePlan")
	assertScalarContains(t, mustMappingValue(t, slice1479, "rule"), "indexed: true")
	assertScalarContains(t, mustMappingValue(t, slice1479, "rule"), "nested")
	assertScalarContains(t, mustMappingValue(t, slice1479, "rule"), "zero executable routes")
}

func TestPlatformSpecCompositionRoutingDemotesProducerTargetAuthority(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)

	crossFlow := mustYAMLPath(t, root, "engine", "cross_flow_routing")
	assertScalarValue(t, mustMappingValue(t, crossFlow, "canonical_owner"), "platform-spec.yaml#flow_model.flow_package.composition_routing")
	assertScalarValue(t, mustMappingValue(t, crossFlow, "implementation_status"), "source_authority_promoted_eventbus_dispatch_partial")
	if !sequenceContainsScalar(mustYAMLPath(t, crossFlow, "target_resolution", "precedence"), "lowered parent connect route plan") {
		t.Fatal("cross_flow_routing target precedence must start from lowered parent connect route plan")
	}
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_resolution", "explicit_target_escape_hatch"), "exceptional dynamic-routing escape hatch")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_resolution", "explicit_target_escape_hatch"), "must not replace lowered parent connect")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_resolution", "explicit_target_escape_hatch"), "illegal common-path composition routing")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_resolution", "fail_closed"), "no lowered parent connect route")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "auto_wiring", "description"), "only as an inference candidate")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "activation", "rule"), "valid lowered parent connect route")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "auto_wiring", "template_pairs"), "lowered parent connect route facts")

	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_forms", "flow_match_allow_fanout"), "explicit dynamic fan-out escape hatch")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_forms", "flow_match"), "as package-internal composition")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_forms", "broadcast"), "producer-authored explicit opt-out escape hatch")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_forms", "broadcast"), "forbidden when it functions as")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "structural_binding", "precedence_guard"), "lower precedence than lowered parent connect")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "structural_binding", "child_to_parent"), "no lowered parent connect route")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "structural_binding", "static_child_no_instance"), "without a lowered parent connect route")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "parent_route", "read_rule"), "no lowered parent connect route applies")

	pinAuthority := mustYAMLPath(t, root, "flow_model", "pins", "routing_authority")
	assertScalarContains(t, pinAuthority, "Parent package connect entries own common inter-flow topology")
	assertScalarContains(t, pinAuthority, "flow_model.flow_package.composition_routing")
	assertScalarContains(t, mustYAMLPath(t, root, "flow_model", "pins", "output_event_pins", "description"), "no lowered connect route applies")

	pinTargetResolution := mustYAMLPath(t, root, "static_analyzer", "slice_3a_pin_target_resolution")
	assertScalarValue(t, mustMappingValue(t, pinTargetResolution, "canonical_replacement"), "flow_model.flow_package.composition_routing.analyzer_verify_requirements")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "lowered_parent_connect", "rule"), "Parent connect")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "explicit_target", "rule"), "genuine dynamic escape hatch")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "explicit_broadcast", "rule"), "no loaded package receiver input")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "structural_parent_route", "rule"), "no lowered parent connect route applies")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "no lowered connect route applies")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "eligible static child delivery-entity")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "static_failure_reasons", "producer_target_common_path_forbidden"), "parent connect is the required route owner")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "static_failure_reasons", "producer_broadcast_common_path_forbidden"), "parent connect broadcast/fan-out")
}

func TestPlatformSpecCompositionRoutingCatalogSurfacesConsumeConnectAuthority(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)

	targetRequiredMissing := collectMappingValuesByKey(root, "target_required_missing")
	if len(targetRequiredMissing) == 0 {
		t.Fatal("expected at least one target_required_missing spec surface")
	}
	for _, node := range targetRequiredMissing {
		assertScalarContains(t, node, "lowered parent connect")
		assertScalarContains(t, node, "explicit target")
		assertScalarContains(t, node, "broadcast:true")
		assertScalarContains(t, node, "eligible static child delivery-entity route")
	}

	checks := mustYAMLPath(t, root, "engine", "boot_verification", "checks")
	inputPinWiring := mustSequenceMappingByScalarField(t, checks, "id", "input_pin_wiring")
	assertScalarContains(t, mustMappingValue(t, inputPinWiring, "trigger"), "parent package.yaml connect entries")
	assertScalarContains(t, mustMappingValue(t, inputPinWiring, "trigger"), "safe same-name sibling")

	pinTargetResolution := mustSequenceMappingByScalarField(t, checks, "id", "pin_target_resolution")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "lowered parent connect route")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "explicit target escape hatch")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "eligible static child delivery-entity route")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "producer target/broadcast")

	bootSteps := mustYAMLPath(t, root, "engine", "boot_sequence", "steps")
	validatePins := mustSequenceMappingByScalarField(t, bootSteps, "name", "validate_pins")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "flow_model.flow_package.composition_routing")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "lowered parent connect supplies singular event.target")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "event.target_set route facts")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "when no lowered connect route applies")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "broadcast:true is the explicit no-target opt-out")
}

func TestPlatformSpecCompositionRoutingRejectsStaleParentRouteAuthorityPhrases(t *testing.T) {
	specPath := platform.DefaultPlatformSpecFile(repoRoot(t))
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read %s: %v", specPath, err)
	}
	text := string(raw)
	for _, phrase := range []string{
		"without explicit target route to the recorded ParentRoute",
		"writes event.target when no explicit target exists",
		"must have a target mechanism or broadcast:true",
		"Pin-declared output has no target, no structural ParentRoute",
		"No explicit target, no structural ParentRoute",
		"checks only sibling flow output pins",
		"pin target mechanism",
		"explicit_target_wins",
	} {
		if strings.Contains(text, phrase) {
			t.Fatalf("platform-spec.yaml still contains stale composition-routing authority phrase %q", phrase)
		}
	}
}

func mustYAMLPath(t *testing.T, node *yaml.Node, keys ...string) *yaml.Node {
	t.Helper()
	current := node
	for _, key := range keys {
		current = mustMappingValue(t, current, key)
	}
	return current
}

func mustSequenceMappingByScalarField(t *testing.T, node *yaml.Node, field, value string) *yaml.Node {
	t.Helper()
	if node == nil || node.Kind != yaml.SequenceNode {
		t.Fatalf("node is kind %v, want sequence", nodeKind(node))
	}
	for _, item := range node.Content {
		if scalarValue(mappingValue(item, field)) == value {
			return item
		}
	}
	t.Fatalf("sequence mapping with %s=%q not found", field, value)
	return nil
}

func collectMappingValuesByKey(node *yaml.Node, key string) []*yaml.Node {
	if node == nil {
		return nil
	}
	var out []*yaml.Node
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			if node.Content[i].Value == key {
				out = append(out, node.Content[i+1])
			}
			out = append(out, collectMappingValuesByKey(node.Content[i+1], key)...)
		}
		return out
	}
	for _, child := range node.Content {
		out = append(out, collectMappingValuesByKey(child, key)...)
	}
	return out
}

func nodeKind(node *yaml.Node) yaml.Kind {
	if node == nil {
		return 0
	}
	return node.Kind
}
