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

	assertScalarValue(t, mustMappingValue(t, composition, "status"), "scalar_receiver_identity_authoritative")
	assertScalarValue(t, mustMappingValue(t, composition, "promoted_by"), "#1467")
	assertScalarValue(t, mustMappingValue(t, composition, "parent_decision"), "#1466")
	assertScalarValue(t, mustMappingValue(t, composition, "owner"), "platform-spec.yaml#flow_model.flow_package.composition_routing")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "Parent-authored composition routing is the canonical source authority")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "Producer emit sites MUST NOT own consumer routing")

	authored := mustMappingValue(t, composition, "authored_shapes")
	outputPin := mustMappingValue(t, authored, "output_event_pin")
	assertScalarContains(t, mustMappingValue(t, outputPin, "canonical_form"), "{name, event, key, carries}")
	assertScalarContains(t, mustMappingValue(t, outputPin, "canonical_form"), "MUST include `key`")
	assertScalarContains(t, mustMappingValue(t, outputPin, "scalar_form"), "never inferred")
	resolved := mustMappingValue(t, authored, "resolved_input_pin")
	assertScalarContains(t, mustMappingValue(t, resolved, "canonical_form"), "{name, event, source, resolution, carries}")
	assertScalarContains(t, mustYAMLPath(t, resolved, "instance_fields", "identity"), "instance: <field>")
	assertScalarContains(t, mustYAMLPath(t, resolved, "instance_fields", "source"), "typed source")
	assertScalarContains(t, mustYAMLPath(t, resolved, "instance_fields", "mode"), "exhaustive typed value")

	connect := mustMappingValue(t, authored, "parent_connect")
	assertScalarValue(t, mustMappingValue(t, connect, "location"), "parent package.yaml connect")
	assertScalarContains(t, mustMappingValue(t, connect, "canonical_form"), "`connect` is a list")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "from"), "producer")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "to"), "receiver")
	if hasMappingKey(mustMappingValue(t, connect, "fields"), "delivery") || hasMappingKey(mustMappingValue(t, connect, "fields"), "reply") {
		t.Fatal("parent connect fields retain retired delivery/reply authoring")
	}
	assertScalarContains(t, mustYAMLPath(t, connect, "retired_fields", "delivery"), "Retired on presence")
	assertScalarContains(t, mustYAMLPath(t, connect, "retired_fields", "reply"), "resolution")
	assertScalarContains(t, mustYAMLPath(t, connect, "retired_fields", "map"), "Retired on presence")
	assertScalarContains(t, mustYAMLPath(t, connect, "retired_fields", "using"), "Retired on presence")

	ownership := mustMappingValue(t, composition, "ownership_split")
	assertScalarContains(t, mustMappingValue(t, ownership, "parent_connect"), "owns only the directed inter-flow edge")
	assertScalarContains(t, mustMappingValue(t, ownership, "receiver_input_resolution"), "cardinality")
	assertScalarContains(t, mustMappingValue(t, ownership, "output_pins"), "never receiver identity")
	assertScalarContains(t, mustMappingValue(t, ownership, "input_pins"), "typed identity source")
	assertScalarContains(t, mustMappingValue(t, ownership, "producer_emit_target"), "exceptional dynamic routing")

	verify := mustMappingValue(t, composition, "analyzer_verify_requirements")
	for _, key := range []string{
		"producer_flow_exists",
		"producer_output_pin_exists",
		"receiver_flow_exists",
		"receiver_input_pin_exists",
		"event_alias_or_adapter_valid",
		"output_carries_instance_field",
		"receiver_route_key_present",
		"key_types_compatible",
		"receiver_resolution_valid",
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
		"producer output pin event identity and verified interface evidence, including explicit package-root `.pin_name` output endpoints",
		"receiver scalar template instance identity from WorkflowContractBundle.ResolveFlowTemplateInstance",
		"receiver input same-named typed carry source and resolution mode",
		"import-boundary pin alias bindings",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, lowering, "consumes"), want) {
			t.Fatalf("route_plan_lowering consumes missing %q", want)
		}
	}
	for _, want := range []string{
		"concrete target routes derived from receiver-owned scalar instance/source/mode facts",
		"typed reply resolution derived from receiver input resolution and paired connect edges",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, lowering, "produces"), want) {
			t.Fatalf("route_plan_lowering produces missing %q", want)
		}
	}

	assertScalarContains(t, mustYAMLPath(t, composition, "pin_alias_interface_adaptation", "owner_consumed"), "pin_alias_interface_adaptation")
	assertScalarContains(t, mustYAMLPath(t, composition, "emit_target_escape_hatch", "role"), "not a compatibility path")
	assertScalarContains(t, mustYAMLPath(t, composition, "split_boundaries", "runtime_route_consumption"), "#1473 closes supported EventBus publish/preflight/outbox")
	assertScalarContains(t, mustYAMLPath(t, composition, "split_boundaries", "runtime_route_consumption"), "selected-contract runfork readiness")
	slice1473 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1473")
	assertScalarValue(t, mustMappingValue(t, slice1473, "status"), "merge_bearing_runtime_behavior")
	assertScalarContains(t, mustMappingValue(t, slice1473, "canonical_code_owner"), "internal/runtime/bus.RoutePlan")
	assertScalarContains(t, mustMappingValue(t, slice1473, "rule"), "Supported EventBus publish/preflight/outbox dispatch consumes lowered ConnectRoutePlan")

	slice1545 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1545")
	assertScalarValue(t, mustMappingValue(t, slice1545, "status"), "merge_bearing_runtime_behavior")
	assertScalarContains(t, mustMappingValue(t, slice1545, "canonical_code_owner"), "ConnectRoutePlan.ResolutionKind")
	assertScalarContains(t, mustMappingValue(t, slice1545, "canonical_code_owner"), "ConnectRoutePlan.InstanceKey")
	assertScalarContains(t, mustMappingValue(t, slice1545, "rule"), "ResolutionKind: instance_key")
	assertScalarContains(t, mustMappingValue(t, slice1545, "rule"), "WorkflowContractBundle.ResolveFlowTemplateInstance")
	assertScalarContains(t, mustMappingValue(t, slice1545, "rule"), "opaque identity field")
	if !sequenceContainsScalar(mustMappingValue(t, slice1545, "non_authoritative_for_this_slice"), "receiver input-pin address") {
		t.Fatal("implementation_slice_1545 must reject receiver input-pin address authority")
	}

	retirement := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1827_connect_delivery_reply_retirement")
	assertScalarValue(t, mustMappingValue(t, retirement, "status"), "merge_bearing_aggressive_retirement")
	assertScalarContains(t, mustMappingValue(t, retirement, "rule"), "ConnectRoutePlan expose no delivery or raw reply compatibility fields")
	assertScalarContains(t, mustYAMLPath(t, retirement, "migration", "delivery_one"), "swarm migrate-connect-delivery-one")
	assertScalarContains(t, mustYAMLPath(t, retirement, "migration", "delivery_reply_and_reply_map"), "resolution.mode: reply")

	slice1546 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1546")
	assertScalarValue(t, mustMappingValue(t, slice1546, "status"), "retired_by_2087")
	assertScalarContains(t, mustMappingValue(t, slice1546, "canonical_code_owner"), "ConnectRoutePlan.InstanceKey.Source")
	assertScalarContains(t, mustMappingValue(t, slice1546, "rule"), "receiver input carry")
	assertScalarContains(t, mustMappingValue(t, slice1546, "rule"), "ConnectRoutePlan.InstanceKey.Mappings are removed")

	slice1475 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1475")
	assertScalarValue(t, mustMappingValue(t, slice1475, "status"), "merge_bearing_runtime_behavior")
	assertScalarContains(t, mustMappingValue(t, slice1475, "canonical_code_owner"), "ProducerRouteCommonPathFailure")
	assertScalarContains(t, mustMappingValue(t, slice1475, "rule"), "not valid common-path")
	assertScalarContains(t, mustMappingValue(t, slice1475, "rule"), "does not grandfather")
	assertScalarContains(t, mustMappingValue(t, slice1475, "rule"), "alias/adapter connect")
	if !sequenceContainsScalar(mustMappingValue(t, slice1475, "consumes"), "loaded package receiver input pins and parent connect graph edges, including alias/adapter connects") {
		t.Fatal("implementation_slice_1475 missing connected adapter proof surface")
	}
	if !sequenceContainsScalar(mustMappingValue(t, slice1475, "produces"), "producer_target_common_path_forbidden for loaded flow-scope target.flow/match common-path composition") {
		t.Fatal("implementation_slice_1475 missing producer_target_common_path_forbidden proof surface")
	}
	if !sequenceContainsScalar(mustMappingValue(t, slice1475, "produces"), "producer_broadcast_common_path_forbidden for loaded flow-scope broadcast:true common-path composition") {
		t.Fatal("implementation_slice_1475 missing producer_broadcast_common_path_forbidden proof surface")
	}
	slice1508 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1508")
	if !sequenceContainsScalar(mustMappingValue(t, slice1508, "proof_obligations"), "semanticview exposes package-aware root connect facts through ResolvedCompositionConnectsFrom, and runtime delivery consumes the same endpoints through lowered ConnectRoutePlan authority") {
		t.Fatal("implementation_slice_1508 must name the package-aware resolved relation and lowered route-plan owner")
	}

	entityContracts := mustYAMLPath(t, root, "entity_contracts")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "indexed: true")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "not composition-routing selectors")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "scalar instance field")

	slice1479 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1479")
	assertScalarValue(t, mustMappingValue(t, slice1479, "status"), "retired_by_2087")
	assertScalarContains(t, mustMappingValue(t, slice1479, "canonical_code_owner"), "ConnectRoutePlan.InstanceKey")
	assertScalarContains(t, mustMappingValue(t, slice1479, "rule"), "indexed: true")
	assertScalarContains(t, mustMappingValue(t, slice1479, "rule"), "invalid")
}

func TestPlatformSpecReplyRuntimeStatusDoesNotContradictHistoricalSlice(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	reply := mustYAMLPath(t, root, "engine", "cross_flow_routing", "reply_resolution")
	slice := mustYAMLPath(t, root, "flow_model", "flow_package", "composition_routing", "route_plan_lowering", "implementation_slice_1835")
	sliceReply := mustYAMLPath(t, slice, "modes", "reply")

	assertScalarValue(t, mustMappingValue(t, reply, "status"), "runnable_v1")
	assertScalarValue(t, mustMappingValue(t, sliceReply, "status"), "runnable_v1")
	assertScalarContains(t, mustMappingValue(t, slice, "status"), "reply_modes_runnable")
	assertScalarContains(t, mustMappingValue(t, slice, "rule"), "Reply resolution is also")
	assertScalarContains(t, mustMappingValue(t, slice, "canonical_code_owner"), "ConnectRoutePlan.InstanceKey/FanIn/ReplyResolution")
	assertScalarContains(t, mustMappingValue(t, slice, "canonical_code_owner"), "internal/store.ReplyContextStore")
	assertScalarContains(t, mustMappingValue(t, sliceReply, "contract"), "engine.cross_flow_routing.reply_resolution")
}

func TestPlatformSpecInstanceIdentityAuthoringSourceAuthority(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	slice := mustYAMLPath(t, root, "flow_model", "flow_package", "composition_routing", "route_plan_lowering", "implementation_slice_1835")
	owner := mustMappingValue(t, slice, "instance_identity_owner_2021")
	assertScalarValue(t, mustMappingValue(t, owner, "status"), "scalar_typed_owner_finalized_by_2087")
	assertScalarValue(t, mustMappingValue(t, owner, "authored_identity_owner"), "flow instance: <field>")
	assertScalarValue(t, mustMappingValue(t, owner, "authored_source_owner"), "receiver input carries.<instance>.from")
	assertScalarValue(t, mustMappingValue(t, owner, "authored_behavior_owner"), "receiver input resolution.mode")
	assertScalarContains(t, mustMappingValue(t, owner, "effective_owner"), "opaque validated Field")
	assertScalarContains(t, mustMappingValue(t, owner, "effective_owner"), "No second representation")
	assertScalarContains(t, mustMappingValue(t, owner, "retirement"), "hard-invalid")
	assertScalarContains(t, mustMappingValue(t, owner, "retirement"), "No warning, codemod")

	create := mustYAMLPath(t, slice, "modes", "create")
	assertScalarContains(t, mustMappingValue(t, create, "contract"), "generated.uuid")
	assertScalarContains(t, mustYAMLPath(t, create, "sources", "generated.uuid"), "admitted event id")
	assertScalarContains(t, mustYAMLPath(t, create, "sources", "generated.uuid"), "fresh event identity")
	assertScalarContains(t, mustYAMLPath(t, create, "sources", "payload.field"), "immutable journal payload")
	assertScalarContains(t, mustMappingValue(t, create, "delivery_projection"), "persisted projection")
	assertScalarContains(t, mustMappingValue(t, create, "contract"), "hard conflict")

	selectMode := mustYAMLPath(t, slice, "modes", "select")
	selectOrCreate := mustYAMLPath(t, slice, "modes", "select-or-create")
	assertScalarContains(t, mustMappingValue(t, selectMode, "contract"), "carry whose name equals")
	assertScalarContains(t, mustMappingValue(t, selectMode, "contract"), "typed payload")
	assertScalarContains(t, mustMappingValue(t, selectOrCreate, "contract"), "scalar `instance`")
	if strings.Contains(mustMappingValue(t, slice, "authoring_shape").Value, "instance_key") {
		t.Fatal("canonical instance identity authoring shape retains retired resolution.instance_key")
	}

	proofs := mustMappingValue(t, slice, "proof_obligations")
	for _, want := range []string{
		"route-plan lowering records typed create mode, the scalar instance Field, and typed Source derived from the same-named carry, with no policy facts or producer output-key dependency",
		"downstream receiver routes can render the receiver-targeted identity projection derived from typed Source as payload.<instance>",
		"route-plan lowering records typed select mode, scalar instance Field, and typed payload Source derived from the same-named carry, with no policy facts",
		"route-plan lowering records typed select-or-create mode, scalar instance Field, and typed payload Source derived from the same-named carry, with no policy facts",
	} {
		if !sequenceContainsScalar(proofs, want) {
			t.Fatalf("instance identity proof obligations missing canonical typed-source proof %q", want)
		}
	}
	var activeScalars []string
	var collectScalars func(*yaml.Node)
	collectScalars = func(node *yaml.Node) {
		if node == nil {
			return
		}
		if node.Kind == yaml.ScalarNode {
			activeScalars = append(activeScalars, node.Value)
			return
		}
		for _, child := range node.Content {
			collectScalars(child)
		}
	}
	collectScalars(slice)
	activeSurface := strings.Join(activeScalars, "\n")
	for _, retired := range []string{"mint kind", "carried `as` field", "carried minted instance key", "declared carried key mapping", "single field mapping"} {
		if strings.Contains(activeSurface, retired) {
			t.Fatalf("active instance identity slice retains retired authority %q", retired)
		}
	}
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

	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_forms", "flow_match"), "more than one match fail closed")
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
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "explicit_target", "rule"), "more than one is ambiguous")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "retired_producer_fanout", "rule"), "examples/routing/notify-all-children")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "retired_producer_fanout", "rule"), "issues/1934")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "explicit_broadcast", "rule"), "no loaded package receiver input")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "structural_parent_route", "rule"), "no lowered parent connect route applies")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "no lowered connect route applies")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "eligible static child delivery-entity")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "agent emit_events")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "do not require producer")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "static_failure_reasons", "producer_target_common_path_forbidden"), "parent connect is the required route owner")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "static_failure_reasons", "producer_broadcast_common_path_forbidden"), "parent connect broadcast/fan-out")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "implementation_slice_1444", "rule"), "Agent emit_events declarations")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "implementation_slice_1444", "rule"), "MUST NOT require producer target")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "implementation_slice_1444", "canonical_code_owner"), "pinRoutingAgentEmitSites")

	fanOut := mustYAMLPath(t, root, "handler_specification", "handler_fields", "fan_out")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "sub_fields", "items_from"), "missing event catalog entry")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "sub_fields", "items_from"), "hard load error")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "sub_fields", "identity"), "statically scalar list item")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "sub_fields", "identity"), "require an explicit identity")
	assertScalarValue(t, mustYAMLPath(t, fanOut, "effective_semantics", "canonical_owner"), "contracts.WorkflowContractBundle.ResolveFanOutEffectiveSemantics")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "collection_iteration"), "declared list order")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "collection_iteration"), "never sorts or deduplicates")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "collection_iteration"), "exact microsecond precision")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "collection_iteration"), "event_id is never an order owner")
	if !sequenceContainsScalar(mustYAMLPath(t, fanOut, "effective_semantics", "consumers"), "engine") ||
		!sequenceContainsScalar(mustYAMLPath(t, fanOut, "effective_semantics", "consumers"), "authoring_view") {
		t.Fatal("fan_out effective semantics must name runtime and authoring consumers")
	}
	assertScalarContains(t, mustYAMLPath(t, fanOut, "platform_ceiling", "overrun"), "raise max_items or split the batch")
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
	assertScalarContains(t, mustMappingValue(t, inputPinWiring, "trigger"), "ResolveFlowInputProducer")
	assertScalarContains(t, mustMappingValue(t, inputPinWiring, "trigger"), "parent connect")
	assertScalarContains(t, mustMappingValue(t, inputPinWiring, "trigger"), "unique same-name sibling output-pin inference")

	pinTargetResolution := mustSequenceMappingByScalarField(t, checks, "id", "pin_target_resolution")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "agent emit_events")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "lowered parent connect route")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "explicit target escape hatch")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "eligible static child delivery-entity route")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "producer target/broadcast")

	outputPinKeyCarries := mustSequenceMappingByScalarField(t, checks, "id", "output_pin_key_carries_validation")
	assertScalarContains(t, mustMappingValue(t, outputPinKeyCarries, "trigger"), "missing key/carries evidence")
	assertScalarContains(t, mustMappingValue(t, outputPinKeyCarries, "trigger"), "Agent emit_events")
	assertScalarContains(t, mustMappingValue(t, outputPinKeyCarries, "trigger"), "auto_emit_on_create")
	assertScalarContains(t, mustMappingValue(t, outputPinKeyCarries, "trigger"), "workflow timers")

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
