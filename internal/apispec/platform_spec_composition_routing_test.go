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
	wave2 := mustMappingValue(t, composition, "w2_compiled_pin_edge_ownership")
	assertScalarContains(t, mustMappingValue(t, wave2, "rule"), "target-free public/provider input edges uniformly reject")
	assertScalarContains(t, mustMappingValue(t, wave2, "rule"), "bound producer schema already declares the receiver instance field")
	assertScalarContains(t, mustMappingValue(t, wave2, "rule"), "distinct immutable producer-declaration and receiver-acceptance schemas")
	assertScalarContains(t, mustMappingValue(t, wave2, "rule"), "schema-only imported pin may stage typed intrinsic projection metadata")
	assertScalarContains(t, mustMappingValue(t, wave2, "rule"), "reply and fan-in cannot declare resolution.from")
	assertScalarContains(t, mustMappingValue(t, wave2, "rule"), "Every W2 mapping key is byte-exact")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "Parent-authored composition routing is the canonical source authority")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "Producer emit sites MUST NOT own consumer routing")

	authored := mustMappingValue(t, composition, "authored_shapes")
	outputPin := mustMappingValue(t, authored, "output_event_pin")
	assertScalarContains(t, mustMappingValue(t, outputPin, "canonical_form"), "{event, sink}")
	assertScalarContains(t, mustMappingValue(t, outputPin, "canonical_form"), "Event-level schema and business-key declarations")
	assertScalarContains(t, mustMappingValue(t, outputPin, "canonical_form"), "production_valid:false")
	assertScalarContains(t, mustMappingValue(t, outputPin, "harness_sink"), "mutually exclusive")
	assertScalarContains(t, mustMappingValue(t, outputPin, "harness_sink"), "creates no semantic")
	assertScalarContains(t, mustMappingValue(t, outputPin, "scalar_form"), "never inferred")
	resolved := mustMappingValue(t, authored, "resolved_input_pin")
	assertScalarContains(t, mustMappingValue(t, resolved, "canonical_form"), "{event, source, resolution}")
	assertScalarContains(t, mustYAMLPath(t, resolved, "instance_fields", "identity"), "instance: <field>")
	assertScalarContains(t, mustYAMLPath(t, resolved, "instance_fields", "source"), "resolution.from")
	assertScalarContains(t, mustYAMLPath(t, resolved, "instance_fields", "mode"), "exhaustive typed value")

	connect := mustMappingValue(t, authored, "parent_connect")
	assertScalarValue(t, mustMappingValue(t, connect, "location"), "parent package.yaml connect")
	assertScalarContains(t, mustMappingValue(t, connect, "canonical_form"), "event-centric structured connections")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "event"), "source event identity")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "from"), "producer flow ID")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "to"), "receiver flow ID")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "rename"), "receiver-visible event identity")
	assertScalarContains(t, mustYAMLPath(t, connect, "retired_fields", "adapter"), "Retired on presence")
	if hasMappingKey(mustMappingValue(t, connect, "fields"), "delivery") || hasMappingKey(mustMappingValue(t, connect, "fields"), "reply") {
		t.Fatal("parent connect fields retain retired delivery/reply authoring")
	}
	assertScalarContains(t, mustYAMLPath(t, connect, "retired_fields", "delivery"), "Retired on presence")
	assertScalarContains(t, mustYAMLPath(t, connect, "retired_fields", "reply"), "resolution")
	assertScalarContains(t, mustYAMLPath(t, connect, "retired_fields", "map"), "Retired on presence")
	assertScalarContains(t, mustYAMLPath(t, connect, "retired_fields", "using"), "Retired on presence")

	ownership := mustMappingValue(t, composition, "ownership_split")
	assertScalarContains(t, mustMappingValue(t, ownership, "parent_connect"), "owns the directed inter-flow event edge")
	assertScalarContains(t, mustMappingValue(t, ownership, "receiver_input_resolution"), "cardinality")
	assertScalarContains(t, mustMappingValue(t, ownership, "output_pins"), "never receiver identity")
	assertScalarContains(t, mustMappingValue(t, ownership, "input_pins"), "typed identity source")
	assertScalarContains(t, mustMappingValue(t, ownership, "producer_emit"), "retired on presence")

	verify := mustMappingValue(t, composition, "analyzer_verify_requirements")
	for _, key := range []string{
		"producer_flow_exists",
		"producer_output_pin_exists",
		"receiver_flow_exists",
		"receiver_input_pin_exists",
		"event_rename_valid",
		"adapter_retired",
		"output_metadata_retired",
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
		"producer output pin exact event identity and immutable producer event schema resolved from event-centric flow endpoints",
		"receiver scalar template instance identity from WorkflowContractBundle.ResolveFlowTemplateInstance",
		"receiver input same-named required payload source or exact resolution.from override and resolution mode",
		"import-boundary event bindings",
		"explicit receiver-event rename",
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
	assertScalarContains(t, mustYAMLPath(t, composition, "producer_routing_retirement", "role"), "retired on presence")
	assertScalarContains(t, mustYAMLPath(t, composition, "producer_routing_retirement", "migration"), "bundle-wide preflight")
	assertScalarContains(t, mustYAMLPath(t, composition, "split_boundaries", "runtime_route_consumption"), "#1473 established EventBus publish/preflight/outbox")
	assertScalarContains(t, mustYAMLPath(t, composition, "split_boundaries", "runtime_route_consumption"), "#2114 closed selected-contract frontier/history")
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
	assertScalarContains(t, mustMappingValue(t, slice1546, "rule"), "resolution.from")
	assertScalarContains(t, mustMappingValue(t, slice1546, "rule"), "ConnectRoutePlan.InstanceKey.Mappings are removed")

	slice1475 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1475")
	assertScalarValue(t, mustMappingValue(t, slice1475, "status"), "superseded_by_complete_retirement")
	assertScalarContains(t, mustMappingValue(t, slice1475, "canonical_code_owner"), "strict emit decoding")
	assertScalarContains(t, mustMappingValue(t, slice1475, "rule"), "fails strict load")
	if !sequenceContainsScalar(mustMappingValue(t, slice1475, "produces"), "RETIRED-EMIT-ROUTING before normalization, verification, or runtime") {
		t.Fatal("implementation_slice_1475 missing complete retirement proof surface")
	}
	slice1508 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1508")
	if !sequenceContainsScalar(mustMappingValue(t, slice1508, "proof_obligations"), "the confined compiled-connect owner resolves package-aware root endpoints once, and static verification plus runtime delivery consume its typed graph projections") {
		t.Fatal("implementation_slice_1508 must name the confined compiled graph owner")
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

func TestPlatformSpecProducerRoutingMigrationCoversWholeBundle(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	behavior := mustYAMLPath(t, root, "cli_specification", "command_catalog", "migrate_producer_routing", "behavior")

	for _, want := range []string{
		"nodes.yaml",
		"schema.yaml",
		"loops.*.escape.emit",
		"stages.*.gate.outcomes.*.emit",
		"whole bundle",
		"before writing any file",
		"entire bundle byte-for-byte unchanged",
	} {
		assertScalarContains(t, behavior, want)
	}
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
	assertScalarValue(t, mustMappingValue(t, owner, "authored_source_owner"), "receiver input resolution.from, with omission deriving the same-named payload field")
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
	assertScalarContains(t, mustMappingValue(t, selectMode, "contract"), "required producer payload field whose name equals")
	assertScalarContains(t, mustMappingValue(t, selectMode, "contract"), "typed payload")
	assertScalarContains(t, mustMappingValue(t, selectOrCreate, "contract"), "scalar `instance`")
	if strings.Contains(mustMappingValue(t, slice, "authoring_shape").Value, "instance_key") {
		t.Fatal("canonical instance identity authoring shape retains retired resolution.instance_key")
	}

	proofs := mustMappingValue(t, slice, "proof_obligations")
	for _, want := range []string{
		"route-plan lowering records typed create mode, the scalar instance Field, and typed Source derived from the required same-named payload field or exact resolution.from, with no policy facts or producer output-key dependency",
		"downstream receiver routes can render the receiver-targeted identity projection derived from typed Source as payload.<instance>",
		"route-plan lowering records typed select mode, scalar instance Field, and typed payload Source derived from the required same-named payload field or exact resolution.from, with no policy facts",
		"route-plan lowering records typed select-or-create mode, scalar instance Field, and typed payload Source derived from the required same-named payload field or exact resolution.from, with no policy facts",
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

func TestPlatformSpecCompositionRoutingRetiresProducerTargetAuthority(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)

	crossFlow := mustYAMLPath(t, root, "engine", "cross_flow_routing")
	assertScalarValue(t, mustMappingValue(t, crossFlow, "canonical_owner"), "platform-spec.yaml#flow_model.flow_package.composition_routing")
	assertScalarValue(t, mustMappingValue(t, crossFlow, "implementation_status"), "typed_composition_authority_complete_for_supported_surface")
	if !sequenceContainsScalar(mustYAMLPath(t, crossFlow, "target_resolution", "precedence"), "lowered parent connect route plan") {
		t.Fatal("cross_flow_routing target precedence must start from lowered parent connect route plan")
	}
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "retired_producer_forms", "target"), "rejected on presence")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "retired_producer_forms", "target"), "receiver-owned")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "retired_producer_forms", "broadcast"), "rejected on presence")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_resolution", "fail_closed"), "canonical typed consumer")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "auto_wiring", "description"), "only as an inference candidate")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "activation", "rule"), "lowered parent connect route")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "auto_wiring", "template_pairs"), "lowered parent connect route facts")

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
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "typed_same_flow_consumer", "rule"), "typed pub/sub")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "harness_sink", "rule"), "Production admission rejects")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "harness_sink", "rule"), "no runtime recipient")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "structural_parent_route", "rule"), "no lowered parent connect route applies")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "typed same-flow consumer")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "eligible static child delivery-entity")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "agent emit_events")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "never author recipient identity")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "static_failure_reasons", "retired_emit_routing"), "present in any shape")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "static_failure_reasons", "harness_consumer_conflict"), "combined with a real")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "implementation_slice_1444", "rule"), "Agent emit_events declarations")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "implementation_slice_1444", "rule"), "MUST NOT require producer routing fields")
	implementationSlice1444 := mustYAMLPath(t, pinTargetResolution, "implementation_slice_1444")
	assertScalarContains(t, mustYAMLPath(t, implementationSlice1444, "canonical_code_owner"), "ClassifyOutputConsumer")
	runtimeConsumers := mustYAMLPath(t, implementationSlice1444, "runtime_consumers")
	if !sequenceContainsScalar(runtimeConsumers, "internal/runtime/bootverify.checkPinTargetResolution") ||
		!sequenceContainsScalar(runtimeConsumers, "internal/runtime/core/pinrouting.ResolveEnvelope") {
		t.Fatal("pin target resolution must name the shared bootverify and runtime classifier consumers")
	}

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
	assertScalarContains(t, mustYAMLPath(t, fanOut, "retired_target_field"), "retired on presence")
	if !sequenceContainsScalar(mustYAMLPath(t, fanOut, "effective_semantics", "consumers"), "engine") ||
		!sequenceContainsScalar(mustYAMLPath(t, fanOut, "effective_semantics", "consumers"), "authoring_view") {
		t.Fatal("fan_out effective semantics must name runtime and authoring consumers")
	}
	assertScalarContains(t, mustYAMLPath(t, fanOut, "platform_ceiling", "overrun"), "raise max_items or split the batch")
}

func TestPlatformSpecE1aRetirementCloseoutIsCanonicalOnly(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)

	routingDerivation := mustYAMLPath(t, root, "contract_formats", "event_schema", "routing_derivation")
	interFlow := mustMappingValue(t, routingDerivation, "inter_flow_connect")
	assertScalarContains(t, interFlow, "Qualified exact subscriptions cannot cross a flow boundary")
	assertScalarContains(t, interFlow, "bind.observe")

	resolutionRules := mustYAMLPath(t, root, "engine", "event_identity", "resolution_rules")
	assertScalarContains(t, mustMappingValue(t, resolutionRules, "contracts_always_local"), "already-normalized exact identity only when it resolves to that same flow")
	assertScalarContains(t, mustMappingValue(t, resolutionRules, "contracts_always_local"), "Exact subscriptions never cross a flow boundary")
	assertScalarContains(t, mustMappingValue(t, resolutionRules, "contracts_always_local"), "bind.observe")
	assertScalarContains(t, mustMappingValue(t, resolutionRules, "handler_lookup_localizes"), "governed import-boundary wildcard observation")

	crossFlow := mustYAMLPath(t, root, "engine", "cross_flow_routing")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "auto_wiring", "ambiguity"), "explicit event-centric parent connect")
	subscriptionBoundary := mustMappingValue(t, crossFlow, "subscription_boundary")
	assertScalarContains(t, mustMappingValue(t, subscriptionBoundary, "exact"), "hard invalidity")
	assertScalarContains(t, mustMappingValue(t, subscriptionBoundary, "same_scope_agent_identity"), "same-scope route carrier")
	assertScalarContains(t, mustMappingValue(t, subscriptionBoundary, "timer_event_references"), "exact receiver-local names")
	assertScalarContains(t, mustMappingValue(t, subscriptionBoundary, "timer_event_references"), "wildcard timer event references are hard invalidities")
	assertScalarContains(t, mustMappingValue(t, subscriptionBoundary, "required_agent_fulfillment"), "not executable subscribers")
	assertScalarContains(t, mustMappingValue(t, subscriptionBoundary, "required_agent_fulfillment"), "or runtime route authority")
	assertScalarContains(t, mustMappingValue(t, subscriptionBoundary, "inter_flow_delivery"), "parent connect")
	assertScalarContains(t, mustMappingValue(t, subscriptionBoundary, "wildcard_observation"), "bind.observe")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "contract_authoring_rules", "subscribes_to"), "already-normalized same-scope identity")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "contract_authoring_rules", "subscribes_to"), "Cross-boundary slash-qualified and full-URI exact values are invalid")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "contract_authoring_rules", "timer_event_references"), "Always local exact names")

	referenceModel := mustYAMLPath(t, root, "engine", "uri_addressing")
	assertScalarContains(t, mustMappingValue(t, referenceModel, "resolution"), "flow-owned agent admission may retain an already-normalized same-scope identity")
	assertScalarContains(t, mustYAMLPath(t, referenceModel, "full_uri", "when"), "Full URIs are not exact subscription authority")

	contractMerger := mustYAMLPath(t, root, "engine", "contract_merger")
	assertScalarContains(t, mustMappingValue(t, contractMerger, "subscription_resolution"), "fail closed rather than resolving globally")
	assertScalarContains(t, mustMappingValue(t, contractMerger, "subscription_resolution"), "wildcard_subscription_scope")

	bootSteps := mustYAMLPath(t, root, "engine", "boot_sequence", "steps")
	resolveSubscriptions := mustSequenceMappingByScalarField(t, bootSteps, "name", "resolve_subscriptions")
	assertScalarContains(t, mustMappingValue(t, resolveSubscriptions, "action"), "already-normalized same-scope identity")
	assertScalarContains(t, mustMappingValue(t, resolveSubscriptions, "action"), "full-URI exact values that cross a flow boundary fail closed")

	eventCatalog := mustYAMLPath(t, root, "static_analyzer", "slice_5_dead_declared_event_schema_surface", "active_role_carriers")
	assertScalarContains(t, mustYAMLPath(t, eventCatalog, "connected_cross_flow_usage", "rule"), "typed compiled connect edge")
	assertScalarContains(t, mustYAMLPath(t, eventCatalog, "connected_cross_flow_usage", "rule"), "Raw qualified exact subscription text is not a liveness proof")

	lowered := mustYAMLPath(t, root, "contract_formats", "event_schema", "routing_derivation", "route_plan_authority", "lowered_connect_route_plan_consumption")
	assertScalarContains(t, mustYAMLPath(t, lowered, "unsupported_or_split", "selected_contract_runfork_readiness"), "Closed by #2114")
	assertScalarContains(t, mustYAMLPath(t, lowered, "unsupported_or_split", "parent_composition_routing_closure"), "Closed by #1466")

	composition := mustYAMLPath(t, root, "flow_model", "flow_package", "composition_routing")
	splitBoundaries := mustMappingValue(t, composition, "split_boundaries")
	assertScalarContains(t, mustMappingValue(t, splitBoundaries, "runtime_route_consumption"), "#2114 closed selected-contract")
	assertScalarContains(t, mustMappingValue(t, splitBoundaries, "parser_model_support"), "#2086 closes strict parser/model retirement")

	var scalarValues []string
	var collect func(*yaml.Node)
	collect = func(node *yaml.Node) {
		if node == nil {
			return
		}
		if node.Kind == yaml.ScalarNode {
			scalarValues = append(scalarValues, node.Value)
			return
		}
		for _, child := range node.Content {
			collect(child)
		}
	}
	collect(root)
	surface := strings.Join(scalarValues, "\n")
	for _, stale := range []string{
		"Explicit scoped subscriptions",
		"explicit scoped subscription escape hatches",
		"scoped subscription escape hatch",
		"Flow-prefixed only as escape hatch",
		"source_authority_promoted_eventbus_dispatch_partial",
		"#1466 remains open",
		"not closed by #1473",
		"Parser/model support beyond source-authority fixtures is follow-up",
		"Exact declarations never use flow prefixes",
		"Exact subscriptions always use local names",
		"flow-prefixed exact values are invalid",
		"Full URI subscriptions resolve globally",
		"Paths with / resolve\n        as absolute from root",
		"cross_flow_qualified_usage",
	} {
		if strings.Contains(surface, stale) {
			t.Fatalf("platform spec retains stale E1a authority/status wording %q", stale)
		}
	}
}

func TestPlatformSpecExecutableNodeIdentityIsExactAndCanonicalOnly(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	rule := mustYAMLPath(t, root, "contract_formats", "event_schema", "routing_derivation", "route_plan_authority", "executable_node_identity")
	for _, fragment := range []string{
		"exact package_key",
		"owning flow_id",
		"local node_id",
		"explicitly empty flow_id",
		"Local node_id",
		"strict canonical codecs",
		"no fallback, migration, or dual string authority",
	} {
		assertScalarContains(t, rule, fragment)
	}
}

func TestPlatformSpecCompositionRoutingCatalogSurfacesConsumeConnectAuthority(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)

	targetRequiredMissing := collectMappingValuesByKey(root, "target_required_missing")
	if len(targetRequiredMissing) == 0 {
		t.Fatal("expected at least one target_required_missing spec surface")
	}
	for _, node := range targetRequiredMissing {
		if !strings.Contains(node.Value, "consumer") {
			t.Fatalf("target_required_missing does not name canonical consumer evidence: %q", node.Value)
		}
		if strings.Contains(node.Value, "explicit target") || strings.Contains(node.Value, "broadcast:true") {
			t.Fatalf("target_required_missing retains retired producer routing: %q", node.Value)
		}
	}

	checks := mustYAMLPath(t, root, "engine", "boot_verification", "checks")
	inputPinWiring := mustSequenceMappingByScalarField(t, checks, "id", "input_pin_wiring")
	assertScalarContains(t, mustMappingValue(t, inputPinWiring, "trigger"), "ResolveFlowInputProducer")
	assertScalarContains(t, mustMappingValue(t, inputPinWiring, "trigger"), "parent connect")
	assertScalarContains(t, mustMappingValue(t, inputPinWiring, "trigger"), "unique same-name sibling output-pin inference")

	pinTargetResolution := mustSequenceMappingByScalarField(t, checks, "id", "pin_target_resolution")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "agent emit_events")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "lowered parent connect route")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "typed same-flow consumer")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "eligible static child delivery-entity route")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "fail strict loading on presence")

	outputPinKeyCarries := mustSequenceMappingByScalarField(t, checks, "id", "output_pin_key_carries_validation")
	assertScalarContains(t, mustMappingValue(t, outputPinKeyCarries, "trigger"), "key or carries is present")
	assertScalarContains(t, mustMappingValue(t, outputPinKeyCarries, "trigger"), "event schema/business key")

	bootSteps := mustYAMLPath(t, root, "engine", "boot_sequence", "steps")
	validatePins := mustSequenceMappingByScalarField(t, bootSteps, "name", "validate_pins")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "canonical input-source resolver")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "typed same-flow consumer")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "sink:harness")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "production admission rejects")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "emit.target and emit.broadcast are rejected on presence")
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
