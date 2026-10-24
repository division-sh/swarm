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
	composition := mustYAMLPath(t, root, "flow_model", "composition_routing")

	assertScalarValue(t, mustMappingValue(t, composition, "status"), "scalar_receiver_identity_authoritative")
	assertScalarValue(t, mustMappingValue(t, composition, "promoted_by"), "#1467")
	assertScalarValue(t, mustMappingValue(t, composition, "parent_decision"), "#1466")
	assertScalarValue(t, mustMappingValue(t, composition, "owner"), "platform-spec.yaml#flow_model.composition_routing")
	wave2 := mustMappingValue(t, composition, "w2_compiled_pin_edge_ownership")
	assertScalarContains(t, mustMappingValue(t, wave2, "rule"), "CompiledConnectionInput owns immutable ordinary source/receiver schema projection only")
	assertScalarContains(t, mustMappingValue(t, wave2, "rule"), "The canonical constructor owns same-name supplied state")
	assertScalarContains(t, mustMappingValue(t, wave2, "rule"), "Compiled connect owns exact child boundary pins or parent-local event schemas")
	assertScalarContains(t, mustMappingValue(t, wave2, "rule"), "without authored-map fallback or synthetic private input edges")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "LCA-owned schema.yaml connect is the sole authored cross-flow edge and ordinary resolution/source owner")
	assertScalarContains(t, mustMappingValue(t, composition, "rule"), "without producer routing")

	authored := mustMappingValue(t, composition, "authored_shapes")
	outputPin := mustMappingValue(t, authored, "output_event_pin")
	assertScalarContains(t, mustMappingValue(t, outputPin, "canonical_form"), "names-only sequence")
	assertScalarContains(t, mustMappingValue(t, outputPin, "canonical_form"), "Event-level schema and business-key declarations")
	if hasMappingKey(outputPin, "harness_sink") {
		t.Fatal("output pins retain harness authority")
	}
	assertScalarContains(t, mustMappingValue(t, outputPin, "scalar_form"), "never inferred")
	resolved := mustMappingValue(t, authored, "resolved_input_pin")
	assertScalarContains(t, mustMappingValue(t, resolved, "canonical_form"), "names-only sequence of scalar exact event identities")
	assertScalarContains(t, mustMappingValue(t, resolved, "canonical_form"), "response connect row")
	if hasMappingKey(resolved, "receiver_initialization") {
		t.Fatal("input pins retain an independent receiver configuration owner")
	}
	construction := mustMappingValue(t, resolved, "state_construction")
	assertScalarContains(t, mustMappingValue(t, construction, "ownership"), "same-name projection from the admitted creating message")
	assertScalarContains(t, mustMappingValue(t, construction, "ownership"), "Internal literal state cannot be overridden")
	assertScalarContains(t, mustMappingValue(t, construction, "lifecycle"), "they never reinitialize state from another message")
	assertScalarContains(t, mustMappingValue(t, construction, "lifecycle"), "pending occurrence remains that original payload")
	assertScalarContains(t, mustMappingValue(t, construction, "persistence"), "Business values live only in declared entity fields")
	assertScalarContains(t, mustMappingValue(t, construction, "history"), "Missing required state, malformed headers or contradictory receipts refuse")
	assertScalarContains(t, mustYAMLPath(t, resolved, "instance_fields", "identity"), "instance: <field>")
	assertScalarContains(t, mustYAMLPath(t, resolved, "instance_fields", "source"), "connect.key_from")
	assertScalarContains(t, mustYAMLPath(t, resolved, "instance_fields", "mode"), "exhaustive typed ordinary value")

	connect := mustMappingValue(t, authored, "parent_connect")

	assertScalarContains(t, mustMappingValue(t, connect, "canonical_form"), "event-centric structured connections")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "event"), "source event identity")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "from"), "producer flow ID")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "to"), "receiver flow ID")
	assertScalarContains(t, mustYAMLPath(t, connect, "fields", "rename"), "receiver-visible event identity")
	assertScalarContains(t, mustMappingValue(t, connect, "admission_rule"), "Unknown fields fail strict")
	if hasMappingKey(mustMappingValue(t, connect, "fields"), "delivery") || hasMappingKey(mustMappingValue(t, connect, "fields"), "reply") {
		t.Fatal("parent connect fields retain retired delivery/reply authoring")
	}
	assertScalarContains(t, mustMappingValue(t, connect, "admission_rule"), "reply pairing belongs to the response edge")
	assertScalarContains(t, mustMappingValue(t, connect, "admission_rule"), "connect.key_from")
	if hasMappingKey(connect, "retired_fields") {
		t.Fatal("connect retains a removed-spelling table")
	}

	ownership := mustMappingValue(t, composition, "ownership_split")
	assertScalarContains(t, mustMappingValue(t, ownership, "parent_connect"), "owns the directed inter-flow event edge")
	assertScalarContains(t, mustMappingValue(t, ownership, "receiver_input_resolution"), "connection-owned")
	assertScalarContains(t, mustMappingValue(t, ownership, "output_pins"), "never receiver identity")
	assertScalarContains(t, mustMappingValue(t, ownership, "input_pins"), "receiver interface identity only")
	assertScalarContains(t, mustMappingValue(t, ownership, "input_pins"), "Constructor-supplied state is message-owned, not a pin passenger")
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
	assertScalarValue(t, mustMappingValue(t, lowering, "owner"), "platform-spec.yaml#flow_model.composition_routing.route_plan_lowering")
	for _, want := range []string{
		"admitted LCA-owned schema.yaml connect entries",
		"producer output pin exact event identity and immutable producer event schema resolved from event-centric flow endpoints",
		"receiver scalar template instance identity from WorkflowContractBundle.ResolveFlowTemplateInstance",
		"connection-owned required payload source or exact key_from override and ordinary resolution",
		"finite filesystem FlowNode ownership and exact child-flow endpoints",
		"explicit receiver-event rename",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, lowering, "consumes"), want) {
			t.Fatalf("route_plan_lowering consumes missing %q", want)
		}
	}
	for _, want := range []string{
		"concrete target routes derived from receiver-owned scalar identity and connection-owned source/resolution facts",
		"typed reply resolution derived from response-edge replies_to/correlation_key and paired connect edges",
	} {
		if !sequenceContainsScalar(mustMappingValue(t, lowering, "produces"), want) {
			t.Fatalf("route_plan_lowering produces missing %q", want)
		}
	}

	if hasMappingKey(composition, "pin_alias_interface_adaptation") {
		t.Fatal("composition_routing retains retired pin alias interface adaptation")
	}
	assertScalarContains(t, mustYAMLPath(t, composition, "producer_routing_retirement", "role"), "retired on presence")
	assertScalarContains(t, mustYAMLPath(t, composition, "producer_routing_retirement", "migration"), "fails strict admission")
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
	assertScalarContains(t, mustYAMLPath(t, retirement, "admission", "rule"), "no codemod or compatibility reader exists")
	if !sequenceContainsScalar(mustMappingValue(t, retirement, "preserved"), "connection resolution create, select and select-or-create; response-edge paired reply facts") {
		t.Fatal("connect retirement must preserve connection-owned reply semantics")
	}

	slice1546 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1546")
	assertScalarValue(t, mustMappingValue(t, slice1546, "status"), "retired_by_2087")
	assertScalarContains(t, mustMappingValue(t, slice1546, "canonical_code_owner"), "ConnectRoutePlan.InstanceKey.Source")
	assertScalarContains(t, mustMappingValue(t, slice1546, "rule"), "key_from")
	assertScalarContains(t, mustMappingValue(t, slice1546, "rule"), "ConnectRoutePlan.InstanceKey.Mappings are removed")

	slice1475 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1475")
	assertScalarValue(t, mustMappingValue(t, slice1475, "status"), "superseded_by_complete_retirement")
	assertScalarContains(t, mustMappingValue(t, slice1475, "canonical_code_owner"), "strict emit decoding")
	assertScalarContains(t, mustMappingValue(t, slice1475, "rule"), "fails strict load")
	if !sequenceContainsScalar(mustMappingValue(t, slice1475, "produces"), "unknown emit field rejection before normalization, verification, or runtime") {
		t.Fatal("implementation_slice_1475 missing complete retirement proof surface")
	}
	slice1508 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1508")
	if !sequenceContainsScalar(mustMappingValue(t, slice1508, "proof_obligations"), "the confined compiled-connect owner resolves flow-path-aware root endpoints once, and static verification plus runtime delivery consume its typed graph projections") {
		t.Fatal("implementation_slice_1508 must name the confined compiled graph owner")
	}

	entityContracts := mustYAMLPath(t, root, "entity_contracts")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "indexed: true")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "not composition-routing selectors")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "native immutable instance key")
	assertScalarContains(t, mustYAMLPath(t, entityContracts, "routing_indexes", "rule"), "exact declaration/run/structural parent")

	slice1479 := mustYAMLPath(t, composition, "route_plan_lowering", "implementation_slice_1479")
	assertScalarValue(t, mustMappingValue(t, slice1479, "status"), "retired_by_2087")
	assertScalarContains(t, mustMappingValue(t, slice1479, "canonical_code_owner"), "ConnectRoutePlan.InstanceKey")
	assertScalarContains(t, mustMappingValue(t, slice1479, "rule"), "indexed: true")
	assertScalarContains(t, mustMappingValue(t, slice1479, "rule"), "invalid")
}

func TestPlatformSpecProducerRoutingRetirementHasNoMigrationSurface(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	retirement := mustYAMLPath(t, root, "flow_model", "composition_routing", "producer_routing_retirement")
	assertScalarContains(t, mustMappingValue(t, retirement, "role"), "retired on presence")
	assertScalarContains(t, mustMappingValue(t, retirement, "migration"), "fails strict admission")

	commandCatalog := mustYAMLPath(t, root, "cli_specification", "command_catalog")
	for _, retired := range []string{"migrate_producer_routing", "migrate_connect_delivery_one"} {
		if hasMappingKey(commandCatalog, retired) {
			t.Fatalf("retired migration command %q remains in the command catalog", retired)
		}
	}
}

func TestPlatformSpecReplyRuntimeStatusDoesNotContradictHistoricalSlice(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	reply := mustYAMLPath(t, root, "engine", "cross_flow_routing", "reply_resolution")
	slice := mustYAMLPath(t, root, "flow_model", "composition_routing", "route_plan_lowering", "implementation_slice_1835")
	sliceReply := mustYAMLPath(t, slice, "modes", "reply")

	assertScalarValue(t, mustMappingValue(t, reply, "status"), "runnable_v1")
	assertScalarValue(t, mustMappingValue(t, sliceReply, "status"), "runnable_v1")
	assertScalarContains(t, mustMappingValue(t, slice, "status"), "reply_runnable")
	assertScalarContains(t, mustMappingValue(t, slice, "rule"), "Reply resolution is also")
	assertScalarContains(t, mustMappingValue(t, slice, "canonical_code_owner"), "ConnectRoutePlan.InstanceKey/ReplyResolution")
	assertScalarContains(t, mustMappingValue(t, slice, "canonical_code_owner"), "internal/store.ReplyContextStore")
	assertScalarContains(t, mustMappingValue(t, sliceReply, "contract"), "engine.cross_flow_routing.reply_resolution")
}

func TestPlatformSpecInstanceIdentityAuthoringSourceAuthority(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	slice := mustYAMLPath(t, root, "flow_model", "composition_routing", "route_plan_lowering", "implementation_slice_1835")
	owner := mustMappingValue(t, slice, "instance_identity_owner_2021")
	assertScalarValue(t, mustMappingValue(t, owner, "status"), "scalar_typed_owner_finalized_by_2087")
	assertScalarValue(t, mustMappingValue(t, owner, "authored_identity_owner"), "flow instance: <field>")
	assertScalarValue(t, mustMappingValue(t, owner, "authored_source_owner"), "connection key_from, with omission deriving the same-named required payload field")
	assertScalarValue(t, mustMappingValue(t, owner, "authored_behavior_owner"), "connection resolution for ordinary variants; response-edge replies_to and optional correlation_key for replies")
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
	assertScalarContains(t, mustMappingValue(t, create, "contract"), "pre-existing identity conflicts")

	selectMode := mustYAMLPath(t, slice, "modes", "select")
	selectOrCreate := mustYAMLPath(t, slice, "modes", "select-or-create")
	assertScalarContains(t, mustMappingValue(t, selectMode, "contract"), "required same-named producer payload field")
	assertScalarContains(t, mustMappingValue(t, selectMode, "contract"), "one top-level payload field with compatible producer and receiver types")
	assertScalarContains(t, mustMappingValue(t, selectOrCreate, "contract"), "receiver scalar instance")
	if strings.Contains(mustMappingValue(t, slice, "authoring_shape").Value, "instance_key") {
		t.Fatal("canonical instance identity authoring shape retains retired resolution.instance_key")
	}

	proofs := mustMappingValue(t, slice, "proof_obligations")
	for _, want := range []string{
		"route-plan lowering consumes the immutable connection's typed create mode, scalar instance Field and Source derived from the required same-named payload field or exact key_from exception, with no shared-pin policy or producer output-key dependency",
		"downstream receiver routes can render the receiver-targeted identity projection derived from typed Source as payload.<instance>",
		"route-plan lowering consumes the immutable connection's typed select mode, scalar instance Field and payload Source derived from the required same-named payload field or exact key_from exception",
		"route-plan lowering consumes the immutable connection's typed select-or-create mode, scalar instance Field and payload Source derived from the required same-named payload field or exact key_from exception",
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
	assertScalarValue(t, mustMappingValue(t, crossFlow, "canonical_owner"), "platform-spec.yaml#flow_model.composition_routing")
	assertScalarValue(t, mustMappingValue(t, crossFlow, "implementation_status"), "typed_composition_authority_complete_for_supported_surface")
	if !sequenceContainsScalar(mustYAMLPath(t, crossFlow, "target_resolution", "precedence"), "lowered parent connect route plan") {
		t.Fatal("cross_flow_routing target precedence must start from lowered parent connect route plan")
	}
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "retired_producer_forms", "target"), "rejected on presence")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "retired_producer_forms", "target"), "receiver-owned")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "retired_producer_forms", "broadcast"), "rejected on presence")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "target_resolution", "fail_closed"), "typed same-flow consumer or explicit compiled connection")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "auto_wiring", "description"), "never create cross-flow subscriptions or connections")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "activation", "rule"), "explicit compiled connection")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "auto_wiring", "template_pairs"), "lowered parent connect route facts")

	assertScalarContains(t, mustYAMLPath(t, crossFlow, "structural_binding", "precedence_guard"), "Structural output-consumer inference is retired")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "structural_binding", "child_to_parent"), "requires an explicit compiled connection")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "structural_binding", "static_child_no_instance"), "do not authorize onward output")
	assertScalarContains(t, mustYAMLPath(t, crossFlow, "parent_route", "read_rule"), "does not read it as consumer or target authority")

	pinAuthority := mustYAMLPath(t, root, "flow_model", "pins", "routing_authority")
	assertScalarContains(t, pinAuthority, "exact least-common-ancestor schema.yaml connect")
	assertScalarContains(t, pinAuthority, "pins never own those policies")
	assertScalarContains(t, mustYAMLPath(t, root, "flow_model", "pins", "output_event_pins", "description"), "A parent or current delivery address is not a consumer")

	pinTargetResolution := mustYAMLPath(t, root, "static_analyzer", "slice_3a_pin_target_resolution")
	assertScalarValue(t, mustMappingValue(t, pinTargetResolution, "canonical_replacement"), "flow_model.composition_routing.analyzer_verify_requirements")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "lowered_parent_connect", "rule"), "Parent connect")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "typed_same_flow_consumer", "rule"), "typed pub/sub")
	if hasMappingKey(mustMappingValue(t, pinTargetResolution, "accepted_target_mechanisms"), "harness_sink") {
		t.Fatal("target mechanisms retain harness authority")
	}
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "accepted_target_mechanisms", "selected_root_export", "rule"), "no fabricated target or delivery")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "typed same-flow consumer")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "selected-root export")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "agent emit_events")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "scope", "description"), "never author recipient identity")
	assertScalarContains(t, mustYAMLPath(t, pinTargetResolution, "static_failure_reasons", "retired_emit_routing"), "present in any shape")
	if hasMappingKey(mustMappingValue(t, pinTargetResolution, "static_failure_reasons"), "harness_consumer_conflict") {
		t.Fatal("retired harness policy remains")
	}
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
	assertScalarValue(t, mustYAMLPath(t, fanOut, "effective_semantics", "canonical_owner"), "contracts.WorkflowContractBundle.CompileFanOutPlan")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "collection_iteration"), "constant-size obligation")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "collection_iteration"), "Lists retain declared order and multiplicity")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "collection_iteration"), "Maps yield exact keys in lexical order")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "collection_iteration"), "without trimming, coercion, value-based deduplication")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "collection_iteration"), "stable per-entry activation identity")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "collection_iteration"), "event_id never own")
	assertScalarContains(t, mustYAMLPath(t, fanOut, "retired_target_field"), "retired on presence")
	if !sequenceContainsScalar(mustYAMLPath(t, fanOut, "effective_semantics", "consumers"), "engine") ||
		!sequenceContainsScalar(mustYAMLPath(t, fanOut, "effective_semantics", "consumers"), "authoring_view") {
		t.Fatal("fan_out effective semantics must name runtime and authoring consumers")
	}
	assertScalarContains(t, mustYAMLPath(t, fanOut, "platform_ceiling", "overrun"), "never recommends author-side batching or pacing")
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
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "not a consuming relation")
	assertScalarContains(t, mustMappingValue(t, pinTargetResolution, "trigger"), "fail strict loading on presence")

	outputPinKeyCarries := mustSequenceMappingByScalarField(t, checks, "id", "output_pin_key_carries_validation")
	assertScalarContains(t, mustMappingValue(t, outputPinKeyCarries, "trigger"), "key or carries is present")
	assertScalarContains(t, mustMappingValue(t, outputPinKeyCarries, "trigger"), "event schema/business key")

	bootSteps := mustYAMLPath(t, root, "engine", "boot_sequence", "steps")
	validatePins := mustSequenceMappingByScalarField(t, bootSteps, "name", "validate_pins")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "canonical selected-root ingress")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "Private outputs require real local consumers or explicit compiled connections")
	assertScalarContains(t, mustMappingValue(t, validatePins, "action"), "source/sink/field grants and option bags are unsupported")
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
		"FlowPackageConnect",
		"package_key:",
		"package-root",
		"package-visible",
		"intra_package_event_consumer_projection",
		"bind.inputs",
		"bind.outputs",
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
